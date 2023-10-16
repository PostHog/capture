use std::{collections::HashSet, sync::Arc, ops::Sub};

use crate::redis::{RedisClient, RedisClusterClient, MockRedisClient};
use async_trait::async_trait;
/// Limit accounts by team ID if they hit a billing limit
///
/// We have an async celery worker that regularly checks on accounts + assesses if they are beyond
/// a billing limit. If this is the case, a key is set in redis.
///
/// On the capture side, we should check if a request should be limited. If so, we can respond with
/// a 429.
///
/// Requirements
///
/// 1. Updates from the celery worker should be reflected in capture within a short period of time
/// 2. Capture should cope with redis being _totally down_, and fail open
/// 3. We should not hit redis for every single request
///
/// The solution here is
///
/// 1. A background task to regularly pull in the latest set from redis
/// 2. A cached store of the most recently known set, so we don't need to keep hitting redis
///
/// Some small delay between an account being limited and the limit taking effect is acceptable.
/// However, ideally we should not allow requests from some pods but 429 from others.
use thiserror::Error;
use time::{Duration, OffsetDateTime, UtcOffset};
use tokio::sync::{Mutex, RwLock};
use tracing::Level;

// todo: fetch from env
const UPDATE_INTERVAL_SECS: u64 = 5;
const QUOTA_LIMITER_CACHE_KEY: &'static str = "@posthog/quota-limits/";

#[derive(Error, Debug)]
pub enum LimiterError {
    #[error("updater already running - there can only be one")]
    UpdaterRunning,
}

struct BillingLimiter {
    limited: Arc<RwLock<HashSet<String>>>,
    interval: Duration,
    updated: Arc<RwLock<time::OffsetDateTime>>,
}

impl BillingLimiter {
    /// Create a new BillingLimiter.
    ///
    /// This connects to a redis cluster - pass in a vec of addresses for the initial nodes.
    ///
    /// You can also initialize the limiter with a set of tokens to liimt from the very beginning.
    /// This may be overridden by Redis, if the sets differ,
    ///
    /// Pass an empty redis node list to only use this initial set.
    pub fn new(
        limited: Option<HashSet<String>>,
        interval: Option<Duration >,
    ) -> anyhow::Result<BillingLimiter> {
        let limited = limited.unwrap_or_else(|| HashSet::new());
        let limited = Arc::new(RwLock::new(limited));

        // Force an update immediately if we have any reasonable interval
        let updated = OffsetDateTime::from_unix_timestamp(0)?;
        let updated = Arc::new(RwLock::new(updated));

        // Default to an interval that's so long, we will never update. If this code is still
        // running in 99yrs that's pretty cool.
        let interval = interval.unwrap_or_else(||Duration::weeks(99 * 52));

        Ok(BillingLimiter {
            interval,
            limited,
            updated,
        })
    }

    async fn fetch_limited(client: &impl RedisClient) -> anyhow::Result<Vec<String>> {
        let now = time::OffsetDateTime::now_utc().unix_timestamp();

        client
            .zrangebyscore(
                format!("{QUOTA_LIMITER_CACHE_KEY}events"),
                now.to_string(),
                String::from("+Inf"),
            )
            .await
    }

    pub async fn is_limited(&self, key: &str, client: &impl RedisClient) -> bool {
        // hold the read lock to clone it, very briefly. clone is ok because it's very small 🤏
        // rwlock can have many readers, but one writer. the writer will wait in a queue with all
        // the readers, so we want to hold read locks for the smallest time possible to avoid
        // writers waiting for too long. and vice versa.
        let updated = {
            let updated = self.updated.read().await;
            updated.clone()
        };

        let now = OffsetDateTime::now_utc();
        let since_update = now.sub(updated);

        if since_update > self.interval {
            let set = Self::fetch_limited(client).await;
            let set = HashSet::from_iter(set.unwrap().iter().cloned());

            let mut limited = self.limited.write().await;
            *limited = set;

            return limited.contains(key);
        } 

        let l = self.limited.read().await;

        l.contains(key)
    }
}

#[cfg(test)]
mod tests {
    use time::Duration;

    use crate::{billing_limits::BillingLimiter, redis::{MockRedisClient, RedisClient}};

    // Test that a token _not_ limited has no restriction applied
    // Avoid messing up and accidentally limiting everyone
    #[tokio::test]
    async fn test_not_limited() {
        let client = MockRedisClient::new();
        let limiter = BillingLimiter::new(None, None).expect("Failed to create billing limiter");

        assert_eq!(
            limiter.is_limited("idk it doesn't matter", &client).await,
            false
        );
    }

    // Test that a token _not_ limited has no restriction applied
    // Avoid messing up and accidentally limiting everyone
    #[tokio::test]
    async fn test_fixed_limited() {
        let client = MockRedisClient::new();

        let limiter = BillingLimiter::new(
            Some(
                vec![String::from("some_org_hit_limits")]
                    .into_iter()
                    .collect(),
            ),
            None,
        )
        .expect("Failed to create billing limiter");

        assert_eq!(
            limiter.is_limited("idk it doesn't matter", &client).await,
            false
        );
        assert!(limiter.is_limited("some_org_hit_limits", &client).await);
    }

    #[tokio::test]
    async fn test_dynamic_limited() {
        let client = MockRedisClient::new().zrangebyscore_ret(vec![String::from("banana")]);

        let limiter = BillingLimiter::new(
            Some(
                vec![String::from("some_org_hit_limits")]
                    .into_iter()
                    .collect(),
            ),
            Some(Duration::microseconds(1)),
        )
        .expect("Failed to create billing limiter");

        assert_eq!(
            limiter.is_limited("idk it doesn't matter", &client).await,
            false
        );

        assert_eq!(limiter.is_limited("some_org_hit_limits", &client).await, false);
        assert!(limiter.is_limited("banana", &client).await);
    }
}
