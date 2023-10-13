use std::{collections::HashSet, sync::Arc};

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
use redis::{cluster::ClusterClient, AsyncCommands, RedisError, ConnectionLike, Cmd};
use thiserror::Error;
use tokio::sync::{RwLock, Mutex};
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
    nodes: Vec<String>,
    limited: Arc<RwLock<HashSet<String>>>,
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
        nodes: Vec<String>,
        limited: Option<HashSet<String>>,
    ) -> Result<BillingLimiter, RedisError> {
        let limited = limited.unwrap_or_else(|| HashSet::new());
        let limited = Arc::new(RwLock::new(limited));

        Ok(BillingLimiter { nodes, limited })
    }
    
    async fn fetch_limited(client: &ClusterClient) -> Result<Vec<String>, RedisError>{
        let mut conn = client.get_async_connection().await?;
        let now = time::OffsetDateTime::now_utc().unix_timestamp();

        conn.zrangebyscore(format!("{QUOTA_LIMITER_CACHE_KEY}events"), now, "+Inf").await

    }

    /// Spawn the tokio updater task
    ///
    /// Note: If no nodes are set, the billing limiter falls back to limiting a fixed set. No
    /// updates.
    ///
    /// This will return an error if called more than once, globally. The background task will run
    /// for the duration of the capture.
    ///
    /// If for some reason the task fails (eg lost redis connection), we fail open by clearing the
    /// local cache and terminating the task.
    pub async fn start_updater(&self) -> Result<(), RedisError> {
        if self.nodes.is_empty() {
            return Ok(())
        }

        let limited = self.limited.clone();
        let client = ClusterClient::new(self.nodes.clone())?;

        tokio::spawn(async move {
            // use an interval rather than sleep
            // this takes into account the last execution time, so it will wait for 5 seconds since
            // the last execution time, not 5 seconds since we start awaiting. hence mut.
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(UPDATE_INTERVAL_SECS));

            // The goal is for the _entire_ iteration of this loop to take UPDATE_INTERVAL_SECS
            // If our execution takes less time than that, the interval will pad it out.
            loop {
                let _span = tracing::debug_span!("billing limit update tick");
                interval.tick().await;

                let tokens = Self::fetch_limited(&client).await;

                match tokens {
                    Ok(tokens) => {
                        tracing::debug!("{} limited tokens fetched from redis", tokens.len());

                        // RAII guard on the write lock
                        let mut l = limited.write().await;
                        
                        for i in tokens {
                            l.insert(i);
                        }
                    }
                    Err(e) => {
                        // Oh no! Clear all limits. Something has gone wrong so let's play it safe.
                        tracing::error!("error fetching limited set from redis: {}", e);

                        let mut l = limited.write().await;
                        l.clear();
                    }
                }
            }
        });

        Ok(())
    }

    pub async fn is_limited(&self, key: &str) -> bool {
        // RAII guard on read
        // The RwLock allows many readers, but just one writer.
        // This will yield the task if the write lock is currently held.
        let l = self.limited.read().await;

        l.contains(key)
    }
}

#[cfg(test)]
mod tests {
    use crate::billing_limits::BillingLimiter;

    // Test that a token _not_ limited has no restriction applied
    // Avoid messing up and accidentally limiting everyone
    #[tokio::test]
    async fn test_not_limited() {
        let limiter = BillingLimiter::new(vec![], None).expect("Failed to create billing limiter");
        limiter
            .start_updater()
            .await
            .expect("Failed to start updater");

        assert_eq!(limiter.is_limited("idk it doesn't matter").await, false);
    }

    // Test that a token _not_ limited has no restriction applied
    // Avoid messing up and accidentally limiting everyone
    #[tokio::test]
    async fn test_fixed_limited() {
        let limiter = BillingLimiter::new(
            vec![],
            Some(
                vec![String::from("some_org_hit_limits")]
                    .into_iter()
                    .collect(),
            ),
        )
        .expect("Failed to create billing limiter");

        limiter
            .start_updater()
            .await
            .expect("Failed to start updater");

        assert_eq!(limiter.is_limited("idk it doesn't matter").await, false);
        assert!(limiter.is_limited("some_org_hit_limits").await);
    }

    // TODO: test the redis stuff somehow
}
