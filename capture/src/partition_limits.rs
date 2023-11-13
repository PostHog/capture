/// When a customer is writing too often to the same key, we get hot partitions. This negatively
/// affects our write latency and cluster health. We try to provide ordering guarantees wherever
/// possible, but this does require that we map key -> partition.
///
/// If the write-rate reaches a certain amount, we need to be able to handle the hot partition
/// before it causes a negative impact. In this case, instead of passing the error to the customer
/// with a 429, we relax our ordering constraints and temporarily override the key, meaning the
/// customers data will be written to random partitions.
use std::{num::NonZeroU32, sync::Arc};

use governor::{clock, state::keyed::DefaultKeyedStateStore, Quota, RateLimiter};

// See: https://docs.rs/governor/latest/governor/_guide/index.html#usage-in-multiple-threads
#[derive(Clone)]
pub struct PartitionLimiter {
    limiter: Arc<RateLimiter<String, DefaultKeyedStateStore<String>, clock::DefaultClock>>,
}

impl PartitionLimiter {
    pub fn new(per_second: NonZeroU32, burst: NonZeroU32) -> Self {
        let quota = Quota::per_second(per_second).allow_burst(burst);
        let limiter = Arc::new(governor::RateLimiter::dashmap(quota));

        PartitionLimiter { limiter }
    }

    pub fn is_limited(&self, key: &String) -> bool {
        self.limiter.check_key(key).is_err()
    }
}

#[cfg(test)]
mod tests {
    use crate::partition_limits::PartitionLimiter;
    use std::num::NonZeroU32;

    #[tokio::test]
    async fn low_limits() {
        let limiter =
            PartitionLimiter::new(NonZeroU32::new(1).unwrap(), NonZeroU32::new(1).unwrap());
        let token = String::from("test");

        assert!(!limiter.is_limited(&token));
        assert!(limiter.is_limited(&token));
    }

    #[tokio::test]
    async fn bursting() {
        let limiter =
            PartitionLimiter::new(NonZeroU32::new(1).unwrap(), NonZeroU32::new(3).unwrap());
        let token = String::from("test");

        assert!(!limiter.is_limited(&token));
        assert!(!limiter.is_limited(&token));
        assert!(!limiter.is_limited(&token));
        assert!(limiter.is_limited(&token));
    }
}
