/// When a customer is writing too often to the same key, we get hot partitions. This negatively
/// affects our write latency and cluster health. We try to provide ordering guarantees wherever
/// possible, but this does require that we map key -> partition.
///
/// If the write-rate reaches a certain amount, we need to be able to handle the hot partition
/// before it causes a negative impact. In this case, instead of passing the error to the customer
/// with a 429, we relax our ordering constraints and temporarily override the key, meaning the
/// customers data will be written to random partitions.
use std::{sync::Arc, num::NonZeroU32};

use governor::{RateLimiter, middleware::NoOpMiddleware, state::{InMemoryState, direct::NotKeyed, keyed::{DashMapStateStore, DefaultKeyedStateStore}}, clock::{DefaultClock, QuantaClock, QuantaInstant, self}, Quota};

// See: https://docs.rs/governor/latest/governor/_guide/index.html#usage-in-multiple-threads
#[derive(Clone)]
pub struct PartitionLimiter {
    limiter: Arc<RateLimiter<String, DefaultKeyedStateStore<String>, clock::DefaultClock>>,
}

impl PartitionLimiter {
    pub fn new(per_second: NonZeroU32, burst: NonZeroU32) -> Self{
        let quota = Quota::per_second(per_second).allow_burst(burst);
        let limiter = Arc::new(governor::RateLimiter::dashmap(quota));

        PartitionLimiter{limiter}
    }
}
