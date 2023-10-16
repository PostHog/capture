use anyhow::Result;
use async_trait::async_trait;
use redis::{cluster::ClusterClient, AsyncCommands};

/// A simple redis wrapper
/// I'm currently just exposing the commands we use, for ease of implementation
/// Allows for testing + injecting failures
/// We can also swap it out for alternative implementations in the future
/// I tried using redis-rs Connection/ConnectionLike traits but honestly things just got really
/// awkward to work with.

#[async_trait]
pub trait RedisClient {
    // A very simplified wrapper, but works for our usage
    async fn zrangebyscore(&self, k: String, min: String, max: String) -> Result<Vec<String>>;
}

pub struct RedisClusterClient {
    client: ClusterClient,
}

impl RedisClusterClient {
    pub fn new(nodes: Vec<String>) -> Result<RedisClusterClient> {
        let client = ClusterClient::new(nodes)?;

        Ok(RedisClusterClient { client })
    }
}

#[async_trait]
impl RedisClient for RedisClusterClient {
    async fn zrangebyscore(&self, k: String, min: String, max: String) -> Result<Vec<String>> {
        let mut conn = self.client.get_async_connection().await?;

        let results = conn.zrangebyscore(k, min, max).await?;

        Ok(results)
    }
}

// mockall got really annoying with async and results so I'm just gonna do my own
#[derive(Clone)]
pub struct MockRedisClient {
    zrangebyscore_ret: Vec<String>
}

impl MockRedisClient {
    pub fn new() -> MockRedisClient{
        MockRedisClient{
            zrangebyscore_ret: Vec::new(),
        }
    }

    pub fn zrangebyscore_ret(&mut self, ret: Vec<String>) -> Self {
        self.zrangebyscore_ret = ret;

        self.clone()
    }
}

#[async_trait]
impl RedisClient for MockRedisClient {
    // A very simplified wrapper, but works for our usage
    async fn zrangebyscore(&self, k: String, min: String, max: String) -> Result<Vec<String>>{
        Ok(self.zrangebyscore_ret.clone())
    }
}
