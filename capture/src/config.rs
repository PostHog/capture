use std::{net::SocketAddr, num::NonZeroU32};

use envconfig::Envconfig;

#[derive(Envconfig, Clone)]
pub struct Config {
    #[envconfig(default = "false")]
    pub print_sink: bool,

    #[envconfig(default = "127.0.0.1:3000")]
    pub address: SocketAddr,

    pub redis_url: String,
    pub otel_url: Option<String>,

    #[envconfig(default = "100")]
    pub per_second_limit: NonZeroU32,

    #[envconfig(default = "1000")]
    pub burst_limit: NonZeroU32,

    #[envconfig(nested = true)]
    pub kafka: KafkaConfig,

    #[envconfig(default = "1.0")]
    pub otel_sampling_rate: f64,

    #[envconfig(default = "true")]
    pub export_prometheus: bool,
}

#[derive(Envconfig, Clone)]
pub struct KafkaConfig {
    #[envconfig(default = "20")]
    pub kafka_producer_linger_ms: u32, // Maximum time between producer batches during low traffic
    #[envconfig(default = "400")]
    pub kafka_producer_queue_mib: u32, // Size of the in-memory producer queue in mebibytes
    #[envconfig(default = "none")]
    pub kafka_compression_codec: String, // none, gzip, snappy, lz4, zstd
    pub kafka_hosts: String,
    pub kafka_topic: String,
    #[envconfig(default = "false")]
    pub kafka_tls: bool,
}
