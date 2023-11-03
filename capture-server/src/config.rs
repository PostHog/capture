use envconfig::Envconfig;
use std::net::SocketAddr;

#[derive(Envconfig, Clone)]
pub struct Config {
    #[envconfig(default = "false")]
    pub print_sink: bool,
    #[envconfig(default = "127.0.0.1:3000")]
    pub address: SocketAddr,
    pub redis_url: String,

    pub kafka_hosts: String,
    pub kafka_topic: String,
    #[envconfig(default = "false")]
    pub kafka_tls: bool,
}
