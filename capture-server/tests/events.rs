use std::net::{SocketAddr, TcpListener};
use std::str::FromStr;
use std::string::ToString;

use anyhow::Result;
use once_cell::sync::Lazy;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use capture_server::config::Config;
use capture_server::server::serve;

static DEFAULT_CONFIG: Lazy<Config> = Lazy::new(|| Config {
    print_sink: false,
    address: SocketAddr::from_str("127.0.0.1:0").unwrap(),
    redis_url: "redis://localhost:6379/".to_string(),
    kafka_hosts: "kafka:9092".to_string(),
    kafka_topic: "events_plugin_ingestion".to_string(),
    kafka_tls: false,
});

struct ServerHandle {
    addr: SocketAddr,
    shutdown: oneshot::Sender<()>,
    join: JoinHandle<()>,
}

impl ServerHandle {
    fn new(config: Config) -> Self {
        let (shutdown, rx) = oneshot::channel::<()>();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let join =
            tokio::spawn(async move { serve(config, listener, async { rx.await.unwrap() }).await });
        Self {
            addr,
            shutdown,
            join,
        }
    }

    async fn stop(self) -> Result<()> {
        self.shutdown.send(()).unwrap();
        self.join.await?;
        Ok(())
    }
}

#[tokio::test]
async fn it_captures_one_event() -> Result<()> {
    let server = ServerHandle::new(DEFAULT_CONFIG.clone());

    server.stop().await
}
