#![allow(dead_code)]

use std::default::Default;
use std::net::{SocketAddr, TcpListener};
use std::str::FromStr;
use std::string::ToString;
use std::sync::{Arc, Once};
use std::time::Duration;

use anyhow::bail;
use once_cell::sync::Lazy;
use rand::distributions::Alphanumeric;
use rand::Rng;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::config::{ClientConfig, FromClientConfig};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::util::Timeout;
use rdkafka::{Message, TopicPartitionList};
use tokio::sync::Notify;
use tracing::debug;

use capture_server::config::Config;
use capture_server::server::serve;

pub static DEFAULT_CONFIG: Lazy<Config> = Lazy::new(|| Config {
    print_sink: false,
    address: SocketAddr::from_str("127.0.0.1:0").unwrap(),
    export_prometheus: false,
    redis_url: "redis://localhost:6379/".to_string(),
    kafka_hosts: "kafka:9092".to_string(),
    kafka_topic: "events_plugin_ingestion".to_string(),
    kafka_tls: false,
});

static TRACING_INIT: Once = Once::new();
pub fn setup_tracing() {
    TRACING_INIT.call_once(|| {
        tracing_subscriber::fmt()
            .with_writer(tracing_subscriber::fmt::TestWriter::new())
            .init()
    });
}
pub struct ServerHandle {
    pub addr: SocketAddr,
    shutdown: Arc<Notify>,
}

impl ServerHandle {
    pub fn for_topic(topic: &EphemeralTopic) -> Self {
        let mut config = DEFAULT_CONFIG.clone();
        config.kafka_topic = topic.topic_name().to_string();
        Self::for_config(config)
    }
    pub fn for_config(config: Config) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let notify = Arc::new(Notify::new());
        let shutdown = notify.clone();

        tokio::spawn(
            async move { serve(config, listener, async { notify.notified().await }).await },
        );
        Self { addr, shutdown }
    }

    pub async fn capture_events<T: Into<reqwest::Body>>(&self, body: T) -> reqwest::Response {
        let client = reqwest::Client::new();
        client
            .post(format!("http://{:?}/i/v0/e", self.addr))
            .body(body)
            .send()
            .await
            .expect("failed to send request")
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.shutdown.notify_one()
    }
}

pub struct EphemeralTopic {
    consumer: BaseConsumer,
    read_timeout: Timeout,
    topic_name: String,
}

impl EphemeralTopic {
    pub async fn new() -> Self {
        let mut config = ClientConfig::new();
        config.set("group.id", "capture_integration_tests");
        config.set("bootstrap.servers", DEFAULT_CONFIG.kafka_hosts.clone());
        config.set("debug", "all");

        // TODO: check for name collision?
        let topic_name = random_string("events_", 16);
        let admin = AdminClient::from_config(&config).expect("failed to create admin client");
        admin
            .create_topics(
                &[NewTopic {
                    name: &topic_name,
                    num_partitions: 1,
                    replication: TopicReplication::Fixed(1),
                    config: vec![],
                }],
                &AdminOptions::default(),
            )
            .await
            .expect("failed to create topic");

        let consumer: BaseConsumer = config.create().expect("failed to create consumer");
        let mut assignment = TopicPartitionList::new();
        assignment.add_partition(&topic_name, 0);
        consumer
            .assign(&assignment)
            .expect("failed to assign topic");

        Self {
            consumer,
            read_timeout: Timeout::After(Duration::from_secs(5)),
            topic_name,
        }
    }

    pub fn next_event(&self) -> anyhow::Result<serde_json::Value> {
        match self.consumer.poll(self.read_timeout) {
            Some(Ok(message)) => {
                let body = message.payload().expect("empty kafka message");
                let event = serde_json::from_slice(body)?;
                Ok(event)
            }
            Some(Err(err)) => bail!("kafka read error: {}", err),
            None => bail!("kafka read timeout"),
        }
    }

    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }
}

impl Drop for EphemeralTopic {
    fn drop(&mut self) {
        debug!("dropping EphemeralTopic {}...", self.topic_name);
        _ = self.consumer.unassign();
        futures::executor::block_on(delete_topic(self.topic_name.clone()));
        debug!("dropped topic");
    }
}

async fn delete_topic(topic: String) {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", DEFAULT_CONFIG.kafka_hosts.clone());
    let admin = AdminClient::from_config(&config).expect("failed to create admin client");
    admin
        .delete_topics(&[&topic], &AdminOptions::default())
        .await
        .expect("failed to delete topic");
}

pub fn random_string(prefix: &str, length: usize) -> String {
    let suffix: String = rand::thread_rng()
        .sample_iter(Alphanumeric)
        .take(length)
        .map(char::from)
        .collect();
    format!("{}_{}", prefix, suffix)
}