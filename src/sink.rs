use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio::task::JoinSet;

use rdkafka::config::ClientConfig;
use rdkafka::producer::future_producer::{FutureProducer, FutureRecord};

use crate::event::ProcessedEvent;

#[async_trait]
pub trait EventSink {
    async fn send(&self, event: ProcessedEvent) -> Result<()>;
    async fn send_batch(&self, events: Vec<ProcessedEvent>) -> Result<()>;
}

pub struct PrintSink {}

#[async_trait]
impl EventSink for PrintSink {
    async fn send(&self, event: ProcessedEvent) -> Result<()> {
        tracing::info!("single event: {:?}", event);

        Ok(())
    }
    async fn send_batch(&self, events: Vec<ProcessedEvent>) -> Result<()> {
        let span = tracing::span!(tracing::Level::INFO, "batch of events");
        let _enter = span.enter();

        for event in events {
            tracing::info!("event: {:?}", event);
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct KafkaSink {
    producer: FutureProducer,
    topic: String,
}

impl KafkaSink {
    pub fn new(topic: String, brokers: String) -> Result<KafkaSink>{
        let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", &brokers)
                .create()?;

        Ok(KafkaSink{
            producer,
            topic
        })
    }
}

impl KafkaSink{
    async fn kafka_send(producer: FutureProducer, topic: String, event: ProcessedEvent) -> Result<()>{
        let payload = serde_json::to_string(&event)?;

        let key = event.key();

        match producer.send_result(FutureRecord{
            topic: topic.as_str(),
            payload: Some(&payload),
            partition: None,
            key: Some(&key),
            timestamp: None,
            headers: None,
        }){
            Ok(_) => {},
            Err(e) => {
                tracing::error!("failed to produce event: {}", e.0);

                // TODO: Improve error handling
                return Err(anyhow!("failed to produce event {}", e.0));
            },
        }
        
        Ok(())
    }
}

#[async_trait]
impl EventSink for KafkaSink {
    async fn send(&self, event: ProcessedEvent) -> Result<()> {
        Self::kafka_send(self.producer.clone(), self.topic.clone(), event).await
    }

    async fn send_batch(&self, events: Vec<ProcessedEvent>) -> Result<()> {
        let mut set = JoinSet::new();

        for event in events {
            let producer = self.producer.clone();
            let topic = self.topic.clone();

            set.spawn(
                Self::kafka_send(producer, topic, event)
            );
        }

        while let Some(res) = set.join_next().await {
            println!("{:?}", res);
        }

        Ok(())
    }
}
