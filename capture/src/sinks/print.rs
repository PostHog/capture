use async_trait::async_trait;
use metrics::{counter, histogram};
use tracing::log::info;

use crate::api::CaptureError;
use crate::event::ProcessedEvent;
use crate::sinks::Event;

pub struct PrintSink {}

#[async_trait]
impl Event for PrintSink {
    async fn send(&self, event: ProcessedEvent) -> Result<(), CaptureError> {
        info!("single event: {:?}", event);
        counter!("capture_events_ingested_total", 1);

        Ok(())
    }
    async fn send_batch(&self, events: Vec<ProcessedEvent>) -> Result<(), CaptureError> {
        let span = tracing::span!(tracing::Level::INFO, "batch of events");
        let _enter = span.enter();

        histogram!("capture_event_batch_size", events.len() as f64);
        counter!("capture_events_ingested_total", events.len() as u64);
        for event in events {
            info!("event: {:?}", event);
        }

        Ok(())
    }
}
