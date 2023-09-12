use std::future::ready;
use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};
use tower_http::trace::TraceLayer;

use crate::{capture, sink, time::TimeSource};

use crate::prometheus::{setup_metrics_recorder, track_metrics};

#[derive(Clone)]
pub struct State {
    pub sink: Arc<dyn sink::EventSink + Send + Sync>,
    pub timesource: Arc<dyn TimeSource + Send + Sync>,
}

async fn index() -> &'static str {
    "capture"
}

pub fn router<
    TZ: TimeSource + Send + Sync + 'static,
    S: sink::EventSink + Send + Sync + 'static,
>(
    timesource: TZ,
    sink: S,
) -> Router {
    let state = State {
        sink: Arc::new(sink),
        timesource: Arc::new(timesource),
    };

    let recorder_handle = setup_metrics_recorder();

    Router::new()
        // TODO: use NormalizePathLayer::trim_trailing_slash
        // I've added GET routes as well, which for now just return "capture"
        // We could possibly make them return something useful (eg, schema)
        .route("/", get(index))
        .route("/i", post(capture::event))
        .route("/i/", post(capture::event))
        .route("/i", get(index))
        .route("/i/", get(index))
        .route("/metrics", get(move || ready(recorder_handle.render())))
        .layer(TraceLayer::new_for_http())
        .layer(axum::middleware::from_fn(track_metrics))
        .with_state(state)
}
