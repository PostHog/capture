use std::sync::Arc;
use std::future::ready;
use std::time::Instant;

use axum::{
    routing::{get, post},
    Router,
};
use tower_http::trace::TraceLayer;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle, Matcher};

use axum::{http::Request, middleware::Next, response::IntoResponse, extract::MatchedPath};

use crate::{capture, sink, time::TimeSource};


#[derive(Clone)]
pub struct State {
    pub sink: Arc<dyn sink::EventSink + Send + Sync>,
    pub timesource: Arc<dyn TimeSource + Send + Sync>,
}

async fn index() -> &'static str {
    "capture"
}

fn setup_metrics_recorder() -> PrometheusHandle {
    const EXPONENTIAL_SECONDS: &[f64] = &[
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ];

    PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full("http_requests_duration_seconds".to_string()),
            EXPONENTIAL_SECONDS,
        )
        .unwrap()
        .install_recorder()
        .unwrap()
}

/// Middleware to record some common HTTP metrics
/// Generic over B to allow for arbitrary body types (eg Vec<u8>, Streams, a deserialized thing, etc)
/// Someday tower-http might provide a metrics middleware: https://github.com/tower-rs/tower-http/issues/57
pub async fn track_metrics<B>(req: Request<B>, next: Next<B>)->impl IntoResponse {
    let start = Instant::now();

    let path = if let Some(matched_path) = req.extensions().get::<MatchedPath>() {
        matched_path.as_str().to_owned()
    } else {
        req.uri().path().to_owned()
    };

    let method = req.method().clone();

    // Run the rest of the request handling first, so we can measure it and get response
    // codes.
    let response = next.run(req).await;

    let latency = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();

    let labels = [
        ("method", method.to_string()),
        ("path", path),
        ("status", status),
    ];

    metrics::increment_counter!("http_requests_total", &labels);
    metrics::histogram!("http_requests_duration_seconds", latency, &labels);

    response
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
