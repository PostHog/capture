use std::future::Future;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;

use time::Duration;

use crate::billing_limits::BillingLimiter;
use crate::config::Config;
use crate::health::{ComponentStatus, HealthRegistry};
use crate::redis::RedisClient;
use crate::{router, sink};
pub async fn serve<F>(config: Config, listener: TcpListener, shutdown: F)
where
    F: Future<Output = ()>,
{
    let liveness = HealthRegistry::new();

    let redis_client =
        Arc::new(RedisClient::new(config.redis_url).expect("failed to create redis client"));

    let billing = BillingLimiter::new(Duration::seconds(5), redis_client.clone())
        .expect("failed to create billing limiter");

    let app = if config.print_sink {
        // Print sink is only used for local debug, don't allow a container with it to run on prod
        liveness
            .register("print_sink".to_string(), Duration::seconds(30))
            .await
            .report_status(ComponentStatus::Unhealthy)
            .await;
        router::router(
            crate::time::SystemTime {},
            liveness,
            sink::PrintSink {},
            redis_client,
            billing,
            config.export_prometheus,
        )
    } else {
        let sink_liveness = liveness
            .register("rdkafka".to_string(), Duration::seconds(30))
            .await;
        let sink = sink::KafkaSink::new(config.kafka, sink_liveness).unwrap();
        router::router(
            crate::time::SystemTime {},
            liveness,
            sink,
            redis_client,
            billing,
            config.export_prometheus,
        )
    };

    // run our app with hyper
    // `axum::Server` is a re-export of `hyper::Server`
    tracing::info!("listening on {:?}", listener.local_addr().unwrap());
    axum::Server::from_tcp(listener)
        .unwrap()
        .serve(app.into_make_service_with_connect_info::<SocketAddr>())
        .with_graceful_shutdown(shutdown)
        .await
        .unwrap()
}
