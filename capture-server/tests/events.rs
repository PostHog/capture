use std::num::NonZeroU32;
use std::ops::Add;

use anyhow::Result;
use assert_json_diff::assert_json_include;
use reqwest::StatusCode;
use serde_json::json;
use time::{Duration, OffsetDateTime};

use crate::common::*;
mod common;

#[tokio::test]
async fn it_captures_one_event() -> Result<()> {
    setup_tracing();
    let token = random_string("token", 16);
    let distinct_id = random_string("id", 16);
    let topic = EphemeralTopic::new().await;
    let server = ServerConfig::default().with(&topic).start();

    let event = json!({
        "token": token,
        "event": "testing",
        "distinct_id": distinct_id
    });
    let res = server.capture_events(event.to_string()).await;
    assert_eq!(StatusCode::OK, res.status());

    let event = topic.next_event()?;
    assert_json_include!(
        actual: event,
        expected: json!({
            "token": token,
            "distinct_id": distinct_id
        })
    );

    Ok(())
}

#[tokio::test]
async fn it_captures_a_batch() -> Result<()> {
    setup_tracing();
    let token = random_string("token", 16);
    let distinct_id1 = random_string("id", 16);
    let distinct_id2 = random_string("id", 16);

    let topic = EphemeralTopic::new().await;
    let server = ServerConfig::default().with(&topic).start();

    let event = json!([{
        "token": token,
        "event": "event1",
        "distinct_id": distinct_id1
    },{
        "token": token,
        "event": "event2",
        "distinct_id": distinct_id2
    }]);
    let res = server.capture_events(event.to_string()).await;
    assert_eq!(StatusCode::OK, res.status());

    assert_json_include!(
        actual: topic.next_event()?,
        expected: json!({
            "token": token,
            "distinct_id": distinct_id1
        })
    );
    assert_json_include!(
        actual: topic.next_event()?,
        expected: json!({
            "token": token,
            "distinct_id": distinct_id2
        })
    );

    Ok(())
}

#[tokio::test]
async fn it_is_limited_with_burst() -> Result<()> {
    setup_tracing();

    let token = random_string("token", 16);
    let distinct_id = random_string("id", 16);

    let topic = EphemeralTopic::new().await;
    let server = ServerConfig::new(|config| {
        config.burst_limit = NonZeroU32::new(2).unwrap();
        config.per_second_limit = NonZeroU32::new(1).unwrap();
    })
    .with(&topic)
    .start();

    let event = json!([{
        "token": token,
        "event": "event1",
        "distinct_id": distinct_id
    },{
        "token": token,
        "event": "event2",
        "distinct_id": distinct_id
    },{
        "token": token,
        "event": "event3",
        "distinct_id": distinct_id
    }]);

    let res = server.capture_events(event.to_string()).await;
    assert_eq!(StatusCode::OK, res.status());

    assert_eq!(
        topic.next_message_key()?.unwrap(),
        format!("{}:{}", token, distinct_id)
    );

    assert_eq!(
        topic.next_message_key()?.unwrap(),
        format!("{}:{}", token, distinct_id)
    );

    assert_eq!(topic.next_message_key()?, None);

    Ok(())
}

#[tokio::test]
async fn it_does_not_partition_limit_different_ids() -> Result<()> {
    setup_tracing();

    let token = random_string("token", 16);
    let distinct_id = random_string("id", 16);
    let distinct_id2 = random_string("id", 16);

    let topic = EphemeralTopic::new().await;
    let server = ServerConfig::new(|config| {
        config.burst_limit = NonZeroU32::new(1).unwrap();
        config.per_second_limit = NonZeroU32::new(1).unwrap();
    })
    .with(&topic)
    .start();

    let event = json!([{
        "token": token,
        "event": "event1",
        "distinct_id": distinct_id
    },{
        "token": token,
        "event": "event2",
        "distinct_id": distinct_id2
    }]);

    let res = server.capture_events(event.to_string()).await;
    assert_eq!(StatusCode::OK, res.status());

    assert_eq!(
        topic.next_message_key()?.unwrap(),
        format!("{}:{}", token, distinct_id)
    );

    assert_eq!(
        topic.next_message_key()?.unwrap(),
        format!("{}:{}", token, distinct_id2)
    );

    Ok(())
}

#[tokio::test]
async fn it_enforces_billing_limits() -> Result<()> {
    setup_tracing();

    let distinct_id = random_string("id", 16);
    let topic = EphemeralTopic::new().await;
    let redis = EphemeralRedis::new().await;

    // team1 hit rate-limits for events, capture should drop
    let token1 = random_string("token1", 16);
    redis.add_billing_limit(
        "events",
        &token1,
        OffsetDateTime::now_utc().add(Duration::hours(1)),
    )?;

    // team2 has no billing limit for events, allow input
    let token2 = random_string("token2", 16);
    redis.add_billing_limit(
        "recordings",
        &token2,
        OffsetDateTime::now_utc().add(Duration::hours(1)),
    )?;

    // team3 was limited, but the new billing period started an hour ago, allow input
    let token3 = random_string("token3", 16);
    redis.add_billing_limit(
        "events",
        &token3,
        OffsetDateTime::now_utc().add(Duration::hours(-1)),
    )?;

    let server = ServerConfig::default().with(&topic).with(&redis).start();

    let res = server
        .capture_events(
            json!({
                "token": token1,
                "event": "NOK: should be dropped",
                "distinct_id": distinct_id
            })
            .to_string(),
        )
        .await;
    assert_eq!(StatusCode::OK, res.status());

    let res = server
        .capture_events(
            json!({
                "token": token2,
                "event": "OK: no billing limit",
                "distinct_id": distinct_id
            })
            .to_string(),
        )
        .await;
    assert_eq!(StatusCode::OK, res.status());

    let res = server
        .capture_events(
            json!({
                "token": token3,
                "event": "OK: billing limit expired",
                "distinct_id": distinct_id
            })
            .to_string(),
        )
        .await;
    assert_eq!(StatusCode::OK, res.status());

    assert_json_include!(
        actual: topic.next_event()?,
        expected: json!({
            "token": token2, // Message with token1 should not pass through
        })
    );
    assert_json_include!(
        actual: topic.next_event()?,
        expected: json!({
            "token": token3,
        })
    );

    Ok(())
}
