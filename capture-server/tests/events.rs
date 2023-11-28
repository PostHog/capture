use std::num::NonZeroU32;

use anyhow::Result;
use assert_json_diff::assert_json_include;
use reqwest::StatusCode;
use serde_json::json;

use crate::common::*;
mod common;

#[tokio::test]
async fn it_captures_one_event() -> Result<()> {
    setup_tracing();
    let token = random_string("token", 16);
    let distinct_id = random_string("id", 16);
    let topic = EphemeralTopic::new().await;
    let server = ServerHandle::for_topic(&topic);

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
    let server = ServerHandle::for_topic(&topic);

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

    let mut config = DEFAULT_CONFIG.clone();
    config.kafka.kafka_topic = topic.topic_name().to_string();
    config.burst_limit = NonZeroU32::new(2).unwrap();
    config.per_second_limit = NonZeroU32::new(1).unwrap();

    let server = ServerHandle::for_config(config);

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

    let mut config = DEFAULT_CONFIG.clone();
    config.kafka.kafka_topic = topic.topic_name().to_string();
    config.burst_limit = NonZeroU32::new(1).unwrap();
    config.per_second_limit = NonZeroU32::new(1).unwrap();

    let server = ServerHandle::for_config(config);

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
async fn it_trims_distinct_id() -> Result<()> {
    setup_tracing();
    let token = random_string("token", 16);
    let distinct_id1 = random_string("id", 200 - 3);
    let distinct_id2 = random_string("id", 200);
    let (trimmed_distinct_id2, _) = distinct_id2.split_at(200); // works because ascii chars

    let topic = EphemeralTopic::new().await;
    let server = ServerHandle::for_topic(&topic);

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
            "distinct_id": trimmed_distinct_id2
        })
    );

    Ok(())
}
