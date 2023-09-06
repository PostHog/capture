use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;

use axum::{http::StatusCode, Json};
use axum::body::HttpBody;
// TODO: stream this instead
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum_client_ip::InsecureClientIp;
use base64::Engine;
use serde_json::Value;

use crate::{
    api::{CaptureResponse,CaptureResponseCode},
    event::{RawEvent, EventFormData, EventQuery, ProcessedEvent},
    router, sink, token,
    utils::uuid_v7
};

pub async fn event(
    state: State<router::State>,
    InsecureClientIp(ip): InsecureClientIp,
    mut meta: Query<EventQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<CaptureResponse>, (StatusCode, String)> {
    tracing::debug!(len = body.len(), "new event request");

    meta.now = Some(state.timesource.current_time());
    meta.client_ip = Some(ip.to_string());

    let events = match headers
        .get("content-type")
        .map_or("", |v| v.to_str().unwrap_or(""))
    {
        "application/x-www-form-urlencoded" => {
            let input: EventFormData = serde_urlencoded::from_bytes(body.deref()).unwrap();
            let payload = base64::engine::general_purpose::STANDARD
                .decode(input.data)
                .unwrap();
            RawEvent::from_bytes(&meta, payload.into())
        }
        _ => RawEvent::from_bytes(&meta, body),
    };

    let events = match events {
        Ok(events) => events,
        Err(e) => {
            tracing::error!("failed to decode event: {:?}", e);
            return Err((
                StatusCode::BAD_REQUEST,
                String::from("Failed to decode event"),
            ));
        }
    };

    println!("Got events {:?}", &events);

    if events.is_empty() {
        return Err((StatusCode::BAD_REQUEST, String::from("No events in batch")));
    }
    match extract_and_verify_token(&events) {
        Ok(token) => meta.token = Some(token),
        Err(msg) => return Err((StatusCode::UNAUTHORIZED, msg)),
    }
    let processed = process_events(state.sink.clone(), &events, &meta).await;

    if let Err(msg) = processed {
        return Err((StatusCode::BAD_REQUEST, msg));
    }

    Ok(Json(CaptureResponse {
        status: CaptureResponseCode::Ok,
    }))
}

pub fn process_single_event(event: &RawEvent, query: &EventQuery) -> Result<ProcessedEvent> {
    let distinct_id = match &event.distinct_id {
        Some(id) => id,
        None => {
            match event.properties.get("distinct_id").map(|v| v.as_str()) {
                Some(Some(id)) => id,
                _ => {
                    return Err(anyhow!("missing distinct_id"))
                },
            }
        }
    };

    Ok(ProcessedEvent {
        uuid: event.uuid.unwrap_or_else(uuid_v7),
        distinct_id: distinct_id.to_string(),
        ip: query.client_ip.clone().unwrap_or_default(),
        site_url: String::new(),
        data: String::from("hallo I am some data 😊"),
        now: query.now.clone().unwrap_or_default(),
        sent_at: String::new(),
        token: query.token.clone().unwrap_or_default(),
    })
}

pub fn extract_and_verify_token(
        events: &[RawEvent]
) -> Result<String, String> {
    let mut request_token: Option<String> = None;

    // Collect the token from the batch, detect multiples to reject request
    for event in events {
        let event_token = match &event.token {
            Some(value) => value.clone(),
            None => match event.properties.get("token").map(Value::as_str) {
                Some(Some(value)) => value.to_string(),
                _ => {
                    return Err("event with no token".into());
                }
            }
        };
        if let Some(token) = &request_token {
            if !token.eq(&event_token) {
                return Err("mismatched tokens in batch".into());
            }
        } else {
            if let Err(invalid) = token::validate_token(event_token.as_str()) {
                return Err(invalid.reason().to_string());
            }
            request_token = Some(event_token);
        }
    }
    request_token.ok_or("no token found in request".into())
}


pub async fn process_events(
    sink: Arc<dyn sink::EventSink + Send + Sync>,
    events: &[RawEvent],
    query: &EventQuery
) -> Result<(), String> {
    let mut distinct_tokens = HashSet::new();

    // 1. Tokens are all valid
    for event in events {
        let token = event.token.clone().unwrap_or_else(|| {
            event
                .properties
                .get("token")
                .map_or(String::new(), |t| String::from(t.as_str().unwrap()))
        });

        if let Err(invalid) = token::validate_token(token.as_str()) {
            return Err(invalid.reason().to_string());
        }

        distinct_tokens.insert(token);
    }

    if distinct_tokens.len() > 1 {
        return Err(String::from("Number of distinct tokens in batch > 1"));
    }

    let events: Vec<ProcessedEvent> = match events.iter().map(|e| process_single_event(e, query)).collect() {
        Err(_) => return Err(String::from("Failed to process all events")),
        Ok(events) => events,
    };

    if events.len() == 1 {
        let sent = sink.send(events[0].clone()).await;

        if let Err(e) = sent {
            tracing::error!("Failed to send event to sink: {:?}", e);

            return Err(String::from("Failed to send event to sink"));
        }
    } else {
        let sent = sink.send_batch(events).await;

        if let Err(e) = sent {
            tracing::error!("Failed to send batch events to sink: {:?}", e);

            return Err(String::from("Failed to send batch events to sink"));
        }
    }

    Ok(())
}



#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use serde_json::json;
    use crate::capture::extract_and_verify_token;
    use crate::event::RawEvent;

    #[tokio::test]
    async fn all_events_have_same_token() {
        let events = vec![
            RawEvent {
                token: Some(String::from("hello")),
                distinct_id: Some("testing".to_string()),
                uuid: None,
                event: String::new(),
                properties: HashMap::new(),
            },
            RawEvent {
                token: None,
                distinct_id: Some("testing".to_string()),
                uuid: None,
                event: String::new(),
                properties: HashMap::from([(String::from("token"), json!("hello"))]),
            },
        ];

        let processed = extract_and_verify_token(&events);
        assert_eq!(processed.is_ok(), true, "{:?}", processed);
    }

    #[tokio::test]
    async fn all_events_have_different_token() {
        let events = vec![
            RawEvent {
                token: Some(String::from("hello")),
                distinct_id: Some("testing".to_string()),
                uuid: None,
                event: String::new(),
                properties: HashMap::new(),
            },
            RawEvent {
                token: None,
                distinct_id: Some("testing".to_string()),
                uuid: None,
                event: String::new(),
                properties: HashMap::from([(String::from("token"), json!("goodbye"))]),
            },
        ];

        let processed = extract_and_verify_token(&events);
        assert_eq!(processed.is_err(), true);
    }
}
