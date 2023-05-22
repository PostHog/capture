use bytes::Bytes;

use axum::{http::StatusCode, Json};
// TODO: stream this instead
use axum::extract::Query;

use crate::{
    api::CaptureResponse,
    event::{EventQuery, Event},
    token,
};

pub async fn event(
    meta: Query<EventQuery>,
    body: Bytes,
) -> Result<Json<CaptureResponse>, (StatusCode, String)> {

    let events = Event::from_bytes(&meta, body);

    let events = match events {
        Ok(events) => events,
        Err(_)=> return Err((StatusCode::BAD_REQUEST, String::from("Failed to decode event"))),
    };

    let processed = process_events(&events);

    if let Err(msg) = processed {
        return Err((StatusCode::BAD_REQUEST, msg));
    }

    Ok(Json(CaptureResponse {}))
}

pub fn process_events(events: &[Event]) -> Result<(), String> {
    for event in events {
        if let Err(invalid) = token::validate_token(event.token.as_str()) {
            return Err(invalid.reason().to_string());
        }
    }

    Ok(())
}

// A group of events! There is no limit here, though our HTTP stack will reject anything above
// 20mb.
pub async fn batch() -> &'static str {
    "No batching for you!"
}
