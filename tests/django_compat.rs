use std::fs::File;
use std::io::{BufRead, BufReader};
// use axum::http::StatusCode;
// use axum::Router;
// use axum_test_helper::TestClient;
use serde::Deserialize;
use serde_json::Value;
use time::OffsetDateTime;
use capture::event::ProcessedEvent;

/*
            "path": request.get_full_path(),
            "method": request.method,
            "content-encoding": request.META.get("content-encoding", ""),
            "ip": request.META.get("HTTP_X_FORWARDED_FOR", request.META.get("REMOTE_ADDR")),
            "now": now.isoformat(),
            "body": base64.b64encode(request.body).decode(encoding="ascii"),
            "output": [],
 */

#[derive(Debug, Deserialize)]
struct RequestDump {
    path: String,
    method: String,
    #[serde(alias = "content-encoding")]
    content_encoding: String,
    ip: String,
    now: String,
    body: String,
    output: Vec<ProcessedEvent>,
}

static REQUESTS_DUMP_FILE_NAME: &str = "tests/requests_dump.jsonl";

#[tokio::test]
async fn it_matches_django_capture_behaviour() {
    let file = File::open(REQUESTS_DUMP_FILE_NAME).expect("could not find input file");
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let request: RequestDump = serde_json::from_str(&line.unwrap()).unwrap();

        OffsetDateTime::now_utc();

        println!("{:?}", request);
    }
}

