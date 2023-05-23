use axum::http::StatusCode;
use axum_test_helper::TestClient;
use capture::event::ProcessedEvent;
use capture::router::router;
use serde::Deserialize;
use std::fs::File;
use std::io::{BufRead, BufReader};
use capture::time::TimeSource;
use time::OffsetDateTime;

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

#[derive(Clone)]
pub struct FixedTime {
    pub time: time::OffsetDateTime,
}

impl TimeSource for FixedTime {
    fn current_time(&self) -> String {
        self.time.to_string()
    }
}

#[ignore]
#[tokio::test]
async fn it_matches_django_capture_behaviour() -> anyhow::Result<()> {
    let file = File::open(REQUESTS_DUMP_FILE_NAME)?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let request: RequestDump = serde_json::from_str(&line?)?;

        if request.path.starts_with("/s") {
            println!("Skipping {} dump", &request.path);
            continue;
        }

        println!("{:?}", &request);
        // TODO: massage data

        let timesource = FixedTime{time: OffsetDateTime::now_utc()};
        let app = router(timesource);

        let client = TestClient::new(app);
        let res = client.post("/e/").send().await;
        assert_eq!(res.status(), StatusCode::OK, "{}", res.text().await);
    }
    Ok(())
}
