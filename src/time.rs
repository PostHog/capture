pub trait TimeSource {
    // Return an ISO timestamp
    fn current_time(&self) -> String;
}

#[derive(Clone)]
pub struct SystemTime {}

impl TimeSource for SystemTime {
    fn current_time(&self) -> String {
        let time = time::OffsetDateTime::now_utc();

        time.to_string()
    }
}

#[derive(Clone)]
pub struct FixedTime {
    pub time: time::OffsetDateTime,
}

impl TimeSource for FixedTime {
    fn current_time(&self) -> String {
        self.time.to_string()
    }
}
