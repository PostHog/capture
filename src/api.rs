use serde::{Deserialize, Serialize};
// Define the API interface for capture here.
// This is used for serializing responses and deserializing requests

#[derive(Debug, Deserialize, Serialize)]
pub struct CaptureRequest{
    #[serde(alias = "$token")]
    pub token: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CaptureResponse{}
