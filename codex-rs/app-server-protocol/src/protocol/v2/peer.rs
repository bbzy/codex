use crate::JsonSchema;
use crate::TS;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct PeerContextParams {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct PeerQueueParams {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    pub message: String,
    pub source: serde_json::Value,
    pub target: serde_json::Value,
    pub resume: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct PeerReplyParams {
    pub receipt: String,
    pub message: String,
    pub r#final: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[ts(export_to = "v2/")]
pub struct PeerResponse {
    #[serde(flatten)]
    pub fields: HashMap<String, serde_json::Value>,
}
