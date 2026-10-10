use super::*;
use agui_acp_bridge_core::{CustomAgentInProcessClient, InProcessAcpClient};
use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use serde_json::Value;
use std::time::Duration;
use tower::ServiceExt;

const TEST_TOKEN: &str = "test-bearer-token-123456";

mod admission;
mod history;
mod keepalive;
mod resume;
mod retirement;
mod security_input;
