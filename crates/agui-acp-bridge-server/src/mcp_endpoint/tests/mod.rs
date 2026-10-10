use super::*;

fn modern_request(
    method: &str,
    id: Option<i64>,
    params: Value,
    accept: Option<&str>,
) -> axum::http::Request<axum::body::Body> {
    let mut params = params;
    if let Value::Object(object) = &mut params {
        let mut metadata = Map::new();
        metadata.insert(
            MODERN_PROTOCOL_METADATA_KEY.into(),
            json!(MODERN_PROTOCOL_VERSION),
        );
        metadata.insert(MODERN_CAPABILITIES_METADATA_KEY.into(), json!({}));
        object.insert("_meta".into(), Value::Object(metadata));
    }
    let mut body = json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
    });
    if let Some(id) = id {
        body["id"] = json!(id);
    }
    let mut builder = axum::http::Request::builder()
        .method(axum::http::Method::POST)
        .uri("/mcp/thread")
        .header("content-type", "application/json")
        .header("MCP-Protocol-Version", MODERN_PROTOCOL_VERSION)
        .header("Mcp-Method", method);
    if let Some(accept) = accept {
        builder = builder.header("accept", accept);
    }
    if method == "tools/call" {
        builder = builder.header("Mcp-Name", "missing-tool");
    }
    builder
        .body(axum::body::Body::from(body.to_string()))
        .unwrap()
}

fn with_origin(
    mut request: axum::http::Request<axum::body::Body>,
    origin: &str,
) -> axum::http::Request<axum::body::Body> {
    request
        .headers_mut()
        .insert("origin", origin.parse().unwrap());
    request
}

async fn response_json(response: Response) -> (StatusCode, Value) {
    use http_body_util::BodyExt;

    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).unwrap()
    };
    (status, value)
}

mod metadata;
mod modern_dispatch;
mod origin;
mod tools;
mod wire;
