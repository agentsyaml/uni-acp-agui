use super::*;

#[test]
fn initialize_does_not_advertise_unimplemented_list_changed_notifications() {
    let result = initialize_result();
    assert_eq!(result["capabilities"]["tools"]["listChanged"], false);
}

#[test]
fn jsonrpc_version_must_be_exactly_two_point_zero() {
    let valid: JsonRpcRequest = serde_json::from_value(json!({
        "jsonrpc": "2.0",
        "method": "notifications/initialized"
    }))
    .unwrap();
    assert_eq!(valid.jsonrpc.as_deref(), Some("2.0"));
    assert!(
        valid.id.is_none(),
        "valid notification must stay a notification"
    );

    for version in [json!("1.0"), json!(2.0), Value::Null] {
        let request: JsonRpcRequest = serde_json::from_value(json!({
            "jsonrpc": version,
            "id": 1,
            "method": "initialize"
        }))
        .unwrap();
        assert_ne!(request.jsonrpc.as_deref(), Some("2.0"));
    }

    let response = serde_json::to_value(JsonRpcResponse::invalid_request(json!(1))).unwrap();
    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 1);
    assert_eq!(response["error"]["code"], -32600);
    assert_eq!(response["error"]["message"], "Invalid Request");
}

#[test]
fn jsonrpc_response_omits_optional_fields() {
    let r = JsonRpcResponse::ok(json!(1), json!({"x":1}));
    let s = serde_json::to_value(r).unwrap();
    assert!(s.get("error").is_none());
    let r = JsonRpcResponse::err(json!(2), -1, "boom");
    let s = serde_json::to_value(r).unwrap();
    assert!(s.get("result").is_none());
    assert_eq!(s["error"]["code"], -1);
}

#[test]
fn mcp_name_header_accepts_plain_and_encoded_utf8_only() {
    assert_eq!(
        decode_mcp_name_value(b"missing-tool").unwrap(),
        "missing-tool"
    );
    assert_eq!(decode_mcp_name_value(b"=?base64?w6k=?=").unwrap(), "é");
    for value in [
        b"=?base64?%%%?=".as_slice(),
        b"=?base64?w6k?=".as_slice(),
        b" missing-tool".as_slice(),
        b"missing-tool ".as_slice(),
        b"m\xc3\xa9".as_slice(),
    ] {
        assert!(decode_mcp_name_value(value).is_err(), "{value:?}");
    }
}
