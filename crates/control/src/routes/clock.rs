//! The sandbox clock: real, frozen, or offset.

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use serde_json::{Value, json};
use simula_core::Sandbox;
use simula_core::clock::ClockMode;

use super::ok;

fn state(sandbox: &Sandbox) -> Json<Value> {
    ok(json!({ "now_ms": sandbox.clock().now_ms(), "mode": sandbox.clock().mode() }))
}

pub async fn get(State(sandbox): State<Arc<Sandbox>>) -> Json<Value> {
    state(&sandbox)
}

/// `PUT /clock` with `{"mode": "frozen", "at_ms": …}`, `{"mode": "offset", "offset_ms": …}`
/// or `{"mode": "real"}`.
pub async fn set(State(sandbox): State<Arc<Sandbox>>, Json(mode): Json<ClockMode>) -> Json<Value> {
    sandbox.clock().set_mode(mode);
    state(&sandbox)
}

#[derive(serde::Deserialize)]
pub struct Advance {
    pub by_ms: i64,
}

pub async fn advance(State(sandbox): State<Arc<Sandbox>>, Json(body): Json<Advance>) -> Json<Value> {
    sandbox.clock().advance(body.by_ms);
    state(&sandbox)
}
