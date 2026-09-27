//! Route table. One file per resource.

mod accounts;
mod act;
mod clock;
mod events;

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::routing::{get, post};
use serde_json::{Value, json};
use simula_core::Sandbox;

pub fn router() -> Router<Arc<Sandbox>> {
    Router::new()
        .route("/health", get(|| async { ok(json!({ "status": "ok" })) }))
        .route("/accounts", get(accounts::list))
        .route("/accounts/{id}", get(accounts::get))
        .route("/users", post(accounts::create_user))
        .route("/bots", post(accounts::create_bot))
        .route("/accounts/{id}/call", post(act::call))
        .route("/accounts/{id}/send", post(act::send))
        .route("/accounts/{id}/press", post(act::press))
        .route("/accounts/{id}/updates", get(events::account_updates))
        .route("/events", get(events::all))
        .route("/clock", get(clock::get).put(clock::set))
        .route("/clock/advance", post(clock::advance))
}

pub fn ok(result: impl serde::Serialize) -> Json<Value> {
    Json(json!({ "ok": true, "result": result }))
}
