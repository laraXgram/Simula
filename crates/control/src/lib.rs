//! Simula's Control API, mounted at `/_simula/v1`. Everything a person can do in the
//! client is reachable here, so test suites in any language can drive a sandbox.
//!
//! Responses use the Bot API's envelope: `{"ok": true, "result": …}` or
//! `{"ok": false, "error": {"code": 400, "message": "PEER_ID_INVALID"}}`.

mod error;
mod routes;

use std::sync::Arc;

use axum::Router;
use simula_core::Sandbox;

pub use error::ControlError;

pub fn router(sandbox: Arc<Sandbox>) -> Router {
    routes::router().with_state(sandbox)
}
