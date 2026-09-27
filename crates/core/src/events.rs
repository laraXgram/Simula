//! Everything that happens in a sandbox, for the inspection tools and the event stream.

use serde_json::Value;

use crate::error::RpcError;
use crate::ids::UserId;

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Event {
    /// A TL function was executed on behalf of an account.
    Rpc {
        at_ms: i64,
        account: UserId,
        method: &'static str,
        request: Value,
        result: Result<Value, RpcError>,
        duration_us: u64,
    },
    /// Updates were delivered to an account.
    Updates { at_ms: i64, account: UserId, updates: Value },
    /// An account was created or changed outside TL (Control API).
    Account { at_ms: i64, account: UserId, action: &'static str },
}
