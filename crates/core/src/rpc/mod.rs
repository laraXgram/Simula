//! TL function handlers, one file per function under `rpc/handlers/<namespace>/`.
//! `handlers/mod.rs` (generated) finds them; this file adds the call wrappers.

mod handlers;

use simula_tl::Function;

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};

/// A handler's result with its concrete type erased: encodable as TL or JSON-TL, and
/// recoverable by typed callers ([`crate::Sandbox::call`]).
pub trait Reply: Send + std::fmt::Debug {
    fn to_tl(&self) -> Vec<u8>;
    fn to_json(&self) -> serde_json::Value;
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send>;
}

impl<T> Reply for T
where
    T: simula_tl::Serializable + serde::Serialize + Send + std::fmt::Debug + 'static,
{
    fn to_tl(&self) -> Vec<u8> {
        self.to_bytes()
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
}

pub use handlers::IMPLEMENTED;

pub fn dispatch(ctx: &mut Ctx<'_>, call: Function) -> RpcResult<Box<dyn Reply>> {
    match call {
        // Wrappers clients put around real calls.
        Function::InvokeWithLayer(call) => dispatch(ctx, *call.query),
        Function::InitConnection(call) => dispatch(ctx, *call.query),
        Function::InvokeWithoutUpdates(call) => dispatch(ctx, *call.query),
        call => match handlers::route(ctx, call) {
            Ok(result) => result,
            Err(unhandled) => Err(RpcError::not_implemented(unhandled.name())),
        },
    }
}
