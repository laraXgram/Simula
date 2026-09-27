//! TL function handlers, one file per function under `rpc/<namespace>/`.

mod messages;
mod updates;
mod users;

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

pub fn dispatch(ctx: &mut Ctx<'_>, call: Function) -> RpcResult<Box<dyn Reply>> {
    Ok(match call {
        // Wrappers clients put around real calls.
        Function::InvokeWithLayer(call) => return dispatch(ctx, *call.query),
        Function::InitConnection(call) => return dispatch(ctx, *call.query),
        Function::InvokeWithoutUpdates(call) => return dispatch(ctx, *call.query),

        Function::MessagesSendMessage(call) => Box::new(messages::send_message::handle(ctx, *call)?),
        Function::UpdatesGetState(call) => Box::new(updates::get_state::handle(ctx, *call)?),
        Function::UsersGetUsers(call) => Box::new(users::get_users::handle(ctx, *call)?),

        other => return Err(RpcError::not_implemented(other.name())),
    })
}
