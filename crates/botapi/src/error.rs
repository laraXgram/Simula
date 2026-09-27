//! Bot API errors: `{"ok": false, "error_code": 400, "description": "Bad Request: …"}`.

use simula_core::RpcError;

use crate::errors::{botapi, tdlib};

#[derive(Debug, Clone, PartialEq)]
pub struct BotError {
    pub code: u16,
    pub description: String,
    /// `parameters.retry_after` for 429 responses.
    pub retry_after: Option<i64>,
    /// `parameters.migrate_to_chat_id` when a group became a supergroup.
    pub migrate_to_chat_id: Option<i64>,
}

pub type BotResult<T> = Result<T, BotError>;

impl BotError {
    pub fn new(code: u16, description: impl Into<String>) -> Self {
        Self {
            code,
            description: description.into(),
            retry_after: None,
            migrate_to_chat_id: None,
        }
    }

    pub fn bad_request(message: impl std::fmt::Display) -> Self {
        Self::new(400, format!("Bad Request: {message}"))
    }

    pub fn forbidden(message: impl std::fmt::Display) -> Self {
        Self::new(403, format!("Forbidden: {message}"))
    }

    pub fn unauthorized() -> Self {
        Self::new(401, "Unauthorized")
    }

    pub fn not_found() -> Self {
        Self::new(404, "Not Found")
    }

    pub fn conflict(message: impl std::fmt::Display) -> Self {
        Self::new(409, format!("Conflict: {message}"))
    }

    /// A documented method Simula doesn't simulate yet. Not a Telegram response.
    pub fn not_implemented(method: &str) -> Self {
        Self::new(
            501,
            format!("Not Implemented: Simula doesn't support {method} yet"),
        )
    }

    pub fn internal(error: impl std::fmt::Display) -> Self {
        tracing::error!(%error, "internal error");
        Self::new(500, "Internal Server Error")
    }

    /// Translate an MTProto error the way tdlib and then telegram-bot-api would.
    pub fn from_rpc(error: &RpcError, context: ErrorContext) -> Self {
        let (code, message) = tdlib::translate(error.code, &error.message, context);
        botapi::format(code, &message)
    }
}

/// Which tdlib path the call took; tdlib only rewrites errors of some.
#[derive(Debug, Clone, Copy, Default)]
pub enum ErrorContext {
    /// Errors pass through tdlib unchanged.
    #[default]
    Plain,
    /// Sending a message: tdlib's `on_send_message_fail` rewrites several errors, some
    /// only for private chats (with a user or bot).
    Send { private_chat: bool },
}

impl From<serde_json::Error> for BotError {
    fn from(error: serde_json::Error) -> Self {
        Self::internal(error)
    }
}
