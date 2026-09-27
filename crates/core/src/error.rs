//! MTProto RPC errors: a numeric code and an UPPER_SNAKE message, as Telegram sends them.
//! Gateways translate these (the Bot API maps `PEER_ID_INVALID` to "chat not found").

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, serde::Serialize, serde::Deserialize)]
#[error("{code} {message}")]
pub struct RpcError {
    pub code: i32,
    pub message: String,
}

impl RpcError {
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self { code, message: message.into() }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(400, message)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(401, message)
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(403, message)
    }

    /// A TL function Simula doesn't simulate yet. Not a real Telegram error.
    pub fn not_implemented(function: &str) -> Self {
        Self::new(501, format!("NOT_IMPLEMENTED_IN_SIMULA: {function}"))
    }

    pub fn internal(error: impl std::fmt::Display) -> Self {
        tracing::error!(%error, "internal error");
        Self::new(500, "INTERNAL_SERVER_ERROR")
    }

    pub fn peer_id_invalid() -> Self {
        Self::bad_request("PEER_ID_INVALID")
    }

    pub fn user_id_invalid() -> Self {
        Self::bad_request("USER_ID_INVALID")
    }

    pub fn message_empty() -> Self {
        Self::bad_request("MESSAGE_EMPTY")
    }

    pub fn message_too_long() -> Self {
        Self::bad_request("MESSAGE_TOO_LONG")
    }

    pub fn auth_key_unregistered() -> Self {
        Self::unauthorized("AUTH_KEY_UNREGISTERED")
    }
}

impl From<rusqlite::Error> for RpcError {
    fn from(error: rusqlite::Error) -> Self {
        Self::internal(error)
    }
}

impl From<serde_json::Error> for RpcError {
    fn from(error: serde_json::Error) -> Self {
        Self::internal(error)
    }
}

pub type RpcResult<T> = Result<T, RpcError>;
