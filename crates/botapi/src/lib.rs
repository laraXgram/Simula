//! Simula's Telegram Bot API gateway. Like telegram-bot-api on top of TDLib, it
//! translates Bot API requests into TL calls on the core, converts the results and
//! updates back, and delivers updates by long polling (and, later, webhooks).

mod convert;
mod ctx;
mod dispatch;
pub mod error;
mod errors;
mod http;
mod methods;
mod resolve;
mod state;
mod updates;

use std::sync::Arc;

pub use error::{BotError, BotResult};
/// Bot API methods Simula has a handler for.
pub use methods::IMPLEMENTED as IMPLEMENTED_METHODS;
pub use state::Gateway;

/// Routes for `/bot<token>/<method>`.
pub fn router(gateway: Arc<Gateway>) -> axum::Router {
    http::router(gateway)
}
