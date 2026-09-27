//! `sendMessage`

use simula_botapi_types::{methods, types as bot};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult};
use crate::send::{self, Content};
use crate::text;

pub async fn handle(ctx: BotCtx, params: methods::SendMessage) -> BotResult<bot::Message> {
    // telegram-bot-api's `get_input_message_text`, before anything else.
    if params.text.is_empty() {
        return Err(BotError::bad_request("message text is empty"));
    }
    let text = text::parse(
        &params.text,
        params.parse_mode.as_deref(),
        params.entities.as_deref(),
    )?;
    let content = Content::Text {
        text,
        link_preview: params.link_preview_options,
    };
    send::send(&ctx, send::options!(params), content).await
}
