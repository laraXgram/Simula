//! `editMessageText`

use simula_botapi_types::{methods, types as bot};

use crate::ctx::BotCtx;
use crate::edit::{self, Change, Target};
use crate::error::{BotError, BotResult};
use crate::text;

pub async fn handle(
    ctx: BotCtx,
    params: methods::EditMessageText,
) -> BotResult<bot::MessageOrTrue> {
    if params.rich_message.is_some() {
        return Err(BotError::not_implemented("rich messages"));
    }
    // telegram-bot-api's `get_input_message_text` comes first.
    let text = params.text.as_deref().unwrap_or("");
    if text.is_empty() {
        return Err(BotError::bad_request("message text is empty"));
    }
    let text = text::parse(
        text,
        params.parse_mode.as_deref(),
        params.entities.as_deref(),
    )?;
    let target = Target {
        business_connection_id: params.business_connection_id,
        chat_id: params.chat_id,
        message_id: params.message_id,
        inline_message_id: params.inline_message_id,
    };
    let change = Change::Text {
        text,
        link_preview: params.link_preview_options,
    };
    edit::edit(&ctx, target, params.reply_markup, change).await
}
