//! `editMessageCaption` — https://core.telegram.org/bots/api#editmessagecaption

use simula_botapi_types::{methods, types as bot};

use crate::ctx::BotCtx;
use crate::edit::{self, Change, Target};
use crate::error::BotResult;
use crate::text;

pub async fn handle(
    ctx: BotCtx,
    params: methods::EditMessageCaption,
) -> BotResult<bot::MessageOrTrue> {
    let caption = text::parse(
        params.caption.as_deref().unwrap_or_default(),
        params.parse_mode.as_deref(),
        params.caption_entities.as_deref(),
    )?;
    let target = Target {
        business_connection_id: params.business_connection_id,
        chat_id: params.chat_id,
        message_id: params.message_id,
        inline_message_id: params.inline_message_id,
    };
    let change = Change::Caption {
        caption,
        above: params.show_caption_above_media == Some(true),
    };
    edit::edit(&ctx, target, params.reply_markup, change).await
}
