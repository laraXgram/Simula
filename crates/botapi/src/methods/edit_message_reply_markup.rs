//! `editMessageReplyMarkup`

use simula_botapi_types::{methods, types as bot};

use crate::ctx::BotCtx;
use crate::edit::{self, Change, Target};
use crate::error::BotResult;

pub async fn handle(
    ctx: BotCtx,
    params: methods::EditMessageReplyMarkup,
) -> BotResult<bot::MessageOrTrue> {
    let target = Target {
        business_connection_id: params.business_connection_id,
        chat_id: params.chat_id,
        message_id: params.message_id,
        inline_message_id: params.inline_message_id,
    };
    edit::edit(&ctx, target, params.reply_markup, Change::ReplyMarkup).await
}
