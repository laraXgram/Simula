//! `deleteMessages`

use simula_botapi_types::methods;

use crate::ctx::BotCtx;
use crate::delete;
use crate::error::{BotError, BotResult};
use crate::lookup;
use crate::resolve;

/// telegram-bot-api's `get_message_ids` limit.
const MAX_MESSAGES: usize = 100;

/// Messages that don't exist are skipped, as in telegram-bot-api (`allow_empty`).
pub async fn handle(ctx: BotCtx, params: methods::DeleteMessages) -> BotResult<bool> {
    if params.message_ids.len() > MAX_MESSAGES {
        return Err(BotError::bad_request(
            "too many message identifiers specified",
        ));
    }
    let mut ids = Vec::with_capacity(params.message_ids.len());
    for &id in &params.message_ids {
        match i32::try_from(id) {
            Ok(id) if id > 0 => ids.push(id),
            Ok(_) => {
                return Err(BotError::bad_request(
                    "invalid message identifier specified",
                ));
            }
            Err(_) => {
                return Err(BotError::bad_request(
                    "can't parse message identifier as a Number",
                ));
            }
        }
    }
    let chat = resolve::chat(&ctx, &params.chat_id).await?;
    let messages = lookup::messages(&ctx, chat, &ids).await?;
    delete::delete(&ctx, &messages).await
}
