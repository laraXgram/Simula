//! `setChatTitle`

use simula_botapi_types::methods;
use simula_core::Peer;
use simula_tl::functions;

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};
use crate::resolve::{self, Access};

pub async fn handle(ctx: BotCtx, params: methods::SetChatTitle) -> BotResult<bool> {
    match resolve::chat_for(&ctx, &params.chat_id, Access::Write).await? {
        Peer::User(_) => Err(BotError::bad_request("can't change private chat title")),
        Peer::Chat(chat) => {
            ctx.call(
                functions::messages::EditChatTitle {
                    chat_id: chat.0,
                    title: params.title,
                },
                ErrorContext::Plain,
            )
            .await?;
            Ok(true)
        }
        Peer::Channel(_) => Err(BotError::not_implemented("channels and supergroups")),
    }
}
