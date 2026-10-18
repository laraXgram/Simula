//! `getChatMemberCount`

use simula_botapi_types::methods;
use simula_core::Peer;

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult};
use crate::members;
use crate::resolve::{self, Access};

pub async fn handle(ctx: BotCtx, params: methods::GetChatMemberCount) -> BotResult<i64> {
    match resolve::chat_for(&ctx, &params.chat_id, Access::Write).await? {
        Peer::User(other) => Ok(if other == ctx.bot.id { 1 } else { 2 }),
        Peer::Chat(chat) => Ok(members::group(&ctx, chat).await?.participants.len() as i64),
        Peer::Channel(_) => Err(BotError::not_implemented("channels and supergroups")),
    }
}
