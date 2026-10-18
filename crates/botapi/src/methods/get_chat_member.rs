//! `getChatMember`

use simula_botapi_types::{methods, types as bot};
use simula_core::Peer;

use crate::ctx::BotCtx;
use crate::error::BotResult;
use crate::members;
use crate::resolve::{self, Access};

pub async fn handle(ctx: BotCtx, params: methods::GetChatMember) -> BotResult<bot::ChatMember> {
    // Reading other members counts as writing for groups the bot left (ReadMembers).
    let access = if params.user_id == ctx.bot.id.0 {
        Access::Read
    } else {
        Access::Write
    };
    match resolve::chat_for(&ctx, &params.chat_id, access).await? {
        Peer::Chat(chat) => Ok(members::group(&ctx, chat).await?.member(params.user_id)),
        Peer::User(other) => {
            // In a private chat, its two sides are members; nobody else is.
            let user = bot::User {
                id: params.user_id,
                ..Default::default()
            };
            Ok(
                if params.user_id == other.0 || params.user_id == ctx.bot.id.0 {
                    bot::ChatMemberMember {
                        user,
                        ..Default::default()
                    }
                    .into()
                } else {
                    bot::ChatMemberLeft {
                        user,
                        ..Default::default()
                    }
                    .into()
                },
            )
        }
        Peer::Channel(_) => Err(crate::error::BotError::not_implemented(
            "channels and supergroups",
        )),
    }
}
