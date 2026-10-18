//! `banChatMember` — https://core.telegram.org/bots/api#banchatmember

use simula_botapi_types::methods;
use simula_core::Peer;
use simula_tl::{functions, types};

use crate::admin;
use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};
use crate::resolve::{self, Access};

/// In basic groups the user is removed (`until_date` doesn't apply); in supergroups and
/// channels they are banned until `until_date`.
pub async fn handle(ctx: BotCtx, params: methods::BanChatMember) -> BotResult<bool> {
    match resolve::chat_for(&ctx, &params.chat_id, Access::Write).await? {
        Peer::User(_) => Err(BotError::bad_request(
            "chat member status can't be changed in private chats",
        )),
        Peer::Chat(chat) => {
            ctx.call(
                functions::messages::DeleteChatUser {
                    revoke_history: params.revoke_messages == Some(true),
                    chat_id: chat.0,
                    user_id: ctx.input_user(params.user_id),
                },
                ErrorContext::Plain,
            )
            .await?;
            Ok(true)
        }
        chat @ Peer::Channel(_) => {
            let rights = types::ChatBannedRights {
                view_messages: true,
                send_messages: true,
                send_media: true,
                send_stickers: true,
                send_gifs: true,
                send_games: true,
                send_inline: true,
                embed_links: true,
                until_date: params.until_date.unwrap_or(0).clamp(0, i64::from(i32::MAX)) as i32,
                ..Default::default()
            };
            admin::edit_banned(&ctx, chat, params.user_id, rights).await?;
            Ok(true)
        }
    }
}
