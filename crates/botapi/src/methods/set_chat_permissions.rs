//! `setChatPermissions` — https://core.telegram.org/bots/api#setchatpermissions

use simula_botapi_types::methods;
use simula_core::Peer;
use simula_tl::functions;

use crate::convert::permissions;
use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};
use crate::resolve::{self, Access};

/// Default permissions of a group or supergroup. tdlib treats "not modified" as done.
pub async fn handle(ctx: BotCtx, params: methods::SetChatPermissions) -> BotResult<bool> {
    let chat = resolve::chat_for(&ctx, &params.chat_id, Access::Write).await?;
    match chat {
        Peer::User(_) => {
            return Err(BotError::bad_request(
                "can't change chat permissions in private chats",
            ));
        }
        Peer::Channel(_) => {
            if resolve::channel(&ctx, chat, Access::Write).await?.broadcast {
                return Err(BotError::bad_request(
                    "can't change channel chat permissions",
                ));
            }
        }
        Peer::Chat(_) => {}
    }
    let rights = permissions::to_banned(
        &params.permissions,
        params.use_independent_chat_permissions == Some(true),
    );
    let result = ctx
        .call(
            functions::messages::EditChatDefaultBannedRights {
                peer: ctx.input_peer(chat),
                banned_rights: rights.into(),
            },
            ErrorContext::Plain,
        )
        .await;
    match result {
        Ok(_) => Ok(true),
        Err(e) if e.description.ends_with("CHAT_NOT_MODIFIED") => Ok(true),
        Err(e) if e.description.ends_with("CHAT_ADMIN_REQUIRED") => Err(BotError::bad_request(
            "not enough rights to change chat permissions",
        )),
        Err(e) => Err(e),
    }
}
