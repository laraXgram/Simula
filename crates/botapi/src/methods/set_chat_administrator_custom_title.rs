//! `setChatAdministratorCustomTitle` — https://core.telegram.org/bots/api#setchatadministratorcustomtitle

use simula_botapi_types::methods;
use simula_tl::enums;

use crate::admin;
use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult};
use crate::resolve::{self, Access};

/// Supergroups only; the bot may retitle only admins it can edit.
pub async fn handle(
    ctx: BotCtx,
    params: methods::SetChatAdministratorCustomTitle,
) -> BotResult<bool> {
    let chat = resolve::chat_for(&ctx, &params.chat_id, Access::Write).await?;
    admin::require_channel(chat, "method is available only for supergroups")?;
    match admin::participant(&ctx, chat, params.user_id).await? {
        Some(enums::ChannelParticipant::Admin(a)) => {
            if !a.can_edit {
                return Err(BotError::bad_request(
                    "not enough rights to change custom title of the user",
                ));
            }
            let enums::ChatAdminRights::ChatAdminRights(rights) = a.admin_rights;
            admin::edit_admin(
                &ctx,
                chat,
                params.user_id,
                *rights,
                Some(params.custom_title),
            )
            .await?;
            Ok(true)
        }
        Some(enums::ChannelParticipant::Creator(_)) => Err(BotError::bad_request(
            "not enough rights to change custom title of the user",
        )),
        _ => Err(BotError::bad_request("user is not an administrator")),
    }
}
