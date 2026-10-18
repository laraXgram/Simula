//! `restrictChatMember` — https://core.telegram.org/bots/api#restrictchatmember

use simula_botapi_types::methods;

use crate::admin;
use crate::convert::permissions;
use crate::ctx::BotCtx;
use crate::error::BotResult;
use crate::resolve::{self, Access};

/// Supergroups only. Granting every permission lifts the restrictions.
pub async fn handle(ctx: BotCtx, params: methods::RestrictChatMember) -> BotResult<bool> {
    let chat = resolve::chat_for(&ctx, &params.chat_id, Access::Write).await?;
    admin::require_channel(chat, "method is available only for supergroups")?;
    let mut rights = permissions::to_banned(
        &params.permissions,
        params.use_independent_chat_permissions == Some(true),
    );
    rights.until_date = params.until_date.unwrap_or(0).clamp(0, i64::from(i32::MAX)) as i32;
    admin::edit_banned(&ctx, chat, params.user_id, rights).await?;
    Ok(true)
}
