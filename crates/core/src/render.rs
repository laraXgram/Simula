//! Domain rows → TL objects, as a specific viewer sees them (access hashes, `self`,
//! phone visibility, bot-only fields).

use std::collections::BTreeSet;

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::ids::UserId;
use crate::store::accounts::Account;

pub fn user(ctx: &Ctx<'_>, viewer: &Account, account: &Account) -> enums::User {
    let is_self = viewer.id == account.id;
    let bot_setting = |key: &str| account.extra.pointer(&format!("/bot/{key}")).and_then(|v| v.as_bool());
    types::User {
        self_: is_self,
        bot: account.is_bot,
        premium: account.is_premium,
        // Privacy mode off = the bot sees all group messages.
        bot_chat_history: account.is_bot && bot_setting("privacy_mode") == Some(false),
        bot_nochats: account.is_bot && bot_setting("can_join_groups") == Some(false),
        id: account.id.0,
        access_hash: Some(ctx.access_hash(viewer.id, account.id.into())),
        first_name: Some(account.first_name.clone()),
        last_name: account.last_name.clone(),
        username: account.username.clone(),
        phone: if is_self { account.phone.clone() } else { None },
        bot_info_version: account.is_bot.then_some(1),
        // Bots learn their users' language; other users don't.
        lang_code: if viewer.is_bot { account.lang_code.clone() } else { None },
        ..Default::default()
    }
    .into()
}

/// Render several users for one viewer, skipping ids that no longer exist.
pub fn users(ctx: &Ctx<'_>, viewer: UserId, ids: &BTreeSet<UserId>) -> RpcResult<Vec<enums::User>> {
    let viewer = ctx.account(viewer)?;
    let mut out = Vec::with_capacity(ids.len());
    for &id in ids {
        if let Ok(account) = ctx.account(id) {
            out.push(user(ctx, &viewer, &account));
        }
    }
    Ok(out)
}
