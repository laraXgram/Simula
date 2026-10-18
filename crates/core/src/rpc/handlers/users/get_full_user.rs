//! `users.getFullUser`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::render;
use crate::services::{commands, peers};

/// A profile; for bots with their description and the commands this viewer sees.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::users::GetFullUser,
) -> RpcResult<enums::users::UserFull> {
    let viewer = ctx.caller()?.account;
    let id = peers::resolve_input_user(ctx, viewer, &req.id)?;
    let account = ctx.account(id)?;
    let bot_text = |key: &str| {
        account
            .extra
            .pointer(&format!("/bot/{key}"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
    };
    let bot_info = if account.is_bot {
        let commands = commands::for_private_chat(ctx, id, viewer)?;
        Some(
            types::BotInfo {
                user_id: Some(id.0),
                description: bot_text("description"),
                commands: Some(commands),
                menu_button: Some(enums::BotMenuButton::Commands),
                ..Default::default()
            }
            .into(),
        )
    } else {
        None
    };
    let full_user = types::UserFull {
        id: id.0,
        about: if account.is_bot {
            bot_text("short_description")
        } else {
            account
                .extra
                .get("about")
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        },
        settings: types::PeerSettings::default().into(),
        notify_settings: types::PeerNotifySettings::default().into(),
        bot_info,
        ..Default::default()
    };
    let viewer_account = ctx.account(viewer)?;
    Ok(types::users::UserFull {
        full_user: full_user.into(),
        chats: vec![],
        users: vec![render::user(ctx, &viewer_account, &account)],
    }
    .into())
}
