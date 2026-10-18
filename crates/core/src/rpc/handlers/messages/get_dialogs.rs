//! `messages.getDialogs`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::render;
use crate::services::dialogs;

/// The chat list, newest first, paged by the last dialog's top message date and id.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::GetDialogs,
) -> RpcResult<enums::messages::Dialogs> {
    if req.folder_id.is_some_and(|folder| folder != 0) {
        return Err(RpcError::not_implemented("archived chats (folder 1)"));
    }
    let owner = ctx.caller()?.account;
    let limit = req.limit.clamp(1, 100) as usize;
    let list = dialogs::list(ctx, owner, req.offset_date, req.offset_id, limit)?;
    let users = render::users(ctx, owner, &list.users)?;
    let chats = render::all_chats(ctx, owner, &list.chats, &list.channels)?;
    Ok(if list.dialogs.len() < list.total {
        types::messages::DialogsSlice {
            count: list.total as i32,
            dialogs: list.dialogs,
            messages: list.messages,
            chats: chats.clone(),
            users,
        }
        .into()
    } else {
        types::messages::Dialogs {
            dialogs: list.dialogs,
            messages: list.messages,
            chats,
            users,
        }
        .into()
    })
}
