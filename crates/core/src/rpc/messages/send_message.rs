//! `messages.sendMessage`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::Peer;
use crate::render;
use crate::services::{messages, peers};

pub fn handle(ctx: &mut Ctx<'_>, req: functions::messages::SendMessage) -> RpcResult<enums::Updates> {
    let caller = ctx.caller()?;
    let Peer::User(to) = peers::resolve_input_peer(ctx, caller.account, &req.peer)? else {
        return Err(RpcError::not_implemented("messages.sendMessage to groups and channels"));
    };

    let sent = messages::send_private(
        ctx,
        caller.account,
        to,
        messages::Outgoing {
            text: req.message,
            entities: req.entities.unwrap_or_default(),
            reply_markup: req.reply_markup,
            random_id: Some(req.random_id),
            silent: req.silent,
        },
    )?;

    // The caller learns the new id through updateMessageID, then gets its own copy.
    let mentioned = ctx.out.users_for(caller.account);
    Ok(types::Updates {
        updates: vec![
            types::UpdateMessageID { id: sent.message.id, random_id: req.random_id }.into(),
            sent.update,
        ],
        users: render::users(ctx, caller.account, &mentioned)?,
        chats: vec![],
        date: ctx.now(),
        seq: 0,
    }
    .into())
}
