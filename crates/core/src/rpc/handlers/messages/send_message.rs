//! `messages.sendMessage`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::ids::Peer;
use crate::render;
use crate::services::{channel_messages, group_messages, messages, peers};

pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::SendMessage,
) -> RpcResult<enums::Updates> {
    let caller = ctx.caller()?;
    let peer = peers::resolve_input_peer(ctx, caller.account, &req.peer)?;
    let out = messages::Outgoing {
        text: req.message,
        entities: req.entities.unwrap_or_default(),
        reply_markup: req.reply_markup,
        reply_to: req.reply_to,
        random_id: Some(req.random_id),
        silent: req.silent,
        noforwards: req.noforwards,
        no_webpage: req.no_webpage,
        invert_media: req.invert_media,
        effect: req.effect,
    };
    let sent = match peer {
        Peer::User(to) => messages::send_private(ctx, caller.account, to, out)?,
        Peer::Chat(chat) => group_messages::send(ctx, caller.account, chat, out)?,
        Peer::Channel(channel) => channel_messages::send(ctx, caller.account, channel, out)?,
    };

    // The caller learns the new id through updateMessageID, then gets its own copy.
    let mentioned = ctx.out.users_for(caller.account);
    Ok(types::Updates {
        updates: vec![
            types::UpdateMessageID {
                id: sent.message.id,
                random_id: req.random_id,
            }
            .into(),
            sent.update,
        ],
        users: render::users(ctx, caller.account, &mentioned)?,
        chats: render::all_chats(
            ctx,
            caller.account,
            &ctx.out.chats_for(caller.account),
            &ctx.out.channels_for(caller.account),
        )?,
        date: ctx.now(),
        seq: 0,
    }
    .into())
}
