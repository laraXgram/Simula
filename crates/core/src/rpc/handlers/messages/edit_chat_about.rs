//! `messages.editChatAbout`

use simula_tl::functions;

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::ids::Peer;
use crate::services::{channels, chats, peers};

/// The description of a basic group, supergroup or channel.
pub fn handle(ctx: &mut Ctx<'_>, req: functions::messages::EditChatAbout) -> RpcResult<bool> {
    let caller = ctx.caller()?.account;
    match peers::resolve_input_peer(ctx, caller, &req.peer)? {
        Peer::Channel(channel) => channels::set_about(ctx, caller, channel, &req.about)?,
        Peer::Chat(chat) => chats::set_about(ctx, caller, chat, &req.about)?,
        Peer::User(_) => return Err(crate::error::RpcError::peer_id_invalid()),
    }
    Ok(true)
}
