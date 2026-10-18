//! `messages.editChatDefaultBannedRights`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::Peer;
use crate::render;
use crate::services::{channels, chats, peers};

/// What members of a group may do by default (as the rights they lack).
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::EditChatDefaultBannedRights,
) -> RpcResult<enums::Updates> {
    let caller = ctx.caller()?.account;
    let enums::ChatBannedRights::ChatBannedRights(rights) = req.banned_rights;
    match peers::resolve_input_peer(ctx, caller, &req.peer)? {
        Peer::Channel(channel) => channels::set_default_banned(ctx, caller, channel, *rights)?,
        Peer::Chat(chat) => chats::set_default_banned(ctx, caller, chat, *rights)?,
        Peer::User(_) => return Err(RpcError::peer_id_invalid()),
    }
    render::own_updates(ctx, caller)
}
