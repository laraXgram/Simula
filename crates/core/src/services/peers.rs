//! Turning client-supplied `InputPeer`/`InputUser` into peers, checking access hashes
//! the way Telegram does, and building inputs for a given viewer.

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::{ChannelId, ChatId, Peer, UserId};
use crate::services::messages;
use crate::store;

pub fn resolve_input_peer(
    ctx: &Ctx<'_>,
    viewer: UserId,
    input: &enums::InputPeer,
) -> RpcResult<Peer> {
    match input {
        enums::InputPeer::InputPeerSelf => Ok(Peer::User(viewer)),
        enums::InputPeer::User(p) => {
            resolve_user(ctx, viewer, p.user_id, p.access_hash).map(Peer::User)
        }
        enums::InputPeer::Chat(p) => Ok(Peer::Chat(ChatId(p.chat_id))),
        enums::InputPeer::Channel(p) => {
            let peer = Peer::Channel(ChannelId(p.channel_id));
            if p.access_hash != ctx.access_hash(viewer, peer) {
                return Err(RpcError::bad_request("CHANNEL_INVALID"));
            }
            Ok(peer)
        }
        _ => Err(RpcError::peer_id_invalid()),
    }
}

pub fn resolve_input_user(
    ctx: &Ctx<'_>,
    viewer: UserId,
    input: &enums::InputUser,
) -> RpcResult<UserId> {
    match input {
        enums::InputUser::InputUserSelf => Ok(viewer),
        enums::InputUser::InputUser(p) => resolve_user(ctx, viewer, p.user_id, p.access_hash),
        _ => Err(RpcError::user_id_invalid()),
    }
}

fn resolve_user(
    ctx: &Ctx<'_>,
    viewer: UserId,
    user_id: i64,
    access_hash: i64,
) -> RpcResult<UserId> {
    let id = UserId(user_id);
    if store::accounts::get(ctx.db, id)?.is_none() {
        return Err(RpcError::peer_id_invalid());
    }
    let valid = access_hash == ctx.access_hash(viewer, Peer::User(id))
        // Bots may pass 0 for users they have met (tdlib does when it has no hash).
        || (access_hash == 0 && ctx.account(viewer)?.is_bot && knows(ctx, viewer, id)?);
    if !valid {
        return Err(RpcError::peer_id_invalid());
    }
    Ok(id)
}

/// Whether `viewer` has met `user`: they share a dialog, or `user` wrote (or did
/// something) in a chat `viewer` was in, so `viewer` saw them.
fn knows(ctx: &Ctx<'_>, viewer: UserId, user: UserId) -> RpcResult<bool> {
    if viewer == user || messages::has_dialog(ctx, viewer, Peer::User(user))? {
        return Ok(true);
    }
    Ok(ctx.db.query_row(
        "SELECT EXISTS (SELECT 1 FROM messages WHERE owner_id = ?1 AND from_id = ?2)",
        rusqlite::params![viewer.0, Peer::User(user).bot_api_id()],
        |row| row.get(0),
    )?)
}

/// The `InputPeer` that `viewer` would send to address `peer`.
pub fn input_peer(ctx: &Ctx<'_>, viewer: UserId, peer: Peer) -> enums::InputPeer {
    match peer {
        Peer::User(id) if id == viewer => enums::InputPeer::InputPeerSelf,
        _ => input_peer_with_hash(peer, ctx.access_hash(viewer, peer)),
    }
}

pub fn input_peer_with_hash(peer: Peer, access_hash: i64) -> enums::InputPeer {
    match peer {
        Peer::User(id) => types::InputPeerUser {
            user_id: id.0,
            access_hash,
        }
        .into(),
        Peer::Chat(id) => types::InputPeerChat { chat_id: id.0 }.into(),
        Peer::Channel(id) => types::InputPeerChannel {
            channel_id: id.0,
            access_hash,
        }
        .into(),
    }
}
