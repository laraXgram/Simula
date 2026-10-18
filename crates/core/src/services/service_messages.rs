//! Service messages in per-account boxes (private chats and basic groups): one copy per
//! recipient, each with its own id and pts, optionally replying to a message (a pinned
//! message's notice points at it, in each owner's own ids).

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::ids::{Peer, UserId};
use crate::services::{chats, updates};
use crate::store;

/// Post `action` by `from` into each `(owner, dialog as the owner sees it)`. With
/// `reply_to` (the copies' `global_id`), every copy replies to the owner's copy of it.
pub fn post(
    ctx: &mut Ctx<'_>,
    from: UserId,
    copies: &[(UserId, Peer)],
    action: enums::MessageAction,
    reply_to: Option<i64>,
) -> RpcResult<()> {
    let date = ctx.now();
    let global_id = store::meta::next(ctx.db, "global_message", 1)?;
    let mut mentioned: Vec<UserId> = vec![from];
    mentioned.extend(chats::action_users(&action));
    for &(owner, peer) in copies {
        let reply_header = match reply_to {
            Some(global) => store::messages::copy_id(ctx.db, owner, global)?.map(|id| {
                types::MessageReplyHeader {
                    reply_to_msg_id: Some(id),
                    ..Default::default()
                }
                .into()
            }),
            None => None,
        };
        let message = types::MessageService {
            out: owner == from,
            id: store::messages::next_id(ctx.db, owner)?,
            from_id: Some(Peer::User(from).to_tl()),
            peer_id: peer.to_tl(),
            reply_to: reply_header,
            date,
            action: action.clone(),
            ..Default::default()
        };
        store::messages::insert_service(ctx.db, owner, peer, global_id, &message)?;
        if let Peer::Chat(chat) = peer {
            ctx.out.mention_chat(owner, chat);
        }
        updates::push_pts(ctx, owner, mentioned.clone(), |pts, pts_count| {
            types::UpdateNewMessage {
                message: message.clone().into(),
                pts,
                pts_count,
            }
            .into()
        })?;
    }
    Ok(())
}
