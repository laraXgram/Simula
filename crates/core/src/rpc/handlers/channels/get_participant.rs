//! `channels.getParticipant`

use std::collections::BTreeSet;

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::Peer;
use crate::render;
use crate::services::{channels, peers};

pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::channels::GetParticipant,
) -> RpcResult<enums::channels::ChannelParticipant> {
    let caller = ctx.caller()?.account;
    let id = peers::resolve_input_channel(ctx, caller, &req.channel)?;
    let channel = channels::get(ctx, id)?;
    channels::require_reader(ctx, &channel, caller)?;
    let Peer::User(user) = peers::resolve_input_peer(ctx, caller, &req.participant)? else {
        return Err(RpcError::bad_request("PARTICIPANT_ID_INVALID"));
    };
    let participant = channels::member(ctx, id, user)?
        .and_then(|m| channels::participant(&channel, caller, &m).map(|p| (m, p)));
    let Some((member, participant)) = participant else {
        return Err(RpcError::bad_request("USER_NOT_PARTICIPANT"));
    };
    let mut users = BTreeSet::from([user]);
    users.extend(member.promoted_by);
    users.extend(member.kicked_by);
    Ok(types::channels::ChannelParticipant {
        participant,
        chats: vec![render::channel(ctx, caller, &channel)?],
        users: render::users(ctx, caller, &users)?,
    }
    .into())
}
