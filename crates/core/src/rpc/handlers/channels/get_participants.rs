//! `channels.getParticipants`

use std::collections::BTreeSet;

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::render;
use crate::services::{channels, peers};
use crate::store::{self, channels::Status};

/// Members by filter: recent (all members), admins, bots, banned ("kicked"),
/// restricted ("banned"), or a name search. Channel subscribers are for admins only.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::channels::GetParticipants,
) -> RpcResult<enums::channels::ChannelParticipants> {
    let caller = ctx.caller()?.account;
    let id = peers::resolve_input_channel(ctx, caller, &req.channel)?;
    let channel = channels::get(ctx, id)?;
    let me = channels::require_member(ctx, &channel, caller)?;
    if channel.broadcast && !me.status.is_admin() {
        return Err(RpcError::bad_request("CHAT_ADMIN_REQUIRED"));
    }
    use enums::ChannelParticipantsFilter as F;
    let everyone = store::channels::members(ctx.db, id)?;
    let mut matching = Vec::new();
    for member in everyone {
        let account = ctx.account(member.user)?;
        let name = |q: &str| {
            let q = q.to_lowercase();
            q.is_empty()
                || [
                    Some(account.first_name.clone()),
                    account.last_name.clone(),
                    account.username.clone(),
                ]
                .into_iter()
                .flatten()
                .any(|n| n.to_lowercase().contains(&q))
        };
        let wanted = match &req.filter {
            F::ChannelParticipantsRecent => member.status.is_member(),
            F::ChannelParticipantsAdmins => member.status.is_admin(),
            F::ChannelParticipantsBots => member.status.is_member() && account.is_bot,
            F::ChannelParticipantsKicked(f) => member.status == Status::Banned && name(&f.q),
            F::ChannelParticipantsBanned(f) => member.status == Status::Restricted && name(&f.q),
            F::ChannelParticipantsSearch(f) => member.status.is_member() && name(&f.q),
            F::ChannelParticipantsContacts(_) | F::ChannelParticipantsMentions(_) => {
                return Err(RpcError::not_implemented(
                    "contacts and mentions participant filters",
                ));
            }
        };
        if wanted {
            matching.push(member);
        }
    }
    let count = matching.len() as i32;
    let page: Vec<_> = matching
        .into_iter()
        .skip(req.offset.max(0) as usize)
        .take(req.limit.clamp(0, 200) as usize)
        .collect();
    let mut users = BTreeSet::new();
    for member in &page {
        users.insert(member.user);
        users.extend(member.promoted_by);
        users.extend(member.kicked_by);
    }
    Ok(types::channels::ChannelParticipants {
        count,
        participants: page
            .iter()
            .filter_map(|m| channels::participant(&channel, caller, m))
            .collect(),
        chats: vec![render::channel(ctx, caller, &channel)?],
        users: render::users(ctx, caller, &users)?,
    }
    .into())
}
