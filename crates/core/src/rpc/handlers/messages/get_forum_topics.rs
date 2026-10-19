//! `messages.getForumTopics`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::services::{channels, forums};

/// The forum's topics, pinned first, then by their newest message; `q` filters titles.
/// A page starts after `offset_topic` when it is given.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::GetForumTopics,
) -> RpcResult<enums::messages::ForumTopics> {
    let caller = ctx.caller()?.account;
    let id = forums::resolve(ctx, caller, &req.peer)?;
    let channel = forums::get_forum(ctx, id)?;
    channels::require_reader(ctx, &channel, caller)?;
    let query = req
        .q
        .as_deref()
        .map(str::to_lowercase)
        .filter(|q| !q.is_empty());
    let all: Vec<_> = forums::list(ctx, id)?
        .into_iter()
        .filter(|t| {
            query
                .as_ref()
                .is_none_or(|q| t.title.to_lowercase().contains(q))
        })
        .collect();
    let start = match req.offset_topic {
        0 => 0,
        after => all
            .iter()
            .position(|t| t.id == after)
            .map_or(all.len(), |i| i + 1),
    };
    let limit = req.limit.clamp(1, 100) as usize;
    let page: Vec<_> = all.iter().skip(start).take(limit).cloned().collect();
    forums::topics_result(ctx, caller, &channel, &page, all.len())
}
