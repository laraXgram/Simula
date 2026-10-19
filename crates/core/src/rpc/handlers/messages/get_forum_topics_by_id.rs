//! `messages.getForumTopicsByID`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::services::{channels, forums};
use crate::store;

/// The named topics that exist, in request order.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::GetForumTopicsByID,
) -> RpcResult<enums::messages::ForumTopics> {
    let caller = ctx.caller()?.account;
    let id = forums::resolve(ctx, caller, &req.peer)?;
    let channel = forums::get_forum(ctx, id)?;
    channels::require_reader(ctx, &channel, caller)?;
    let mut found = Vec::new();
    for &topic in &req.topics {
        if let Some(topic) = store::topics::get(ctx.db, id, topic)? {
            found.push(topic);
        }
    }
    let count = found.len();
    forums::topics_result(ctx, caller, &channel, &found, count)
}
