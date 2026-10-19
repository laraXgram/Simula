//! `unpinAllForumTopicMessages` — https://core.telegram.org/bots/api#unpinallforumtopicmessages

use simula_botapi_types::methods;

use crate::ctx::BotCtx;
use crate::error::BotResult;
use crate::forums;
use crate::pins;

pub async fn handle(ctx: BotCtx, params: methods::UnpinAllForumTopicMessages) -> BotResult<bool> {
    let forum = forums::forum(&ctx, &params.chat_id).await?;
    let topic = forums::check_topic_id(params.message_thread_id)?;
    pins::unpin_all(&ctx, forum.peer, Some(topic)).await?;
    Ok(true)
}
