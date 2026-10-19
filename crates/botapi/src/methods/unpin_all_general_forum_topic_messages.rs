//! `unpinAllGeneralForumTopicMessages` — https://core.telegram.org/bots/api#unpinallgeneralforumtopicmessages

use simula_botapi_types::methods;

use crate::ctx::BotCtx;
use crate::error::BotResult;
use crate::forums;
use crate::pins;

pub async fn handle(
    ctx: BotCtx,
    params: methods::UnpinAllGeneralForumTopicMessages,
) -> BotResult<bool> {
    let forum = forums::forum(&ctx, &params.chat_id).await?;
    pins::unpin_all(&ctx, forum.peer, Some(forums::GENERAL)).await?;
    Ok(true)
}
