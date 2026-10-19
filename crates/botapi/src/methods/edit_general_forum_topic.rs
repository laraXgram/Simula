//! `editGeneralForumTopic` — https://core.telegram.org/bots/api#editgeneralforumtopic

use simula_botapi_types::methods;
use simula_tl::functions;

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult};
use crate::forums;

pub async fn handle(ctx: BotCtx, params: methods::EditGeneralForumTopic) -> BotResult<bool> {
    let forum = forums::forum(&ctx, &params.chat_id).await?;
    forum
        .check_own(
            &ctx,
            forums::GENERAL,
            forum.can_manage_topics(),
            "not enough rights to edit the topic",
        )
        .await?;
    if params.name.is_empty() {
        return Ok(true);
    }
    let title = forums::clean_name(&params.name);
    if title.is_empty() {
        return Err(BotError::bad_request("name must be non-empty"));
    }
    forum
        .edit(
            &ctx,
            functions::messages::EditForumTopic {
                topic_id: forums::GENERAL,
                title: Some(title),
                ..Default::default()
            },
        )
        .await?;
    Ok(true)
}
