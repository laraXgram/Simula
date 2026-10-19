//! `reopenGeneralForumTopic` — https://core.telegram.org/bots/api#reopengeneralforumtopic

use simula_botapi_types::methods;
use simula_tl::functions;

use crate::ctx::BotCtx;
use crate::error::BotResult;
use crate::forums;

pub async fn handle(ctx: BotCtx, params: methods::ReopenGeneralForumTopic) -> BotResult<bool> {
    let forum = forums::forum(&ctx, &params.chat_id).await?;
    forum
        .check_own(
            &ctx,
            forums::GENERAL,
            forum.can_manage_topics(),
            "not enough rights to close or open the topic",
        )
        .await?;
    forum
        .edit(
            &ctx,
            functions::messages::EditForumTopic {
                topic_id: forums::GENERAL,
                closed: Some(false),
                ..Default::default()
            },
        )
        .await?;
    Ok(true)
}
