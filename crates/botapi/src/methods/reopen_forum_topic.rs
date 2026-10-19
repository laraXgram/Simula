//! `reopenForumTopic` — https://core.telegram.org/bots/api#reopenforumtopic

use simula_botapi_types::methods;
use simula_tl::functions;

use crate::ctx::BotCtx;
use crate::error::BotResult;
use crate::forums;

pub async fn handle(ctx: BotCtx, params: methods::ReopenForumTopic) -> BotResult<bool> {
    let forum = forums::forum(&ctx, &params.chat_id).await?;
    let topic = forums::check_topic_id(params.message_thread_id)?;
    forum
        .check_own(
            &ctx,
            topic,
            forum.can_manage_topics(),
            "not enough rights to close or open the topic",
        )
        .await?;
    forum
        .edit(
            &ctx,
            functions::messages::EditForumTopic {
                topic_id: topic,
                closed: Some(false),
                ..Default::default()
            },
        )
        .await?;
    Ok(true)
}
