//! `deleteForumTopic` — https://core.telegram.org/bots/api#deleteforumtopic

use simula_botapi_types::methods;
use simula_tl::functions;

use crate::ctx::BotCtx;
use crate::error::{BotResult, ErrorContext};
use crate::forums;

/// The topic goes with everything in it (`messages.deleteTopicHistory`).
pub async fn handle(ctx: BotCtx, params: methods::DeleteForumTopic) -> BotResult<bool> {
    let forum = forums::forum(&ctx, &params.chat_id).await?;
    let topic = forums::check_topic_id(params.message_thread_id)?;
    forum
        .check_own(
            &ctx,
            topic,
            forum.can_delete_messages(),
            "not enough rights to delete the topic",
        )
        .await?;
    ctx.call(
        functions::messages::DeleteTopicHistory {
            peer: forum.input_peer(&ctx),
            top_msg_id: topic,
        },
        ErrorContext::Plain,
    )
    .await?;
    Ok(true)
}
