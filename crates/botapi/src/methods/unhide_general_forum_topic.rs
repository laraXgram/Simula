//! `unhideGeneralForumTopic` — https://core.telegram.org/bots/api#unhidegeneralforumtopic

use simula_botapi_types::methods;
use simula_tl::functions;

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult};
use crate::forums;

pub async fn handle(ctx: BotCtx, params: methods::UnhideGeneralForumTopic) -> BotResult<bool> {
    let forum = forums::forum(&ctx, &params.chat_id).await?;
    if !forum.can_manage_topics() {
        return Err(BotError::bad_request(
            "not enough rights to close or open the topic",
        ));
    }
    forum
        .edit(
            &ctx,
            functions::messages::EditForumTopic {
                topic_id: forums::GENERAL,
                hidden: Some(false),
                ..Default::default()
            },
        )
        .await?;
    Ok(true)
}
