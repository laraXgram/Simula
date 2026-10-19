//! `editForumTopic` — https://core.telegram.org/bots/api#editforumtopic

use simula_botapi_types::methods;
use simula_tl::functions;

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult};
use crate::forums;

pub async fn handle(ctx: BotCtx, params: methods::EditForumTopic) -> BotResult<bool> {
    let forum = forums::forum(&ctx, &params.chat_id).await?;
    let topic = forums::check_topic_id(params.message_thread_id)?;
    forum
        .check_own(
            &ctx,
            topic,
            forum.can_manage_topics(),
            "not enough rights to edit the topic",
        )
        .await?;
    // An empty name changes nothing; a present (even empty) emoji id sets the icon.
    let name = params.name.as_deref().unwrap_or_default();
    let title = (!name.is_empty()).then(|| forums::clean_name(name));
    if title.as_deref() == Some("") {
        return Err(BotError::bad_request("name must be non-empty"));
    }
    let emoji = params
        .icon_custom_emoji_id
        .as_deref()
        .map(forums::parse_emoji_id);
    if title.is_none() && emoji.is_none() {
        return Ok(true);
    }
    forum
        .edit(
            &ctx,
            functions::messages::EditForumTopic {
                topic_id: topic,
                title,
                icon_emoji_id: emoji,
                ..Default::default()
            },
        )
        .await?;
    Ok(true)
}
