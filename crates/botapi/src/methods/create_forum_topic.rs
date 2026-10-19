//! `createForumTopic` — https://core.telegram.org/bots/api#createforumtopic

use simula_botapi_types::{methods, types as bot};
use simula_tl::{enums, functions};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};
use crate::forums;

pub async fn handle(ctx: BotCtx, params: methods::CreateForumTopic) -> BotResult<bot::ForumTopic> {
    let forum = forums::forum(&ctx, &params.chat_id).await?;
    if !forum.can_create_topics() {
        return Err(BotError::bad_request("not enough rights to create a topic"));
    }
    let name = forums::clean_name(&params.name);
    if name.is_empty() {
        return Err(BotError::bad_request("name must be non-empty"));
    }
    // telegram-bot-api's `get_integer_arg` (default 0), then tdlib's range check.
    let icon_color = params
        .icon_color
        .unwrap_or(0)
        .clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32;
    if !(0..=0xFF_FFFF).contains(&icon_color) {
        return Err(BotError::bad_request("invalid icon color specified"));
    }
    let emoji = params
        .icon_custom_emoji_id
        .as_deref()
        .map(forums::parse_emoji_id)
        .filter(|&id| id != 0);
    let updates = ctx
        .call(
            functions::messages::CreateForumTopic {
                peer: forum.input_peer(&ctx),
                title: name,
                icon_color: Some(icon_color),
                icon_emoji_id: emoji,
                random_id: rand::random(),
                ..Default::default()
            },
            ErrorContext::Plain,
        )
        .await?;
    created(&updates).ok_or_else(|| BotError::internal("no topic in the result"))
}

/// The topic from its creation message.
fn created(updates: &enums::Updates) -> Option<bot::ForumTopic> {
    let enums::Updates::Updates(batch) = updates else {
        return None;
    };
    batch.updates.iter().find_map(|update| {
        let enums::Update::NewChannelMessage(u) = update else {
            return None;
        };
        let enums::Message::Service(s) = &u.message else {
            return None;
        };
        let enums::MessageAction::TopicCreate(a) = &s.action else {
            return None;
        };
        Some(bot::ForumTopic {
            message_thread_id: s.id.into(),
            name: a.title.clone(),
            icon_color: a.icon_color.into(),
            icon_custom_emoji_id: forums::emoji_id(a.icon_emoji_id),
            is_name_implicit: a.title_missing,
        })
    })
}
