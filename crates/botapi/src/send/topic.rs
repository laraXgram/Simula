//! `message_thread_id`: telegram-bot-api's `check_message_topic`. In forum supergroups
//! it names a topic, which must exist (General can't be named); private chats would
//! name a bot's topic, which Simula's bots don't have yet; elsewhere it is ignored.

use simula_core::Peer;

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult};
use crate::forums;
use crate::resolve::{self, Access};

/// The forum topic a message goes to, if any.
pub async fn check(ctx: &BotCtx, chat: Peer, thread: Option<i64>) -> BotResult<Option<i32>> {
    // telegram-bot-api's `get_forum_topic_id`: negative counts as none.
    let Some(thread) = thread.filter(|&id| id > 0) else {
        return Ok(None);
    };
    let not_found = || BotError::bad_request("message thread not found");
    match chat {
        Peer::Channel(_) => {
            let channel = resolve::channel(ctx, chat, Access::Write).await?;
            if channel.broadcast || !channel.forum {
                return Ok(None);
            }
            let topic = i32::try_from(thread).map_err(|_| not_found())?;
            if topic == forums::GENERAL {
                return Err(not_found());
            }
            let forum = forums::Forum {
                peer: chat,
                channel,
            };
            match forum.topic(ctx, topic).await? {
                Some(_) => Ok(Some(topic)),
                None => Err(not_found()),
            }
        }
        Peer::User(_) => Err(not_found()),
        Peer::Chat(_) => Ok(None),
    }
}
