//! Forum topics as tdlib's `ForumTopicManager` handles them for a bot: the chat must be
//! a forum, rights are checked from the bot's own status before the server is asked
//! (with tdlib's texts), and a topic the bot created counts as its own ("outgoing"):
//! it may edit, close and delete it without `can_manage_topics`.

use simula_botapi_types::ChatId;
use simula_core::Peer;
use simula_tl::{enums, functions, types};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};
use crate::resolve::{self, Access};

/// General's topic id; the Bot API never names it by `message_thread_id`.
pub const GENERAL: i32 = 1;
const MAX_NAME_LENGTH: usize = 128;

/// A forum supergroup and the bot's view of it.
pub struct Forum {
    pub peer: Peer,
    pub channel: types::Channel,
}

/// `chat_id` after telegram-bot-api's `check_chat` (write access), as a forum.
pub async fn forum(ctx: &BotCtx, chat_id: &ChatId) -> BotResult<Forum> {
    let peer = resolve::chat_for(ctx, chat_id, Access::Write).await?;
    let not_forum = || BotError::bad_request("the chat is not a forum");
    let Peer::Channel(_) = peer else {
        return Err(not_forum());
    };
    let channel = resolve::channel(ctx, peer, Access::Write).await?;
    if !channel.forum {
        return Err(not_forum());
    }
    Ok(Forum { peer, channel })
}

impl Forum {
    fn admin_right(&self, right: impl Fn(&types::ChatAdminRights) -> bool) -> bool {
        self.channel.creator
            || matches!(&self.channel.admin_rights, Some(enums::ChatAdminRights::ChatAdminRights(r)) if right(r))
    }

    /// `can_edit_topics` / `can_pin_topics`: only admins granted `can_manage_topics`.
    pub fn can_manage_topics(&self) -> bool {
        self.admin_right(|r| r.manage_topics)
    }

    /// `can_create_topics`: admins with the right, or members the permissions allow.
    pub fn can_create_topics(&self) -> bool {
        let banned = |rights: &Option<enums::ChatBannedRights>| matches!(rights, Some(enums::ChatBannedRights::ChatBannedRights(r)) if r.manage_topics);
        let admin = self.channel.creator || self.channel.admin_rights.is_some();
        self.can_manage_topics()
            || (!admin
                && !banned(&self.channel.banned_rights)
                && !banned(&self.channel.default_banned_rights))
    }

    pub fn can_delete_messages(&self) -> bool {
        self.admin_right(|r| r.delete_messages)
    }

    pub fn input_peer(&self, ctx: &BotCtx) -> enums::InputPeer {
        ctx.input_peer(self.peer)
    }

    /// The topic as the server has it, if it exists (tdlib's cached topic info).
    pub async fn topic(&self, ctx: &BotCtx, id: i32) -> BotResult<Option<types::ForumTopic>> {
        let found = ctx
            .call(
                functions::messages::GetForumTopicsByID {
                    peer: self.input_peer(ctx),
                    topics: vec![id],
                },
                ErrorContext::Plain,
            )
            .await?;
        let enums::messages::ForumTopics::ForumTopics(found) = found;
        Ok(found.topics.into_iter().find_map(|t| match t {
            enums::ForumTopic::ForumTopic(t) if t.id == id => Some(*t),
            _ => None,
        }))
    }

    /// tdlib's check before editing, closing or deleting a topic: without the admin
    /// right, a known topic must be the bot's own.
    pub async fn check_own(
        &self,
        ctx: &BotCtx,
        id: i32,
        allowed: bool,
        error: &str,
    ) -> BotResult<()> {
        if allowed {
            return Ok(());
        }
        match self.topic(ctx, id).await? {
            Some(topic) if !topic.my => Err(BotError::bad_request(error)),
            _ => Ok(()),
        }
    }

    /// `messages.editForumTopic`.
    pub async fn edit(
        &self,
        ctx: &BotCtx,
        edit: functions::messages::EditForumTopic,
    ) -> BotResult<()> {
        ctx.call(
            functions::messages::EditForumTopic {
                peer: self.input_peer(ctx),
                ..edit
            },
            ErrorContext::Plain,
        )
        .await?;
        Ok(())
    }
}

/// tdlib's `can_be_forum_topic_id`.
pub fn check_topic_id(id: i64) -> BotResult<i32> {
    match i32::try_from(id) {
        Ok(id) if id > 0 => Ok(id),
        _ => Err(BotError::bad_request(
            "invalid forum topic identifier specified",
        )),
    }
}

/// tdlib's `clean_name`: whitespace runs become one space, the ends are trimmed, and the
/// name is cut to 128 characters.
pub fn clean_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut space = false;
    for c in name.chars() {
        if c.is_whitespace() || c.is_control() {
            space = true;
            continue;
        }
        if space && !out.is_empty() {
            out.push(' ');
        }
        space = false;
        out.push(c);
    }
    out.chars().take(MAX_NAME_LENGTH).collect()
}

/// A topic's custom emoji id as the Bot API writes it.
pub fn emoji_id(id: Option<i64>) -> Option<String> {
    id.filter(|&id| id != 0).map(|id| id.to_string())
}

/// telegram-bot-api's `td::to_integer<int64>` for custom emoji ids.
pub fn parse_emoji_id(id: &str) -> i64 {
    id.trim().parse().unwrap_or(0)
}
