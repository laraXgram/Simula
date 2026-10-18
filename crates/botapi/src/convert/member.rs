//! Basic group membership → Bot API `ChatMember` / `ChatMemberUpdated` (telegram-bot-api's
//! `JsonChatMember`, from tdlib's statuses: no participant is "left").

use simula_botapi_types::types as bot;
use simula_core::{ChatId, Peer};
use simula_tl::{enums, types};

use super::{Entities, chat::chat, user::user};

/// `my_chat_member` / `chat_member` for an `updateChatParticipant`.
pub fn updated(u: &types::UpdateChatParticipant, context: &Entities<'_>) -> bot::ChatMemberUpdated {
    let person = |id: i64| {
        context
            .user(id)
            .map(|found| user(found, false))
            .unwrap_or(bot::User {
                id,
                ..Default::default()
            })
    };
    let member =
        |participant: Option<&enums::ChatParticipant>| status(participant, person(u.user_id));
    bot::ChatMemberUpdated {
        chat: chat(Peer::Chat(ChatId(u.chat_id)), context),
        from: person(u.actor_id),
        date: u.date.into(),
        old_chat_member: member(u.prev_participant.as_ref()),
        new_chat_member: member(u.new_participant.as_ref()),
        ..Default::default()
    }
}

/// A basic group member's status. Basic group admins hold every right that applies.
pub fn status(participant: Option<&enums::ChatParticipant>, user: bot::User) -> bot::ChatMember {
    match participant {
        None => bot::ChatMemberLeft {
            user,
            ..Default::default()
        }
        .into(),
        Some(enums::ChatParticipant::Creator(creator)) => bot::ChatMemberOwner {
            user,
            is_anonymous: false,
            custom_title: creator.rank.clone(),
            ..Default::default()
        }
        .into(),
        Some(enums::ChatParticipant::Admin(admin)) => bot::ChatMemberAdministrator {
            user,
            can_be_edited: false,
            can_manage_chat: true,
            can_delete_messages: true,
            can_manage_video_chats: true,
            can_restrict_members: true,
            can_change_info: true,
            can_invite_users: true,
            can_pin_messages: Some(true),
            custom_title: admin.rank.clone(),
            ..Default::default()
        }
        .into(),
        Some(enums::ChatParticipant::ChatParticipant(_)) => bot::ChatMemberMember {
            user,
            ..Default::default()
        }
        .into(),
    }
}
