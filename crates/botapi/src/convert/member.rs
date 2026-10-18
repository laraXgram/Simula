//! Group and channel membership → Bot API `ChatMember` / `ChatMemberUpdated`
//! (telegram-bot-api's `JsonChatMember`, from tdlib's statuses: no participant is
//! "left").

use simula_botapi_types::types as bot;
use simula_core::{ChannelId, ChatId, Peer};
use simula_tl::{enums, types};

use super::{Entities, chat::chat, invite, permissions, user::user};

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
        invite_link: u
            .invite
            .as_ref()
            .and_then(|i| invite::link(i, context))
            .map(Box::new),
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

/// `my_chat_member` / `chat_member` for an `updateChannelParticipant`.
pub fn updated_channel(
    u: &types::UpdateChannelParticipant,
    context: &Entities<'_>,
) -> bot::ChatMemberUpdated {
    let person = |id: i64| {
        context
            .user(id)
            .map(|found| user(found, false))
            .unwrap_or(bot::User {
                id,
                ..Default::default()
            })
    };
    let broadcast = context.channel(u.channel_id).is_some_and(|c| c.broadcast);
    let member = |participant: Option<&enums::ChannelParticipant>| {
        channel_status(participant, person(u.user_id), broadcast)
    };
    bot::ChatMemberUpdated {
        chat: chat(Peer::Channel(ChannelId(u.channel_id)), context),
        from: person(u.actor_id),
        date: u.date.into(),
        old_chat_member: member(u.prev_participant.as_ref()),
        new_chat_member: member(u.new_participant.as_ref()),
        invite_link: u
            .invite
            .as_ref()
            .and_then(|i| invite::link(i, context))
            .map(Box::new),
        via_join_request: matches!(
            u.invite,
            Some(enums::ExportedChatInvite::ChatInvitePublicJoinRequests)
        )
        .then_some(true),
        ..Default::default()
    }
}

/// A supergroup or channel member's status (tdlib's `DialogParticipantStatus`).
pub fn channel_status(
    participant: Option<&enums::ChannelParticipant>,
    user: bot::User,
    broadcast: bool,
) -> bot::ChatMember {
    use enums::ChannelParticipant as P;
    match participant {
        None | Some(P::Left(_)) => bot::ChatMemberLeft {
            user,
            ..Default::default()
        }
        .into(),
        Some(P::Creator(creator)) => {
            let enums::ChatAdminRights::ChatAdminRights(rights) = &creator.admin_rights;
            bot::ChatMemberOwner {
                user,
                is_anonymous: rights.anonymous,
                custom_title: creator.rank.clone(),
                ..Default::default()
            }
            .into()
        }
        Some(P::Admin(admin)) => {
            let enums::ChatAdminRights::ChatAdminRights(rights) = &admin.admin_rights;
            permissions::administrator(rights, broadcast, user, admin.can_edit, admin.rank.clone())
                .into()
        }
        Some(P::ChannelParticipant(p)) => bot::ChatMemberMember {
            user,
            until_date: p.subscription_until_date.map(Into::into),
            tag: p.rank.clone(),
            ..Default::default()
        }
        .into(),
        Some(P::ChannelParticipantSelf(p)) => bot::ChatMemberMember {
            user,
            until_date: p.subscription_until_date.map(Into::into),
            tag: p.rank.clone(),
            ..Default::default()
        }
        .into(),
        Some(P::Banned(banned)) => {
            let enums::ChatBannedRights::ChatBannedRights(rights) = &banned.banned_rights;
            let until = until_date(rights.until_date);
            if banned.left {
                return bot::ChatMemberBanned {
                    user,
                    until_date: until,
                    ..Default::default()
                }
                .into();
            }
            let allowed = permissions::from_banned(rights);
            let yes = |v: Option<bool>| v == Some(true);
            bot::ChatMemberRestricted {
                user,
                is_member: true,
                can_send_messages: yes(allowed.can_send_messages),
                can_send_audios: yes(allowed.can_send_audios),
                can_send_documents: yes(allowed.can_send_documents),
                can_send_photos: yes(allowed.can_send_photos),
                can_send_videos: yes(allowed.can_send_videos),
                can_send_video_notes: yes(allowed.can_send_video_notes),
                can_send_voice_notes: yes(allowed.can_send_voice_notes),
                can_send_polls: yes(allowed.can_send_polls),
                can_send_other_messages: yes(allowed.can_send_other_messages),
                can_add_web_page_previews: yes(allowed.can_add_web_page_previews),
                can_react_to_messages: yes(allowed.can_react_to_messages),
                can_edit_tag: yes(allowed.can_edit_tag),
                can_change_info: yes(allowed.can_change_info),
                can_invite_users: yes(allowed.can_invite_users),
                can_pin_messages: yes(allowed.can_pin_messages),
                can_manage_topics: yes(allowed.can_manage_topics),
                until_date: until,
                tag: banned.rank.clone(),
                ..Default::default()
            }
            .into()
        }
    }
}

/// Bot API `until_date`: 0 for forever.
fn until_date(until: i32) -> i64 {
    if until == i32::MAX {
        0
    } else {
        until.max(0).into()
    }
}
