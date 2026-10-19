//! TL `message` → Bot API `Message`.

use simula_botapi_types::types as bot;
use simula_core::Peer;
use simula_tl::{enums, types};

use super::{Entities, chat::chat, entities::entities, link_preview, markup, user::user};

pub fn message(m: &types::Message, context: &Entities<'_>) -> bot::Message {
    convert(m, context, true)
}

/// `with_reply`: whether to include `reply_to_message` (never nested twice, like
/// telegram-bot-api's `need_reply`).
fn convert(m: &types::Message, context: &Entities<'_>, with_reply: bool) -> bot::Message {
    let from = m
        .from_id
        .as_ref()
        .map(Peer::from_tl)
        .and_then(|peer| match peer {
            Peer::User(id) => context.user(id.0).map(|u| Box::new(user(u, false))),
            _ => None,
        });
    // Channel posts (and messages sent on behalf of a chat) carry the chat as sender.
    let sender_chat = match m.from_id.as_ref().map(Peer::from_tl) {
        None if m.post => Some(Peer::from_tl(&m.peer_id)),
        Some(peer @ (Peer::Channel(_) | Peer::Chat(_))) => Some(peer),
        _ => None,
    };
    let header = match &m.reply_to {
        Some(enums::MessageReplyHeader::MessageReplyHeader(header)) => Some(&**header),
        _ => None,
    };
    let is_text = is_text(m);
    bot::Message {
        message_id: m.id.into(),
        from,
        sender_chat: sender_chat.map(|peer| Box::new(chat(peer, context))),
        author_signature: m.post_author.clone(),
        date: m.date.into(),
        chat: chat(Peer::from_tl(&m.peer_id), context),
        reply_to_message: header
            .filter(|_| with_reply)
            .and_then(|h| replied(m, h, context))
            .map(|replied| Box::new(convert(replied, context, false))),
        external_reply: header.and_then(|h| external(h, context)).map(Box::new),
        quote: header.and_then(|h| quote(h, context)).map(Box::new),
        edit_date: m.edit_date.map(Into::into),
        has_protected_content: m.noforwards,
        text: (!m.message.is_empty() && is_text).then(|| m.message.clone()),
        entities: entities(m.entities.as_ref(), context),
        link_preview_options: is_text
            .then(|| link_preview::options(m))
            .flatten()
            .map(Box::new),
        effect_id: m.effect.map(|id| id.to_string()),
        reply_markup: markup::inline_keyboard(m.reply_markup.as_ref()).map(Box::new),
        ..Default::default()
    }
}

/// A text message, possibly with a link preview (which bots don't see as media).
pub fn is_text(m: &types::Message) -> bool {
    matches!(m.media, None | Some(enums::MessageMedia::WebPage(_)))
}

/// The chat and id of the message a service message points at (a pinned message).
pub fn service_reply_ref(m: &types::MessageService) -> Option<(Peer, i32)> {
    match &m.reply_to {
        Some(enums::MessageReplyHeader::MessageReplyHeader(h)) if h.reply_to_peer_id.is_none() => {
            h.reply_to_msg_id.map(|id| (Peer::from_tl(&m.peer_id), id))
        }
        _ => None,
    }
}

/// One message without its replied message, as `pinned_message` carries it.
pub fn without_reply(m: &types::Message, context: &Entities<'_>) -> bot::Message {
    convert(m, context, false)
}

/// The chat and id of the message `m` replies to in the same chat, if any.
pub fn reply_ref(m: &types::Message) -> Option<(Peer, i32)> {
    reply_id(m).map(|id| (Peer::from_tl(&m.peer_id), id))
}

/// The id of the message `m` replies to in the same chat, if any.
pub fn reply_id(m: &types::Message) -> Option<i32> {
    match &m.reply_to {
        Some(enums::MessageReplyHeader::MessageReplyHeader(h)) if h.reply_to_peer_id.is_none() => {
            h.reply_to_msg_id
        }
        _ => None,
    }
}

/// The replied message, when it is in the same chat and was fetched.
fn replied<'a>(
    m: &types::Message,
    header: &types::MessageReplyHeader,
    context: &Entities<'a>,
) -> Option<&'a types::Message> {
    if header.reply_to_peer_id.is_some() {
        return None;
    }
    context.message(Peer::from_tl(&m.peer_id), header.reply_to_msg_id?)
}

/// A reply to a message in another chat: where it came from (telegram-bot-api's
/// `JsonExternalReplyInfo`). The chat and id are known only for supergroups and channels.
fn external(
    header: &types::MessageReplyHeader,
    context: &Entities<'_>,
) -> Option<bot::ExternalReplyInfo> {
    let Some(enums::MessageFwdHeader::MessageFwdHeader(from)) = &header.reply_from else {
        return None;
    };
    let channel = header
        .reply_to_peer_id
        .as_ref()
        .map(Peer::from_tl)
        .filter(|peer| matches!(peer, Peer::Channel(_)));
    Some(bot::ExternalReplyInfo {
        origin: origin(from, context)?,
        chat: channel.map(|peer| Box::new(chat(peer, context))),
        message_id: channel.and(header.reply_to_msg_id).map(Into::into),
        ..Default::default()
    })
}

/// Who a forwarded or externally replied message came from (tdlib's `MessageOrigin`).
pub fn origin(
    from: &types::MessageFwdHeader,
    context: &Entities<'_>,
) -> Option<bot::MessageOrigin> {
    let date = from.date.into();
    Some(match from.from_id.as_ref().map(Peer::from_tl) {
        Some(Peer::User(id)) => bot::MessageOrigin::User(Box::new(bot::MessageOriginUser {
            date,
            sender_user: context
                .user(id.0)
                .map(|u| user(u, false))
                .unwrap_or(bot::User {
                    id: id.0,
                    ..Default::default()
                }),
            ..Default::default()
        })),
        Some(peer) => match from.channel_post {
            Some(post) => bot::MessageOrigin::Channel(Box::new(bot::MessageOriginChannel {
                date,
                chat: chat(peer, context),
                message_id: post.into(),
                author_signature: from.post_author.clone(),
                ..Default::default()
            })),
            None => bot::MessageOrigin::Chat(Box::new(bot::MessageOriginChat {
                date,
                sender_chat: chat(peer, context),
                author_signature: from.post_author.clone(),
                ..Default::default()
            })),
        },
        None => bot::MessageOrigin::HiddenUser(Box::new(bot::MessageOriginHiddenUser {
            date,
            sender_user_name: from.from_name.clone()?,
            ..Default::default()
        })),
    })
}

fn quote(header: &types::MessageReplyHeader, context: &Entities<'_>) -> Option<bot::TextQuote> {
    let text = header.quote_text.clone().filter(|t| !t.is_empty())?;
    Some(bot::TextQuote {
        text,
        entities: entities(header.quote_entities.as_ref(), context),
        position: header.quote_offset.unwrap_or(0).max(0).into(),
        is_manual: header.quote,
    })
}

/// A service message as the Bot API shows it, or `None` for kinds bots don't get.
pub fn service(m: &types::MessageService, context: &Entities<'_>) -> Option<bot::Message> {
    let person = |id: i64| {
        context
            .user(id)
            .map(|u| user(u, false))
            .unwrap_or(bot::User {
                id,
                ..Default::default()
            })
    };
    let from = match m.from_id.as_ref().map(Peer::from_tl) {
        Some(Peer::User(id)) => Some(Box::new(person(id.0))),
        _ => None,
    };
    let sender_chat =
        (m.from_id.is_none() && m.post).then(|| Box::new(chat(Peer::from_tl(&m.peer_id), context)));
    let base = bot::Message {
        message_id: m.id.into(),
        from,
        sender_chat,
        date: m.date.into(),
        chat: chat(Peer::from_tl(&m.peer_id), context),
        ..Default::default()
    };
    Some(match &m.action {
        enums::MessageAction::ChatCreate(_) => bot::Message {
            group_chat_created: true,
            ..base
        },
        enums::MessageAction::ChatAddUser(a) => bot::Message {
            new_chat_members: Some(a.users.iter().map(|&id| person(id)).collect()),
            ..base
        },
        // Joining by link or by approved request shows the joiner as the new member.
        enums::MessageAction::ChatJoinedByLink(_) | enums::MessageAction::ChatJoinedByRequest => {
            let joiner = match m.from_id.as_ref().map(Peer::from_tl) {
                Some(Peer::User(id)) => person(id.0),
                _ => return None,
            };
            bot::Message {
                new_chat_members: Some(vec![joiner]),
                ..base
            }
        }
        enums::MessageAction::ChatDeleteUser(a) => bot::Message {
            left_chat_member: Some(Box::new(person(a.user_id))),
            ..base
        },
        enums::MessageAction::ChatEditTitle(a) => bot::Message {
            new_chat_title: Some(a.title.clone()),
            ..base
        },
        enums::MessageAction::PinMessage => {
            let chat_peer = Peer::from_tl(&m.peer_id);
            let id = match &m.reply_to {
                Some(enums::MessageReplyHeader::MessageReplyHeader(h)) => h.reply_to_msg_id,
                _ => None,
            }?;
            // The pinned message, without its own reply (telegram-bot-api's
            // `need_reply = false`), or inaccessible when it can't be fetched.
            let pinned = match context.message(chat_peer, id) {
                Some(found) => {
                    bot::MaybeInaccessibleMessage::Message(Box::new(convert(found, context, false)))
                }
                None => bot::MaybeInaccessibleMessage::InaccessibleMessage(Box::new(
                    bot::InaccessibleMessage {
                        chat: chat(chat_peer, context),
                        message_id: id.into(),
                        date: Default::default(),
                    },
                )),
            };
            bot::Message {
                pinned_message: Some(Box::new(pinned)),
                ..base
            }
        }
        enums::MessageAction::ChannelCreate(_) if m.post => bot::Message {
            channel_chat_created: true,
            ..base
        },
        enums::MessageAction::ChannelCreate(_) => bot::Message {
            supergroup_chat_created: true,
            ..base
        },
        enums::MessageAction::ChatMigrateTo(a) => bot::Message {
            migrate_to_chat_id: Some(
                Peer::Channel(simula_core::ChannelId(a.channel_id)).bot_api_id(),
            ),
            ..base
        },
        enums::MessageAction::ChannelMigrateFrom(a) => bot::Message {
            migrate_from_chat_id: Some(Peer::Chat(simula_core::ChatId(a.chat_id)).bot_api_id()),
            ..base
        },
        _ => return None,
    })
}
