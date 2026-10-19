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
    let topic = topic(Peer::from_tl(&m.peer_id), m.id, header, false, context);
    bot::Message {
        message_id: m.id.into(),
        message_thread_id: topic.thread.map(Into::into),
        is_topic_message: topic.is_topic_message,
        from,
        sender_chat: sender_chat.map(|peer| Box::new(chat(peer, context))),
        author_signature: m.post_author.clone(),
        date: m.date.into(),
        chat: chat(Peer::from_tl(&m.peer_id), context),
        reply_to_message: header
            .filter(|_| with_reply)
            .and_then(|h| replied(Peer::from_tl(&m.peer_id), h, context))
            .map(Box::new),
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
/// The replied message without its own reply, when it is in the same chat and was
/// fetched. A message in a forum topic replies to the topic's creation message.
fn replied(
    chat: Peer,
    header: &types::MessageReplyHeader,
    context: &Entities<'_>,
) -> Option<bot::Message> {
    if header.reply_to_peer_id.is_some() || header.reply_from.is_some() {
        return None;
    }
    match context.any_message(chat, header.reply_to_msg_id?)? {
        enums::Message::Message(m) => Some(convert(m, context, false)),
        enums::Message::Service(s) => service_inner(s, context, false),
        enums::Message::Empty(_) => None,
    }
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

/// Which forum topic or thread a supergroup message is in, as telegram-bot-api reports
/// it from tdlib's `MessageTopic`.
#[derive(Debug, Default)]
struct Topic {
    /// `message_thread_id`: the forum topic (never General), or the reply thread.
    thread: Option<i32>,
    is_topic_message: bool,
}

/// tdlib's `MessageReplyHeader` and `MessageTopic`: in forums, messages marked
/// `forum_topic` are in the topic their header names and the rest in General (which
/// the Bot API doesn't name); in other supergroups a reply is in the thread its reply
/// chain starts from. A topic's creation message is in its own topic.
fn topic(
    peer: Peer,
    id: i32,
    header: Option<&types::MessageReplyHeader>,
    creates_topic: bool,
    context: &Entities<'_>,
) -> Topic {
    let Peer::Channel(channel) = peer else {
        return Topic::default();
    };
    let Some(info) = context.channel(channel.0).filter(|c| !c.broadcast) else {
        return Topic::default();
    };
    let (top, is_topic) = if creates_topic {
        (Some(id), true)
    } else {
        let is_topic = header.is_some_and(|h| h.forum_topic);
        let same_chat = header
            .filter(|h| h.reply_to_peer_id.is_none() && h.reply_from.is_none())
            .and_then(|h| h.reply_to_msg_id);
        let top = header.and_then(|h| h.reply_to_top_id).or(same_chat);
        (top.filter(|&top| top < id), is_topic && top.is_some())
    };
    if info.forum {
        return match top {
            Some(top) if is_topic && top != crate::forums::GENERAL => Topic {
                thread: Some(top),
                is_topic_message: true,
            },
            _ => Topic::default(),
        };
    }
    Topic {
        thread: top,
        is_topic_message: false,
    }
}

/// A service message as the Bot API shows it, or `None` for kinds bots don't get.
pub fn service(m: &types::MessageService, context: &Entities<'_>) -> Option<bot::Message> {
    service_inner(m, context, true)
}

fn service_inner(
    m: &types::MessageService,
    context: &Entities<'_>,
    with_reply: bool,
) -> Option<bot::Message> {
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
    let header = match &m.reply_to {
        Some(enums::MessageReplyHeader::MessageReplyHeader(h)) => Some(&**h),
        _ => None,
    };
    let creates_topic = matches!(m.action, enums::MessageAction::TopicCreate(_));
    let topic = topic(
        Peer::from_tl(&m.peer_id),
        m.id,
        header,
        creates_topic,
        context,
    );
    // A pinned notice names the pinned message in its content instead.
    let pins = matches!(m.action, enums::MessageAction::PinMessage);
    let reply_to_message = header
        .filter(|_| with_reply && !pins)
        .and_then(|h| replied(Peer::from_tl(&m.peer_id), h, context))
        .map(Box::new);
    let base = bot::Message {
        message_id: m.id.into(),
        message_thread_id: topic.thread.map(Into::into),
        is_topic_message: topic.is_topic_message,
        reply_to_message,
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
        enums::MessageAction::TopicCreate(a) => bot::Message {
            forum_topic_created: Some(Box::new(bot::ForumTopicCreated {
                name: a.title.clone(),
                icon_color: a.icon_color.into(),
                icon_custom_emoji_id: crate::forums::emoji_id(a.icon_emoji_id),
                is_name_implicit: a.title_missing,
            })),
            ..base
        },
        // tdlib makes one of three contents of a topic edit: closed or reopened, General
        // hidden or shown, or a new name and icon.
        enums::MessageAction::TopicEdit(a) => match (a.closed, a.hidden) {
            (Some(true), _) => bot::Message {
                forum_topic_closed: Some(Box::default()),
                ..base
            },
            (Some(false), _) => bot::Message {
                forum_topic_reopened: Some(Box::default()),
                ..base
            },
            (None, Some(true)) => bot::Message {
                general_forum_topic_hidden: Some(Box::default()),
                ..base
            },
            (None, Some(false)) => bot::Message {
                general_forum_topic_unhidden: Some(Box::default()),
                ..base
            },
            (None, None) => bot::Message {
                forum_topic_edited: Some(Box::new(bot::ForumTopicEdited {
                    name: a.title.clone(),
                    icon_custom_emoji_id: a.icon_emoji_id.map(|id| {
                        if id == 0 {
                            String::new()
                        } else {
                            id.to_string()
                        }
                    }),
                })),
                ..base
            },
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
