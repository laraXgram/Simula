//! TL updates (rendered for the bot) → Bot API `Update` payloads, without `update_id`
//! (the queue assigns it).

use simula_botapi_types::types as bot;
use simula_core::Peer;
use simula_tl::{enums, types};

use super::{Entities, callback, member, message};

/// Bot API updates for a TL batch. A bot's own outgoing messages produce nothing (the
/// Bot API never echoes them back); deletions in private chats aren't reported either.
pub fn updates(updates: &enums::Updates, context: &Entities<'_>, me: i64) -> Vec<bot::Update> {
    let enums::Updates::Updates(batch) = updates else {
        return Vec::new();
    };
    batch
        .updates
        .iter()
        .filter_map(|update| {
            Some(match update {
                enums::Update::NewMessage(u) => bot::Update {
                    message: Some(Box::new(match &u.message {
                        enums::Message::Service(s) if !s.out => message::service(s, context)?,
                        other => message::message(incoming(other)?, context),
                    })),
                    ..Default::default()
                },
                enums::Update::ChatParticipant(u) if u.user_id == me => bot::Update {
                    my_chat_member: Some(Box::new(member::updated(u, context))),
                    ..Default::default()
                },
                enums::Update::ChatParticipant(u) => bot::Update {
                    chat_member: Some(Box::new(member::updated(u, context))),
                    ..Default::default()
                },
                // Channel posts are `channel_post`; supergroup messages are `message`.
                enums::Update::NewChannelMessage(u) => {
                    let (message, post) = match &u.message {
                        enums::Message::Service(s) if !s.out => {
                            (message::service(s, context)?, s.post)
                        }
                        other => {
                            let m = incoming(other)?;
                            (message::message(m, context), m.post)
                        }
                    };
                    if post {
                        bot::Update {
                            channel_post: Some(Box::new(message)),
                            ..Default::default()
                        }
                    } else {
                        bot::Update {
                            message: Some(Box::new(message)),
                            ..Default::default()
                        }
                    }
                }
                enums::Update::EditChannelMessage(u) => {
                    let m = incoming(&u.message)?;
                    let message = Some(Box::new(message::message(m, context)));
                    if m.post {
                        bot::Update {
                            edited_channel_post: message,
                            ..Default::default()
                        }
                    } else {
                        bot::Update {
                            edited_message: message,
                            ..Default::default()
                        }
                    }
                }
                enums::Update::ChannelParticipant(u) if u.user_id == me => bot::Update {
                    my_chat_member: Some(Box::new(member::updated_channel(u, context))),
                    ..Default::default()
                },
                enums::Update::ChannelParticipant(u) => bot::Update {
                    chat_member: Some(Box::new(member::updated_channel(u, context))),
                    ..Default::default()
                },
                enums::Update::EditMessage(u) => bot::Update {
                    edited_message: Some(Box::new(message::message(
                        incoming(&u.message)?,
                        context,
                    ))),
                    ..Default::default()
                },
                enums::Update::BotCallbackQuery(q) => bot::Update {
                    callback_query: Some(Box::new(callback::query(q, context))),
                    ..Default::default()
                },
                _ => return None,
            })
        })
        .collect()
}

/// Messages [`updates`] needs fetched, by chat: what incoming messages reply to, and
/// the messages of callback queries.
pub fn wanted(updates: &enums::Updates) -> Vec<(Peer, i32)> {
    let enums::Updates::Updates(batch) = updates else {
        return Vec::new();
    };
    batch
        .updates
        .iter()
        .filter_map(|update| match update {
            enums::Update::NewMessage(u) => references(&u.message),
            enums::Update::EditMessage(u) => incoming(&u.message).and_then(message::reply_ref),
            enums::Update::NewChannelMessage(u) => references(&u.message),
            enums::Update::EditChannelMessage(u) => {
                incoming(&u.message).and_then(message::reply_ref)
            }
            enums::Update::BotCallbackQuery(q) => Some((Peer::from_tl(&q.peer), q.msg_id)),
            _ => None,
        })
        .collect()
}

/// What a new message refers to: the replied message, or a pinned one.
fn references(message: &enums::Message) -> Option<(Peer, i32)> {
    match message {
        enums::Message::Service(s) if !s.out => message::service_reply_ref(s),
        other => incoming(other).and_then(message::reply_ref),
    }
}

fn incoming(message: &enums::Message) -> Option<&types::Message> {
    match message {
        enums::Message::Message(m) if !m.out => Some(m),
        _ => None,
    }
}
