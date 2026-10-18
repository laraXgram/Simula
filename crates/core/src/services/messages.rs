//! Creating messages in boxes and fanning out `updateNewMessage`.

use std::collections::BTreeSet;

use rusqlite::params;
use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::{Peer, UserId};
use crate::services::{dialogs, entities, markup, previews, replies, updates};
use crate::store::{self, messages::StoredMessage};

/// Documented text limit for one message, in UTF-16 code units.
pub const MAX_TEXT_LENGTH: usize = 4096;

#[derive(Debug, Default)]
pub struct Outgoing {
    pub text: String,
    pub entities: Vec<enums::MessageEntity>,
    /// Only bots may attach keyboards; ignored for users, as in Telegram.
    pub reply_markup: Option<enums::ReplyMarkup>,
    pub reply_to: Option<enums::InputReplyTo>,
    pub random_id: Option<i64>,
    pub silent: bool,
    /// Protected content: can't be forwarded or saved.
    pub noforwards: bool,
    /// Send no link preview.
    pub no_webpage: bool,
    /// Show the link preview above the text.
    pub invert_media: bool,
    /// Message effect (private chats only). Simula has no effect catalog, so any id is kept.
    pub effect: Option<i64>,
}

pub struct Sent {
    /// The sender's copy.
    pub message: types::Message,
    /// The sender's `updateNewMessage`.
    pub update: enums::Update,
}

/// Send a message in a private chat. Each side gets its own copy (own id, own pts).
pub fn send_private(ctx: &mut Ctx<'_>, from: UserId, to: UserId, out: Outgoing) -> RpcResult<Sent> {
    let sender = ctx.account(from)?;
    let recipient = ctx.account(to).map_err(|_| RpcError::peer_id_invalid())?;
    check_text(&out.text)?;
    if sender.is_bot && recipient.is_bot && from != to {
        return Err(RpcError::bad_request("USER_IS_BOT"));
    }
    // A bot can only write to users who have written to it first.
    if sender.is_bot && !has_dialog(ctx, from, Peer::User(to))? {
        return Err(RpcError::peer_id_invalid());
    }

    let with_commands = sender.is_bot || recipient.is_bot;
    let prepared = prepare(ctx, from, Peer::User(to), out, with_commands)?;
    let copies: Vec<(UserId, Peer)> = if from == to {
        vec![(from, Peer::User(to))]
    } else {
        vec![(from, Peer::User(to)), (to, Peer::User(from))]
    };
    let (sent, global_id) = deliver(ctx, from, &prepared, &copies)?;
    // Bots read what users send them at once: the sender sees it read right away.
    if recipient.is_bot
        && !sender.is_bot
        && let Some(id) = store::messages::copy_id(ctx.db, to, global_id)?
    {
        dialogs::read(ctx, to, Peer::User(from), id)?;
    }
    Ok(sent)
}

/// Telegram's checks on a message's text: non-empty, at most 4096 UTF-16 code units.
pub(crate) fn check_text(text: &str) -> RpcResult<()> {
    if text.trim().is_empty() {
        return Err(RpcError::message_empty());
    }
    if entities::utf16_len(text) as usize > MAX_TEXT_LENGTH {
        return Err(RpcError::message_too_long());
    }
    Ok(())
}

/// What every copy of a new message shares, checked and resolved once.
pub(crate) struct Prepared {
    out: Outgoing,
    pub entities: Vec<enums::MessageEntity>,
    preview: Option<enums::MessageMedia>,
    reply_markup: Option<enums::ReplyMarkup>,
    pub reply_to: Option<replies::Target>,
}

impl Prepared {
    pub fn text(&self) -> &str {
        &self.out.text
    }
}

/// Entities, link preview, keyboard and reply target of a message `from` sends into the
/// dialog with `peer` (as `from` sees it).
pub(crate) fn prepare(
    ctx: &Ctx<'_>,
    from: UserId,
    peer: Peer,
    mut out: Outgoing,
    with_bot_commands: bool,
) -> RpcResult<Prepared> {
    let explicit = std::mem::take(&mut out.entities);
    let entities = text_entities(ctx, from, with_bot_commands, &out.text, explicit)?;
    let preview = if out.no_webpage {
        None
    } else {
        previews::for_text(&out.text, &entities)
    };
    let reply_markup = markup::normalize(ctx, from, out.reply_markup.take())?;
    let reply_to = replies::resolve(ctx, from, peer, out.reply_to.as_ref())?;
    Ok(Prepared {
        out,
        entities,
        preview,
        reply_markup,
        reply_to,
    })
}

/// One copy per `(owner, dialog as the owner sees it)`, each with its own id and pts.
/// Returns the sender's copy and the copies' shared `global_id`.
pub(crate) fn deliver(
    ctx: &mut Ctx<'_>,
    from: UserId,
    prepared: &Prepared,
    copies: &[(UserId, Peer)],
) -> RpcResult<(Sent, i64)> {
    let out = &prepared.out;
    let date = ctx.now();
    let global_id = store::meta::next(ctx.db, "global_message", 1)?;
    let mut sent = None;
    for &(owner, peer) in copies {
        let is_out = owner == from;
        let message = types::Message {
            out: is_out,
            mentioned: !is_out && mentions(ctx, prepared, owner)?,
            silent: out.silent,
            noforwards: out.noforwards,
            invert_media: out.invert_media && prepared.preview.is_some(),
            media: prepared.preview.clone(),
            id: store::messages::next_id(ctx.db, owner)?,
            from_id: Some(Peer::User(from).to_tl()),
            peer_id: peer.to_tl(),
            reply_to: prepared
                .reply_to
                .as_ref()
                .map(|target| replies::header(ctx, owner, target))
                .transpose()?,
            date,
            message: out.text.clone(),
            entities: (!prepared.entities.is_empty()).then(|| prepared.entities.clone()),
            reply_markup: prepared.reply_markup.clone(),
            effect: out.effect,
            ..Default::default()
        };
        store::messages::insert(
            ctx.db,
            &StoredMessage {
                owner,
                peer,
                global_id,
                random_id: if is_out { out.random_id } else { None },
                message: &message,
            },
        )?;
        if let Peer::Chat(chat) = peer {
            ctx.out.mention_chat(owner, chat);
        }
        let update = updates::push_pts(ctx, owner, users(&message), |pts, pts_count| {
            types::UpdateNewMessage {
                message: message.clone().into(),
                pts,
                pts_count,
            }
            .into()
        })?;
        if is_out {
            sent = Some(Sent { message, update });
        }
    }
    let sent = sent.ok_or_else(|| RpcError::internal("sender copy missing"))?;
    Ok((sent, global_id))
}

/// Whether a message mentions `user`: by @username, by id, or by replying to them.
fn mentions(ctx: &Ctx<'_>, prepared: &Prepared, user: UserId) -> RpcResult<bool> {
    let username = ctx.account(user)?.username.map(|u| u.to_ascii_lowercase());
    let text: Vec<u16> = prepared.out.text.encode_utf16().collect();
    let named = prepared.entities.iter().any(|entity| match entity {
        enums::MessageEntity::MentionName(m) => m.user_id == user.0,
        enums::MessageEntity::Mention(m) => {
            let start = (m.offset + 1).max(0) as usize;
            let end = ((m.offset + m.length).max(0) as usize).min(text.len());
            username.as_deref().is_some_and(|name| {
                String::from_utf16_lossy(text.get(start..end).unwrap_or_default())
                    .to_ascii_lowercase()
                    == name
            })
        }
        _ => false,
    });
    Ok(named
        || prepared
            .reply_to
            .as_ref()
            .is_some_and(|t| t.author() == Some(user)))
}

/// A message's final entities: the sender's, plus what Telegram adds itself (mentions,
/// hashtags, links…; bot commands only in chats with a bot).
pub(crate) fn text_entities(
    ctx: &Ctx<'_>,
    sender: UserId,
    with_bot_commands: bool,
    text: &str,
    explicit: Vec<enums::MessageEntity>,
) -> RpcResult<Vec<enums::MessageEntity>> {
    let explicit = entities::resolve_input(ctx, sender, explicit)?;
    Ok(entities::merge(
        explicit,
        entities::detect(text, with_bot_commands),
    ))
}

/// Users any message refers to, service messages included.
pub fn users_any(message: &enums::Message) -> BTreeSet<UserId> {
    match message {
        enums::Message::Message(m) => users(m),
        enums::Message::Service(s) => {
            let mut users: BTreeSet<UserId> = crate::services::chats::action_users(&s.action)
                .into_iter()
                .collect();
            if let Some(Peer::User(from)) = s.from_id.as_ref().map(Peer::from_tl) {
                users.insert(from);
            }
            if let Peer::User(peer) = Peer::from_tl(&s.peer_id) {
                users.insert(peer);
            }
            users
        }
        enums::Message::Empty(_) => BTreeSet::new(),
    }
}

/// Users a message refers to; whoever receives the message receives these users too.
pub fn users(message: &types::Message) -> BTreeSet<UserId> {
    let peers = [Some(&message.peer_id), message.from_id.as_ref()];
    let mut users: BTreeSet<UserId> = peers
        .into_iter()
        .flatten()
        .filter_map(|peer| match Peer::from_tl(peer) {
            Peer::User(id) => Some(id),
            _ => None,
        })
        .collect();
    for entity in message.entities.iter().flatten() {
        if let enums::MessageEntity::MentionName(mention) = entity {
            users.insert(UserId(mention.user_id));
        }
    }
    users.extend(markup::users(message.reply_markup.as_ref()));
    users
}

pub(crate) fn has_dialog(ctx: &Ctx<'_>, owner: UserId, peer: Peer) -> RpcResult<bool> {
    Ok(ctx.db.query_row(
        "SELECT EXISTS (SELECT 1 FROM messages WHERE owner_id = ?1 AND peer = ?2)",
        params![owner.0, peer.bot_api_id()],
        |row| row.get(0),
    )?)
}
