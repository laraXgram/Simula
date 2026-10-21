//! Creating messages in boxes and fanning out `updateNewMessage`.

use std::collections::BTreeSet;

use rusqlite::params;
use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::{ChannelId, Peer, UserId};
use crate::services::{dialogs, entities, markup, media, previews, replies, updates};
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
    /// A photo or document (the text is then its caption).
    pub media: Option<enums::MessageMedia>,
    /// The album the message belongs to.
    pub grouped_id: Option<i64>,
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
    if recipient.is_deleted() {
        return Err(RpcError::bad_request("INPUT_USER_DEACTIVATED"));
    }
    check_content(ctx, from, &out)?;
    if sender.is_bot && recipient.is_bot && from != to {
        return Err(RpcError::bad_request("USER_IS_BOT"));
    }
    // A bot can only write to users who have written to it first (or who just asked to
    // join a chat it handles join requests for).
    if sender.is_bot
        && !has_dialog(ctx, from, Peer::User(to))?
        && !crate::services::invites::may_contact(ctx, from, to)?
    {
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

/// Send into any dialog: a private chat, a basic group, a supergroup or channel.
pub fn send_to(ctx: &mut Ctx<'_>, from: UserId, peer: Peer, out: Outgoing) -> RpcResult<Sent> {
    match peer {
        Peer::User(to) => send_private(ctx, from, to, out),
        Peer::Chat(chat) => crate::services::group_messages::send(ctx, from, chat, out),
        Peer::Channel(channel) => crate::services::channel_messages::send(ctx, from, channel, out),
    }
}

/// The `Updates` a send call answers with: for each message sent, `updateMessageID`
/// (its `random_id` → id) and the sender's own copy, with the users and chats they name.
pub fn sent_result(
    ctx: &Ctx<'_>,
    caller: UserId,
    sent: Vec<(i64, Sent)>,
) -> RpcResult<enums::Updates> {
    let mut updates = Vec::with_capacity(sent.len() * 2);
    for (random_id, sent) in &sent {
        updates.push(
            types::UpdateMessageID {
                id: sent.message.id,
                random_id: *random_id,
            }
            .into(),
        );
    }
    updates.extend(sent.into_iter().map(|(_, sent)| sent.update));
    Ok(types::Updates {
        updates,
        users: crate::render::users(ctx, caller, &ctx.out.users_for(caller))?,
        chats: crate::render::all_chats(
            ctx,
            caller,
            &ctx.out.chats_for(caller),
            &ctx.out.channels_for(caller),
        )?,
        date: ctx.now(),
        seq: 0,
    }
    .into())
}

/// A message's text, or a medium's caption (which may be empty; at most 1024 UTF-16
/// units, 4096 for Premium accounts).
pub(crate) fn check_content(ctx: &Ctx<'_>, from: UserId, out: &Outgoing) -> RpcResult<()> {
    if out.media.is_none() {
        return check_text(&out.text);
    }
    let max = if ctx.account(from)?.is_premium {
        media::MAX_PREMIUM_CAPTION_LENGTH
    } else {
        media::MAX_CAPTION_LENGTH
    };
    if entities::utf16_len(out.text.trim_end()) as usize > max {
        return Err(RpcError::bad_request("MEDIA_CAPTION_TOO_LONG"));
    }
    Ok(())
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
    pub out: Outgoing,
    pub entities: Vec<enums::MessageEntity>,
    /// The photo or document, or else the text's link preview.
    pub media: Option<enums::MessageMedia>,
    pub reply_markup: Option<enums::ReplyMarkup>,
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
    let media = match out.media.take() {
        Some(media) => Some(media),
        None if out.no_webpage => None,
        None => previews::for_text(&out.text, &entities),
    };
    let reply_markup = markup::normalize(ctx, from, out.reply_markup.take())?;
    let reply_to = replies::resolve(ctx, from, peer, out.reply_to.as_ref())?;
    Ok(Prepared {
        out,
        entities,
        media,
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
            invert_media: out.invert_media && prepared.media.is_some(),
            media: prepared.media.clone(),
            grouped_id: out.grouped_id,
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
        for channel in channels(&message) {
            ctx.out.mention_channel(owner, channel);
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
pub(crate) fn mentions(ctx: &Ctx<'_>, prepared: &Prepared, user: UserId) -> RpcResult<bool> {
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
    if let Some(Peer::User(user)) = reply_from(message)
        .and_then(|f| f.from_id.as_ref())
        .map(Peer::from_tl)
    {
        users.insert(user);
    }
    users
}

/// Supergroups and channels a message refers to besides its own chat: where the
/// message it replies to came from. Whoever receives the message receives these too.
pub fn channels(message: &types::Message) -> BTreeSet<ChannelId> {
    let mut out = BTreeSet::new();
    if let Some(enums::MessageReplyHeader::MessageReplyHeader(h)) = &message.reply_to {
        let peers = [
            h.reply_to_peer_id.as_ref(),
            reply_from(message).and_then(|f| f.from_id.as_ref()),
        ];
        for peer in peers.into_iter().flatten() {
            if let Peer::Channel(channel) = Peer::from_tl(peer) {
                out.insert(channel);
            }
        }
    }
    out
}

/// [`channels`] for any message (service messages refer to none).
pub fn channels_any(message: &enums::Message) -> BTreeSet<ChannelId> {
    match message {
        enums::Message::Message(m) => channels(m),
        _ => BTreeSet::new(),
    }
}

/// Where a reply to another chat's message came from.
fn reply_from(message: &types::Message) -> Option<&types::MessageFwdHeader> {
    match &message.reply_to {
        Some(enums::MessageReplyHeader::MessageReplyHeader(h)) => match &h.reply_from {
            Some(enums::MessageFwdHeader::MessageFwdHeader(from)) => Some(from),
            None => None,
        },
        _ => None,
    }
}

/// The id a message replies to in its own chat (none for replies to other chats).
pub fn same_chat_reply_id(message: &types::Message) -> Option<i32> {
    match &message.reply_to {
        Some(enums::MessageReplyHeader::MessageReplyHeader(h))
            if h.reply_to_peer_id.is_none() && h.reply_from.is_none() =>
        {
            h.reply_to_msg_id
        }
        _ => None,
    }
}

pub(crate) fn has_dialog(ctx: &Ctx<'_>, owner: UserId, peer: Peer) -> RpcResult<bool> {
    Ok(ctx.db.query_row(
        "SELECT EXISTS (SELECT 1 FROM messages WHERE owner_id = ?1 AND peer = ?2)",
        params![owner.0, peer.bot_api_id()],
        |row| row.get(0),
    )?)
}
