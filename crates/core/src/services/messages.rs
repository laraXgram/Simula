//! Creating messages in boxes and fanning out `updateNewMessage`.

use std::collections::BTreeSet;

use rusqlite::params;
use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::{Peer, UserId};
use crate::services::{entities, markup, previews, replies, updates};
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

    if out.text.trim().is_empty() {
        return Err(RpcError::message_empty());
    }
    // Telegram measures message length in UTF-16 code units.
    if entities::utf16_len(&out.text) as usize > MAX_TEXT_LENGTH {
        return Err(RpcError::message_too_long());
    }
    if sender.is_bot && recipient.is_bot && from != to {
        return Err(RpcError::bad_request("USER_IS_BOT"));
    }
    // A bot can only write to users who have written to it first.
    if sender.is_bot && !has_dialog(ctx, from, Peer::User(to))? {
        return Err(RpcError::peer_id_invalid());
    }

    let entities = text_entities(
        ctx,
        from,
        sender.is_bot || recipient.is_bot,
        &out.text,
        out.entities,
    )?;
    let preview = if out.no_webpage {
        None
    } else {
        previews::for_text(&out.text, &entities)
    };
    let reply_markup = markup::normalize(ctx, from, out.reply_markup)?;
    let reply_to = replies::resolve(ctx, from, Peer::User(to), out.reply_to.as_ref())?;

    let date = ctx.now();
    let global_id = store::meta::next(ctx.db, "global_message", 1)?;
    let sides: &[(UserId, UserId, bool)] = if from == to {
        &[(from, to, true)]
    } else {
        &[(from, to, true), (to, from, false)]
    };

    let mut sent = None;
    for &(owner, peer, is_out) in sides {
        let message = types::Message {
            out: is_out,
            silent: out.silent,
            noforwards: out.noforwards,
            invert_media: out.invert_media && preview.is_some(),
            media: preview.clone(),
            id: store::messages::next_id(ctx.db, owner)?,
            from_id: Some(Peer::User(from).to_tl()),
            peer_id: Peer::User(peer).to_tl(),
            reply_to: reply_to
                .as_ref()
                .map(|target| replies::header(ctx, owner, target))
                .transpose()?,
            date,
            message: out.text.clone(),
            entities: (!entities.is_empty()).then(|| entities.clone()),
            reply_markup: reply_markup.clone(),
            effect: out.effect,
            ..Default::default()
        };
        store::messages::insert(
            ctx.db,
            &StoredMessage {
                owner,
                peer: Peer::User(peer),
                global_id,
                random_id: if is_out { out.random_id } else { None },
                message: &message,
            },
        )?;
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
    sent.ok_or_else(|| RpcError::internal("sender copy missing"))
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
