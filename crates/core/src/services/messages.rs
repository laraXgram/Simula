//! Creating messages in boxes and fanning out `updateNewMessage`.

use rusqlite::params;
use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::{Peer, UserId};
use crate::services::{entities, updates};
use crate::store::{self, messages::StoredMessage};

/// Documented text limit for one message.
pub const MAX_TEXT_LENGTH: usize = 4096;

#[derive(Debug, Default)]
pub struct Outgoing {
    pub text: String,
    pub entities: Vec<enums::MessageEntity>,
    /// Only bots may attach keyboards; ignored for users, as in Telegram.
    pub reply_markup: Option<enums::ReplyMarkup>,
    pub random_id: Option<i64>,
    pub silent: bool,
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
    if out.text.chars().count() > MAX_TEXT_LENGTH {
        return Err(RpcError::message_too_long());
    }
    if sender.is_bot && recipient.is_bot && from != to {
        return Err(RpcError::bad_request("USER_IS_BOT"));
    }
    // A bot can only write to users who have written to it first.
    if sender.is_bot && !has_dialog(ctx, from, Peer::User(to))? {
        return Err(RpcError::peer_id_invalid());
    }

    // Telegram adds mentions, hashtags, links… itself; bot commands only in chats with a bot.
    let with_commands = sender.is_bot || recipient.is_bot;
    let entities = entities::merge(out.entities, entities::detect(&out.text, with_commands));

    let date = ctx.now();
    let global_id = store::meta::next(ctx.db, "global_message", 1)?;
    let reply_markup = if sender.is_bot { out.reply_markup } else { None };
    let sides: &[(UserId, UserId, bool)] = if from == to { &[(from, to, true)] } else { &[(from, to, true), (to, from, false)] };

    let mut sent = None;
    for &(owner, peer, is_out) in sides {
        let message = types::Message {
            out: is_out,
            silent: out.silent,
            id: store::messages::next_id(ctx.db, owner)?,
            from_id: Some(Peer::User(from).to_tl()),
            peer_id: Peer::User(peer).to_tl(),
            date,
            message: out.text.clone(),
            entities: (!entities.is_empty()).then(|| entities.clone()),
            reply_markup: reply_markup.clone(),
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
        let update = updates::push_pts(ctx, owner, [from, to], |pts, pts_count| {
            types::UpdateNewMessage { message: message.clone().into(), pts, pts_count }.into()
        })?;
        if is_out {
            sent = Some(Sent { message, update });
        }
    }
    sent.ok_or_else(|| RpcError::internal("sender copy missing"))
}

fn has_dialog(ctx: &Ctx<'_>, owner: UserId, peer: Peer) -> RpcResult<bool> {
    Ok(ctx.db.query_row(
        "SELECT EXISTS (SELECT 1 FROM messages WHERE owner_id = ?1 AND peer = ?2)",
        params![owner.0, peer.bot_api_id()],
        |row| row.get(0),
    )?)
}
