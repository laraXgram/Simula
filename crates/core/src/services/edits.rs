//! Editing messages: text, entities, the inline keyboard. Every copy changes and each
//! owner gets `updateEditMessage` with its own pts. Only the author may edit.

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::{Peer, UserId};
use crate::services::{entities, markup, messages, previews, updates};
use crate::store;

/// Users may edit for 48 hours; a bot's own messages have no limit.
const EDIT_TIME_LIMIT: i32 = 2 * 86400;

#[derive(Debug, Default)]
pub struct Edit {
    /// New text; `None` keeps the current one (and its entities).
    pub text: Option<String>,
    pub entities: Vec<enums::MessageEntity>,
    /// The new keyboard; `None` removes it, as on Telegram. Only inline keyboards.
    pub reply_markup: Option<enums::ReplyMarkup>,
    /// With new text: send no link preview.
    pub no_webpage: bool,
    pub invert_media: bool,
}

pub struct Edited {
    /// The editor's copy.
    pub message: types::Message,
    /// The editor's `updateEditMessage`.
    pub update: enums::Update,
}

pub fn edit(
    ctx: &mut Ctx<'_>,
    editor: UserId,
    peer: Peer,
    id: i32,
    edit: Edit,
) -> RpcResult<Edited> {
    let Peer::User(other) = peer else {
        return Err(RpcError::not_implemented(
            "editing messages in groups and channels",
        ));
    };
    let account = ctx.account(editor)?;
    let Some((global_id, old)) = store::messages::get_in(ctx.db, editor, peer, id)? else {
        return Err(RpcError::bad_request("MESSAGE_ID_INVALID"));
    };
    if !old.out {
        return Err(RpcError::forbidden("MESSAGE_AUTHOR_REQUIRED"));
    }
    if !account.is_bot && ctx.now() - old.date >= EDIT_TIME_LIMIT {
        return Err(RpcError::bad_request("MESSAGE_EDIT_TIME_EXPIRED"));
    }
    if edit
        .reply_markup
        .as_ref()
        .is_some_and(|m| !matches!(m, enums::ReplyMarkup::ReplyInlineMarkup(_)))
    {
        return Err(RpcError::bad_request("REPLY_MARKUP_INVALID"));
    }

    let (text, entities) = match edit.text.clone() {
        Some(text) => {
            if text.trim().is_empty() {
                return Err(RpcError::message_empty());
            }
            if entities::utf16_len(&text) as usize > messages::MAX_TEXT_LENGTH {
                return Err(RpcError::message_too_long());
            }
            let with_commands = account.is_bot || ctx.account(other)?.is_bot;
            let entities =
                messages::text_entities(ctx, editor, with_commands, &text, edit.entities)?;
            (text, (!entities.is_empty()).then_some(entities))
        }
        None => (old.message.clone(), old.entities.clone()),
    };
    // Preview placement goes with the text; a keyboard-only edit keeps it.
    let invert_media = if edit.text.is_some() {
        edit.invert_media
    } else {
        old.invert_media
    };
    // The preview follows the text; a keyboard-only edit keeps it.
    let media = match &edit.text {
        Some(_) if edit.no_webpage => None,
        Some(_) => previews::for_text(&text, entities.as_deref().unwrap_or_default()),
        None => old.media.clone(),
    };
    let invert_media = invert_media && media.is_some();
    let reply_markup = markup::normalize(ctx, editor, edit.reply_markup)?;
    let text_changed = text != old.message || entities != old.entities;
    if !text_changed && reply_markup == old.reply_markup && invert_media == old.invert_media {
        return Err(RpcError::bad_request("MESSAGE_NOT_MODIFIED"));
    }

    let date = ctx.now();
    let mut edited = None;
    for (owner, copy_id) in store::messages::copies(ctx.db, global_id)? {
        let Some(mut copy) = store::messages::get(ctx.db, owner, copy_id)? else {
            continue;
        };
        copy.message = text.clone();
        copy.entities = entities.clone();
        copy.reply_markup = reply_markup.clone();
        copy.invert_media = invert_media;
        copy.media = media.clone();
        copy.edit_date = Some(date);
        // A keyboard-only change doesn't mark the message as edited.
        copy.edit_hide = !text_changed;
        store::messages::update(ctx.db, owner, &copy)?;
        let update = updates::push_pts(ctx, owner, messages::users(&copy), |pts, pts_count| {
            types::UpdateEditMessage {
                message: copy.clone().into(),
                pts,
                pts_count,
            }
            .into()
        })?;
        if owner == editor {
            edited = Some(Edited {
                message: copy,
                update,
            });
        }
    }
    edited.ok_or_else(|| RpcError::internal("editor copy missing"))
}
