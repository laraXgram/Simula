//! The pipeline every `send*` method shares, in telegram-bot-api's order
//! (`do_send_message`): parse the common options, reply parameters and reply markup,
//! resolve the chat, check the replied message, then let "tdlib" finish the content and
//! markup and make the TL call. The result is the bot's copy of the sent message.

pub mod markup;
pub mod media;
mod reply;
mod topic;

use simula_botapi_types::{ChatId, types as bot};
use simula_core::Peer;
use simula_tl::{enums, functions, types};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};
use crate::present;
use crate::resolve;
use crate::text::Formatted;

/// Parameters shared by the `send*` methods (same names in each; see [`options!`]).
pub struct Options {
    pub chat_id: ChatId,
    pub business_connection_id: Option<String>,
    pub message_thread_id: Option<i64>,
    pub direct_messages_topic_id: Option<i64>,
    pub ephemeral_message_parameters: Option<Box<bot::EphemeralMessageParameters>>,
    pub disable_notification: Option<bool>,
    pub protect_content: Option<bool>,
    pub allow_paid_broadcast: Option<bool>,
    pub message_effect_id: Option<String>,
    pub suggested_post_parameters: Option<Box<bot::SuggestedPostParameters>>,
    pub reply_parameters: Option<Box<bot::ReplyParameters>>,
    pub reply_markup: Option<Box<bot::ReplyMarkup>>,
}

/// [`Options`] taken out of a `send*` method's parameters.
macro_rules! options {
    ($params:expr) => {
        $crate::send::Options {
            chat_id: $params.chat_id,
            business_connection_id: $params.business_connection_id,
            message_thread_id: $params.message_thread_id,
            direct_messages_topic_id: $params.direct_messages_topic_id,
            ephemeral_message_parameters: $params.ephemeral_message_parameters,
            disable_notification: $params.disable_notification,
            protect_content: $params.protect_content,
            allow_paid_broadcast: $params.allow_paid_broadcast,
            message_effect_id: $params.message_effect_id,
            suggested_post_parameters: $params.suggested_post_parameters,
            reply_parameters: $params.reply_parameters,
            reply_markup: $params.reply_markup,
        }
    };
}
pub(crate) use options;

/// What is being sent, as parsed by the method.
pub enum Content {
    Text {
        text: Formatted,
        link_preview: Option<Box<bot::LinkPreviewOptions>>,
    },
    /// A photo, document… with its caption (`above`: shown above the media).
    Media {
        media: media::Media,
        caption: Formatted,
        above: bool,
    },
}

/// One item of an album.
pub struct AlbumItem {
    pub media: media::Media,
    pub caption: Formatted,
    pub above: bool,
}

/// What every send shares once the chat is checked.
struct Prepared {
    peer: Peer,
    reply_to: Option<enums::InputReplyTo>,
    reply_markup: Option<enums::ReplyMarkup>,
    effect: Option<i64>,
    silent: bool,
    noforwards: bool,
    paid: bool,
}

/// telegram-bot-api's parsing (before the chat is looked at), then the chat, topic,
/// rights for `kinds` (text when empty), reply and keyboard checks.
async fn prepare(ctx: &BotCtx, options: Options, kinds: &[&media::Kind]) -> BotResult<Prepared> {
    if options.ephemeral_message_parameters.is_some() {
        return Err(BotError::not_implemented("ephemeral messages"));
    }
    let reply = reply::parse(options.reply_parameters.as_deref())?;
    if options.suggested_post_parameters.is_some() {
        return Err(BotError::not_implemented("suggested posts"));
    }
    let markup = markup::parse(options.reply_markup.as_deref())?;
    // telegram-bot-api's `to_integer`: anything unparsable counts as no effect.
    let effect = options
        .message_effect_id
        .as_deref()
        .and_then(|id| id.parse::<i64>().ok())
        .filter(|&id| id != 0);
    if options
        .business_connection_id
        .is_some_and(|id| !id.is_empty())
    {
        return Err(BotError::not_implemented("business connections"));
    }
    if options.direct_messages_topic_id.is_some_and(|id| id != 0) {
        return Err(BotError::not_implemented("channel direct messages"));
    }

    let peer = resolve::chat_for(ctx, &options.chat_id, resolve::Access::Write).await?;
    let topic = topic::check(ctx, peer, options.message_thread_id).await?;
    if let Peer::Channel(_) = peer {
        let channel = resolve::channel(ctx, peer, resolve::Access::Write).await?;
        if kinds.is_empty() {
            can_send(&channel, None)?;
        }
        for kind in kinds {
            can_send(&channel, Some(kind))?;
        }
    }
    let reply_to = reply::check(ctx, peer, reply, topic).await?;
    let private_chat = matches!(peer, Peer::User(_));
    // tdlib: keyboard rules for this chat.
    let reply_markup = match markup {
        Some(markup) => markup.into_tl(markup::Target { private_chat })?,
        None => None,
    };
    Ok(Prepared {
        peer,
        reply_to,
        reply_markup,
        effect,
        silent: options.disable_notification == Some(true),
        noforwards: options.protect_content == Some(true),
        paid: options.allow_paid_broadcast == Some(true),
    })
}

pub async fn send(ctx: &BotCtx, options: Options, content: Content) -> BotResult<bot::Message> {
    let kinds: Vec<&media::Kind> = match &content {
        Content::Text { .. } => vec![],
        Content::Media { media, .. } => vec![&media.kind],
    };
    let p = prepare(ctx, options, &kinds).await?;
    let private_chat = matches!(p.peer, Peer::User(_));
    let updates = match content {
        Content::Text { text, link_preview } => {
            let text = text.fix()?;
            let preview = link_preview.as_deref();
            let call = functions::messages::SendMessage {
                no_webpage: preview.is_some_and(|p| p.is_disabled == Some(true)),
                invert_media: preview.is_some_and(|p| p.show_above_text == Some(true)),
                silent: p.silent,
                noforwards: p.noforwards,
                allow_paid_floodskip: p.paid,
                peer: ctx.input_peer(p.peer),
                reply_to: p.reply_to,
                entities: Some(ctx.tl_entities(&text.entities)).filter(|e| !e.is_empty()),
                message: text.text,
                random_id: rand::random(),
                reply_markup: p.reply_markup,
                effect: p.effect,
                ..Default::default()
            };
            ctx.call(call, ErrorContext::Send { private_chat }).await?
        }
        Content::Media {
            media,
            caption,
            above,
        } => {
            let caption = caption.fix_caption()?;
            let media = media.into_tl(ctx).await?;
            let call = functions::messages::SendMedia {
                invert_media: above,
                silent: p.silent,
                noforwards: p.noforwards,
                allow_paid_floodskip: p.paid,
                peer: ctx.input_peer(p.peer),
                reply_to: p.reply_to,
                media,
                entities: Some(ctx.tl_entities(&caption.entities)).filter(|e| !e.is_empty()),
                message: caption.text,
                random_id: rand::random(),
                reply_markup: p.reply_markup,
                effect: p.effect,
                ..Default::default()
            };
            ctx.call(call, ErrorContext::Send { private_chat }).await?
        }
    };
    let mut sent = sent_messages(&updates);
    let (message, users, chats) = sent
        .pop()
        .ok_or_else(|| BotError::internal("the send returned no message"))?;
    Ok(present::message(&ctx.gateway, ctx.bot.id, message, users, chats).await)
}

/// An album (`sendMediaGroup`), after tdlib's checks: at most ten, photos and videos
/// together or documents or audio files alone, captions above or below for all.
pub async fn send_album(
    ctx: &BotCtx,
    options: Options,
    items: Vec<AlbumItem>,
) -> BotResult<Vec<bot::Message>> {
    let kinds: Vec<&media::Kind> = items.iter().map(|i| &i.media.kind).collect();
    let p = prepare(ctx, options, &kinds).await?;
    if items.len() > 10 {
        return Err(BotError::bad_request(
            "too many messages to send as an album",
        ));
    }
    if items.is_empty() {
        return Err(BotError::bad_request("there are no messages to send"));
    }
    for item in &items {
        if matches!(
            item.media.kind,
            media::Kind::Voice { .. }
                | media::Kind::Animation { .. }
                | media::Kind::VideoNote { .. }
        ) {
            return Err(BotError::bad_request("invalid message content type"));
        }
        if item.above != items[0].above {
            return Err(BotError::bad_request(
                "parameter show_caption_above_media must be the same for all messages",
            ));
        }
    }
    let types: Vec<&str> = items.iter().map(|i| i.media.kind.content_type()).collect();
    if types.iter().any(|t| *t != types[0])
        && let Some(lone) = types.iter().find(|t| matches!(**t, "Audio" | "Document"))
    {
        return Err(BotError::bad_request(format!(
            "{lone} can't be mixed with other media types"
        )));
    }
    let above = items[0].above;
    let mut multi_media = Vec::with_capacity(items.len());
    for item in items {
        let caption = item.caption.fix_caption()?;
        multi_media.push(
            types::InputSingleMedia {
                media: item.media.into_tl(ctx).await?,
                random_id: rand::random(),
                entities: Some(ctx.tl_entities(&caption.entities)).filter(|e| !e.is_empty()),
                message: caption.text,
            }
            .into(),
        );
    }
    let private_chat = matches!(p.peer, Peer::User(_));
    let updates = ctx
        .call(
            functions::messages::SendMultiMedia {
                invert_media: above,
                silent: p.silent,
                noforwards: p.noforwards,
                allow_paid_floodskip: p.paid,
                peer: ctx.input_peer(p.peer),
                reply_to: p.reply_to,
                multi_media,
                effect: p.effect,
                ..Default::default()
            },
            ErrorContext::Send { private_chat },
        )
        .await?;
    let mut out = Vec::new();
    for (message, users, chats) in sent_messages(&updates) {
        out.push(present::message(&ctx.gateway, ctx.bot.id, message, users, chats).await);
    }
    Ok(out)
}

/// tdlib's `can_send_message_content`, from the rights in the bot's channel object:
/// posting in channels takes `post_messages`; supergroups may restrict text or each kind
/// of media.
fn can_send(channel: &simula_tl::types::Channel, kind: Option<&media::Kind>) -> BotResult<()> {
    let admin = channel.creator || channel.admin_rights.is_some();
    if channel.broadcast {
        let may_post = channel.creator
            || matches!(&channel.admin_rights, Some(enums::ChatAdminRights::ChatAdminRights(r)) if r.post_messages);
        if !may_post {
            return Err(BotError::bad_request(
                "need administrator rights in the channel chat",
            ));
        }
        return Ok(());
    }
    let (right, what): (fn(&types::ChatBannedRights) -> bool, &str) = match kind {
        None => (|r| r.send_messages || r.send_plain, "text messages"),
        Some(kind) => kind.right(),
    };
    let banned = |rights: &Option<enums::ChatBannedRights>| matches!(rights, Some(enums::ChatBannedRights::ChatBannedRights(r)) if right(r));
    if !admin && (banned(&channel.banned_rights) || banned(&channel.default_banned_rights)) {
        return Err(BotError::bad_request(format!(
            "not enough rights to send {what} to the chat"
        )));
    }
    Ok(())
}

/// The bot's copies of the messages it just sent, in order, from the call's `Updates`.
fn sent_messages(
    updates: &enums::Updates,
) -> Vec<(&simula_tl::types::Message, &[enums::User], &[enums::Chat])> {
    let enums::Updates::Updates(batch) = updates else {
        return Vec::new();
    };
    batch
        .updates
        .iter()
        .filter_map(|update| {
            let message = match update {
                enums::Update::NewMessage(u) => &u.message,
                enums::Update::NewChannelMessage(u) => &u.message,
                _ => return None,
            };
            match message {
                enums::Message::Message(m) => {
                    Some((&**m, batch.users.as_slice(), batch.chats.as_slice()))
                }
                _ => None,
            }
        })
        .collect()
}
