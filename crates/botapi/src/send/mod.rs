//! The pipeline every `send*` method shares, in telegram-bot-api's order
//! (`do_send_message`): parse the common options, reply parameters and reply markup,
//! resolve the chat, check the replied message, then let "tdlib" finish the content and
//! markup and make the TL call. The result is the bot's copy of the sent message.

pub mod markup;
mod reply;

use simula_botapi_types::{ChatId, types as bot};
use simula_core::Peer;
use simula_tl::{enums, functions};

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
}

pub async fn send(ctx: &BotCtx, options: Options, content: Content) -> BotResult<bot::Message> {
    // telegram-bot-api's parsing, before the chat is looked at.
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
    let in_topic = |id: Option<i64>| id.is_some_and(|id| id != 0);
    if in_topic(options.message_thread_id) || in_topic(options.direct_messages_topic_id) {
        return Err(BotError::not_implemented("topics"));
    }

    let peer = resolve::chat(ctx, &options.chat_id).await?;
    let reply_to = reply::check(ctx, peer, reply).await?;
    let private_chat = matches!(peer, Peer::User(_));

    // tdlib: keyboard rules for this chat, then the content.
    let reply_markup = match markup {
        Some(markup) => markup.into_tl(markup::Target { private_chat })?,
        None => None,
    };
    let call = match content {
        Content::Text { text, link_preview } => {
            let text = text.fix()?;
            let preview = link_preview.as_deref();
            functions::messages::SendMessage {
                no_webpage: preview.is_some_and(|p| p.is_disabled == Some(true)),
                invert_media: preview.is_some_and(|p| p.show_above_text == Some(true)),
                silent: options.disable_notification == Some(true),
                noforwards: options.protect_content == Some(true),
                allow_paid_floodskip: options.allow_paid_broadcast == Some(true),
                peer: ctx.input_peer(peer),
                reply_to,
                entities: Some(ctx.tl_entities(&text.entities)).filter(|e| !e.is_empty()),
                message: text.text,
                random_id: rand::random(),
                reply_markup,
                effect,
                ..Default::default()
            }
        }
    };
    let updates = ctx.call(call, ErrorContext { private_chat }).await?;
    let (message, users) = sent_message(&updates)
        .ok_or_else(|| BotError::internal("messages.sendMessage returned no message"))?;
    Ok(present::message(&ctx.gateway, ctx.bot.id, message, users).await)
}

/// The bot's copy of the message it just sent, from the call's `Updates`.
fn sent_message(updates: &enums::Updates) -> Option<(&simula_tl::types::Message, &[enums::User])> {
    let enums::Updates::Updates(batch) = updates else {
        return None;
    };
    batch.updates.iter().find_map(|update| match update {
        enums::Update::NewMessage(u) => match &u.message {
            enums::Message::Message(m) => Some((&**m, batch.users.as_slice())),
            _ => None,
        },
        _ => None,
    })
}
