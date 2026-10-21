//! `sendMediaGroup` — https://core.telegram.org/bots/api#sendmediagroup

use simula_botapi_types::{methods, types as bot};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult};
use crate::files::input;
use crate::send::{self, AlbumItem, media};
use crate::text;

pub async fn handle(ctx: BotCtx, params: methods::SendMediaGroup) -> BotResult<Vec<bot::Message>> {
    let mut items = Vec::with_capacity(params.media.len());
    for item in &params.media {
        items.push(
            album_item(&ctx, item)
                .map_err(|e| BotError::bad_request(format!("can't parse InputMedia: {e}")))?,
        );
    }
    // Albums take no keyboard, ephemeral parameters or suggested posts.
    let options = send::Options {
        chat_id: params.chat_id,
        business_connection_id: params.business_connection_id,
        message_thread_id: params.message_thread_id,
        direct_messages_topic_id: params.direct_messages_topic_id,
        ephemeral_message_parameters: None,
        disable_notification: params.disable_notification,
        protect_content: params.protect_content,
        allow_paid_broadcast: params.allow_paid_broadcast,
        message_effect_id: params.message_effect_id,
        suggested_post_parameters: None,
        reply_parameters: params.reply_parameters,
        reply_markup: None,
    };
    send::send_album(&ctx, options, items).await
}

/// telegram-bot-api's `get_input_media` for one album item.
fn album_item(ctx: &BotCtx, item: &bot::MediaGroupItem) -> Result<AlbumItem, String> {
    let file =
        |media: &str| input::from_string(ctx, media).ok_or_else(|| "media not found".to_owned());
    let thumbnail = |thumbnail: &Option<String>| {
        thumbnail
            .as_deref()
            .filter(|t| t.starts_with("attach://"))
            .and_then(|t| input::from_string(ctx, t))
    };
    let caption = |caption: &Option<String>,
                   mode: &Option<String>,
                   entities: &Option<Vec<bot::MessageEntity>>| {
        text::parse(
            caption.as_deref().unwrap_or_default(),
            mode.as_deref(),
            entities.as_deref(),
        )
        .map_err(|e| e.description.trim_start_matches("Bad Request: ").to_owned())
    };
    Ok(match item {
        bot::MediaGroupItem::InputMediaPhoto(m) => AlbumItem {
            media: media::Media {
                kind: media::Kind::Photo,
                file: file(&m.media)?,
                thumbnail: None,
                spoiler: m.has_spoiler == Some(true),
            },
            caption: caption(&m.caption, &m.parse_mode, &m.caption_entities)?,
            above: m.show_caption_above_media == Some(true),
        },
        bot::MediaGroupItem::InputMediaVideo(m) => AlbumItem {
            media: media::Media {
                kind: media::Kind::Video {
                    duration: media::integer(m.duration, media::MAX_DURATION),
                    width: media::integer(m.width, media::MAX_LENGTH),
                    height: media::integer(m.height, media::MAX_LENGTH),
                    supports_streaming: m.supports_streaming == Some(true),
                    cover: m.cover.as_deref().and_then(|c| input::from_string(ctx, c)),
                    start_timestamp: media::integer(m.start_timestamp, media::MAX_DURATION),
                },
                file: file(&m.media)?,
                thumbnail: thumbnail(&m.thumbnail),
                spoiler: m.has_spoiler == Some(true),
            },
            caption: caption(&m.caption, &m.parse_mode, &m.caption_entities)?,
            above: m.show_caption_above_media == Some(true),
        },
        bot::MediaGroupItem::InputMediaAudio(m) => AlbumItem {
            media: media::Media {
                kind: media::Kind::Audio {
                    duration: media::integer(m.duration, media::MAX_DURATION),
                    title: m.title.clone().filter(|t| !t.is_empty()),
                    performer: m.performer.clone().filter(|p| !p.is_empty()),
                },
                file: file(&m.media)?,
                thumbnail: thumbnail(&m.thumbnail),
                spoiler: false,
            },
            caption: caption(&m.caption, &m.parse_mode, &m.caption_entities)?,
            above: false,
        },
        bot::MediaGroupItem::InputMediaDocument(m) => AlbumItem {
            media: media::Media {
                kind: media::Kind::Document {
                    force_file: m.disable_content_type_detection == Some(true),
                },
                file: file(&m.media)?,
                thumbnail: thumbnail(&m.thumbnail),
                spoiler: false,
            },
            caption: caption(&m.caption, &m.parse_mode, &m.caption_entities)?,
            above: false,
        },
        bot::MediaGroupItem::InputMediaLivePhoto(_) => {
            return Err("live photos aren't supported in Simula yet".into());
        }
    })
}
