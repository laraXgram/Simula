//! `sendVideo` — https://core.telegram.org/bots/api#sendvideo

use simula_botapi_types::{methods, types as bot};

use crate::ctx::BotCtx;
use crate::error::BotResult;
use crate::files::input;
use crate::send::{self, Content, media};
use crate::text;

pub async fn handle(ctx: BotCtx, params: methods::SendVideo) -> BotResult<bot::Message> {
    let file = input::source(&ctx, Some(&params.video), "video")?;
    let thumbnail = input::upload_only(&ctx, params.thumbnail.as_ref());
    let cover = params
        .cover
        .as_ref()
        .and_then(|c| input::source(&ctx, Some(c), "cover").ok());
    let kind = media::Kind::Video {
        duration: media::integer(params.duration, media::MAX_DURATION),
        width: media::integer(params.width, media::MAX_LENGTH),
        height: media::integer(params.height, media::MAX_LENGTH),
        supports_streaming: params.supports_streaming == Some(true),
        cover,
        start_timestamp: media::integer(params.start_timestamp, media::MAX_DURATION),
    };
    let caption = text::parse(
        params.caption.as_deref().unwrap_or_default(),
        params.parse_mode.as_deref(),
        params.caption_entities.as_deref(),
    )?;
    let content = Content::Media {
        media: media::Media {
            kind,
            file,
            thumbnail,
            spoiler: params.has_spoiler == Some(true),
        },
        caption,
        above: params.show_caption_above_media == Some(true),
    };
    send::send(&ctx, send::options!(params), content).await
}
