//! `sendAudio` — https://core.telegram.org/bots/api#sendaudio

use simula_botapi_types::{methods, types as bot};

use crate::ctx::BotCtx;
use crate::error::BotResult;
use crate::files::input;
use crate::send::{self, Content, media};
use crate::text;

pub async fn handle(ctx: BotCtx, params: methods::SendAudio) -> BotResult<bot::Message> {
    let file = input::source(&ctx, Some(&params.audio), "audio")?;
    let thumbnail = input::upload_only(&ctx, params.thumbnail.as_ref());
    let caption = text::parse(
        params.caption.as_deref().unwrap_or_default(),
        params.parse_mode.as_deref(),
        params.caption_entities.as_deref(),
    )?;
    let content = Content::Media {
        media: media::Media {
            kind: media::Kind::Audio {
                duration: media::integer(params.duration, media::MAX_DURATION),
                title: params.title.filter(|t| !t.is_empty()),
                performer: params.performer.filter(|p| !p.is_empty()),
            },
            file,
            thumbnail,
            spoiler: false,
        },
        caption,
        above: false,
    };
    send::send(&ctx, send::options!(params), content).await
}
