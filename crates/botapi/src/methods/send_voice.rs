//! `sendVoice` — https://core.telegram.org/bots/api#sendvoice

use simula_botapi_types::{methods, types as bot};

use crate::ctx::BotCtx;
use crate::error::BotResult;
use crate::files::input;
use crate::send::{self, Content, media};
use crate::text;

pub async fn handle(ctx: BotCtx, params: methods::SendVoice) -> BotResult<bot::Message> {
    let file = input::source(&ctx, Some(&params.voice), "voice")?;
    let caption = text::parse(
        params.caption.as_deref().unwrap_or_default(),
        params.parse_mode.as_deref(),
        params.caption_entities.as_deref(),
    )?;
    let content = Content::Media {
        media: media::Media {
            kind: media::Kind::Voice {
                duration: media::integer(params.duration, media::MAX_DURATION),
            },
            file,
            thumbnail: None,
            spoiler: false,
        },
        caption,
        above: false,
    };
    send::send(&ctx, send::options!(params), content).await
}
