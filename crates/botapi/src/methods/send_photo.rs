//! `sendPhoto` — https://core.telegram.org/bots/api#sendphoto

use simula_botapi_types::{methods, types as bot};

use crate::ctx::BotCtx;
use crate::error::BotResult;
use crate::files::input;
use crate::send::{self, Content, media};
use crate::text;

pub async fn handle(ctx: BotCtx, params: methods::SendPhoto) -> BotResult<bot::Message> {
    let file = input::source(&ctx, Some(&params.photo), "photo")?;
    let caption = text::parse(
        params.caption.as_deref().unwrap_or_default(),
        params.parse_mode.as_deref(),
        params.caption_entities.as_deref(),
    )?;
    let content = Content::Media {
        media: media::Media {
            kind: media::Kind::Photo,
            file,
            thumbnail: None,
            spoiler: params.has_spoiler == Some(true),
        },
        caption,
        above: params.show_caption_above_media == Some(true),
    };
    send::send(&ctx, send::options!(params), content).await
}
