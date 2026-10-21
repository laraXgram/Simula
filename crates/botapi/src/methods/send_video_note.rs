//! `sendVideoNote` — https://core.telegram.org/bots/api#sendvideonote

use simula_botapi_types::{methods, types as bot};

use crate::ctx::BotCtx;
use crate::error::BotResult;
use crate::files::input;
use crate::send::{self, Content, media};
use crate::text::Formatted;

pub async fn handle(ctx: BotCtx, params: methods::SendVideoNote) -> BotResult<bot::Message> {
    let file = input::source(&ctx, Some(&params.video_note), "video note")?;
    let thumbnail = input::upload_only(&ctx, params.thumbnail.as_ref());
    let content = Content::Media {
        media: media::Media {
            kind: media::Kind::VideoNote {
                duration: media::integer(params.duration, media::MAX_DURATION),
                length: media::integer(params.length, media::MAX_LENGTH),
            },
            file,
            thumbnail,
            spoiler: false,
        },
        caption: Formatted::default(),
        above: false,
    };
    send::send(&ctx, send::options!(params), content).await
}
