//! `sendDocument` — https://core.telegram.org/bots/api#senddocument

use simula_botapi_types::{methods, types as bot};

use crate::ctx::BotCtx;
use crate::error::BotResult;
use crate::files::input;
use crate::send::{self, Content, media};
use crate::text;

pub async fn handle(ctx: BotCtx, params: methods::SendDocument) -> BotResult<bot::Message> {
    let file = input::source(&ctx, Some(&params.document), "document")?;
    let thumbnail = input::upload_only(&ctx, params.thumbnail.as_ref());
    let caption = text::parse(
        params.caption.as_deref().unwrap_or_default(),
        params.parse_mode.as_deref(),
        params.caption_entities.as_deref(),
    )?;
    let content = Content::Media {
        media: media::Media {
            kind: media::Kind::Document {
                force_file: params.disable_content_type_detection == Some(true),
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
