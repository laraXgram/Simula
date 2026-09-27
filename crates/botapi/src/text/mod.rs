//! Message text preparation, as telegram-bot-api + tdlib do before sending: `parse_mode`
//! parsing (HTML, MarkdownV2, Markdown — ports of tdlib's parsers), explicit `entities`,
//! entity validation and whitespace trimming, with the same error texts.

mod bytes;
pub mod entity;
mod fix;
mod html;
mod input;
mod link;
mod markdown;
mod markdown_v2;

use simula_botapi_types::types as bot;

pub use entity::Entity;

use crate::error::BotError;
use crate::errors::botapi::format as bot_error;

/// telegram-bot-api rejects longer texts before parsing.
const MAX_INPUT_BYTES: usize = 1 << 15;

#[derive(Debug, Clone, PartialEq)]
pub struct Formatted {
    pub text: String,
    pub entities: Vec<Entity>,
}

/// Text plus `parse_mode` or explicit `entities` → final text and entities.
pub fn prepare(text: &str, parse_mode: Option<&str>, entities: Option<&[bot::MessageEntity]>) -> Result<Formatted, BotError> {
    if text.len() > MAX_INPUT_BYTES {
        return Err(bot_error(400, "Text is too long"));
    }
    let mode = parse_mode.unwrap_or("").to_ascii_lowercase();
    let (text, entities) = if !text.is_empty() && !mode.is_empty() && mode != "none" {
        let parsed = match mode.as_str() {
            "markdown" => markdown::parse(text),
            "markdownv2" => markdown_v2::parse(text),
            "html" => html::parse(text),
            _ => return Err(bot_error(400, "Unsupported parse_mode")),
        };
        parsed.map_err(|message| bot_error(400, &format!("Can't parse entities: {message}")))?
    } else {
        let converted = input::entities(entities.unwrap_or_default())
            .map_err(|message| bot_error(400, &format!("Can't parse MessageEntity: {message}")))?;
        (text.to_owned(), converted)
    };
    fix::fix(text, entities).map_err(|message| bot_error(400, message))
}

#[cfg(test)]
mod tests;
