//! Explicit Bot API `entities` → entities (telegram-bot-api's `get_text_entity_type`).
//! Automatic kinds (mention, url, …) are dropped: the server detects those itself.

use simula_botapi_types::types as bot;

use super::entity::{DateFormat, Entity, Kind};

pub fn entities(input: &[bot::MessageEntity]) -> Result<Vec<Entity>, String> {
    let mut out = Vec::with_capacity(input.len());
    for e in input {
        if let Some(kind) = kind(e)? {
            out.push(Entity::new(kind, e.offset as i32, e.length as i32));
        }
    }
    Ok(out)
}

fn kind(e: &bot::MessageEntity) -> Result<Option<Kind>, String> {
    let required = |field: &str| format!("Can't find field \"{field}\"");
    Ok(Some(match e.r#type.as_str() {
        "" => return Err("Type is not specified".into()),
        "bold" => Kind::Bold,
        "italic" => Kind::Italic,
        "underline" => Kind::Underline,
        "strikethrough" => Kind::Strikethrough,
        "spoiler" => Kind::Spoiler,
        "code" => Kind::Code,
        "pre" => match e.language.as_deref() {
            Some(language) if !language.is_empty() => Kind::PreCode(language.to_owned()),
            _ => Kind::Pre,
        },
        "text_link" => Kind::TextUrl(e.url.clone().ok_or_else(|| required("url"))?),
        "text_mention" => Kind::MentionName(e.user.as_ref().ok_or_else(|| required("user"))?.id),
        "custom_emoji" => Kind::CustomEmoji(
            e.custom_emoji_id
                .as_deref()
                .ok_or_else(|| required("custom_emoji_id"))?
                .parse()
                .map_err(|_| "Can't parse custom_emoji_id")?,
        ),
        "blockquote" => Kind::BlockQuote,
        "expandable_blockquote" => Kind::ExpandableBlockQuote,
        "date_time" => Kind::FormattedDate {
            unix: e.unix_time.ok_or_else(|| required("unix_time"))? as i32,
            format: DateFormat::parse(e.date_time_format.as_deref().unwrap_or(""))?,
        },
        "mention" | "hashtag" | "cashtag" | "bot_command" | "url" | "email" | "phone_number"
        | "bank_card_number" => {
            return Ok(None);
        }
        _ => return Err("Unsupported type specified".into()),
    }))
}
