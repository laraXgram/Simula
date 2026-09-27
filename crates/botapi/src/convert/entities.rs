//! TL `MessageEntity` → Bot API `MessageEntity`.

use simula_botapi_types::types as bot;
use simula_tl::enums;

pub fn entity(e: &enums::MessageEntity) -> Option<bot::MessageEntity> {
    use enums::MessageEntity as E;
    let (kind, offset, length, url, language) = match e {
        E::Mention(e) => ("mention", e.offset, e.length, None, None),
        E::Hashtag(e) => ("hashtag", e.offset, e.length, None, None),
        E::Cashtag(e) => ("cashtag", e.offset, e.length, None, None),
        E::BotCommand(e) => ("bot_command", e.offset, e.length, None, None),
        E::Url(e) => ("url", e.offset, e.length, None, None),
        E::Email(e) => ("email", e.offset, e.length, None, None),
        E::Phone(e) => ("phone_number", e.offset, e.length, None, None),
        E::Bold(e) => ("bold", e.offset, e.length, None, None),
        E::Italic(e) => ("italic", e.offset, e.length, None, None),
        E::Underline(e) => ("underline", e.offset, e.length, None, None),
        E::Strike(e) => ("strikethrough", e.offset, e.length, None, None),
        E::Spoiler(e) => ("spoiler", e.offset, e.length, None, None),
        E::Code(e) => ("code", e.offset, e.length, None, None),
        E::Pre(e) => (
            "pre",
            e.offset,
            e.length,
            None,
            (!e.language.is_empty()).then(|| e.language.clone()),
        ),
        E::TextUrl(e) => ("text_link", e.offset, e.length, Some(e.url.clone()), None),
        E::Blockquote(e) if e.collapsed => {
            ("expandable_blockquote", e.offset, e.length, None, None)
        }
        E::Blockquote(e) => ("blockquote", e.offset, e.length, None, None),
        _ => return None,
    };
    Some(bot::MessageEntity {
        r#type: kind.into(),
        offset: offset.into(),
        length: length.into(),
        url,
        language,
        ..Default::default()
    })
}

pub fn entities(list: Option<&Vec<enums::MessageEntity>>) -> Option<Vec<bot::MessageEntity>> {
    let converted: Vec<_> = list?.iter().filter_map(entity).collect();
    (!converted.is_empty()).then_some(converted)
}
