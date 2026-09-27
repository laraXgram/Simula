//! TL `MessageEntity` → Bot API `MessageEntity`.

use simula_botapi_types::types as bot;
use simula_tl::enums;

use super::{Entities, user::user};

pub fn entity(e: &enums::MessageEntity, users: &Entities<'_>) -> Option<bot::MessageEntity> {
    use enums::MessageEntity as E;
    let base = |kind: &str| bot::MessageEntity {
        r#type: kind.into(),
        offset: e.offset().into(),
        length: e.length().into(),
        ..Default::default()
    };
    Some(match e {
        E::Mention(_) => base("mention"),
        E::Hashtag(_) => base("hashtag"),
        E::Cashtag(_) => base("cashtag"),
        E::BotCommand(_) => base("bot_command"),
        E::Url(_) => base("url"),
        E::Email(_) => base("email"),
        E::Phone(_) => base("phone_number"),
        E::BankCard(_) => base("bank_card_number"),
        E::Bold(_) => base("bold"),
        E::Italic(_) => base("italic"),
        E::Underline(_) => base("underline"),
        E::Strike(_) => base("strikethrough"),
        E::Spoiler(_) => base("spoiler"),
        E::Code(_) => base("code"),
        E::Pre(p) => bot::MessageEntity { language: (!p.language.is_empty()).then(|| p.language.clone()), ..base("pre") },
        E::TextUrl(t) => bot::MessageEntity { url: Some(t.url.clone()), ..base("text_link") },
        E::MentionName(m) => bot::MessageEntity {
            user: Some(Box::new(users.user(m.user_id).map(|u| user(u, false)).unwrap_or(bot::User {
                id: m.user_id,
                ..Default::default()
            }))),
            ..base("text_mention")
        },
        E::CustomEmoji(c) => bot::MessageEntity { custom_emoji_id: Some(c.document_id.to_string()), ..base("custom_emoji") },
        E::Blockquote(b) if b.collapsed => base("expandable_blockquote"),
        E::Blockquote(_) => base("blockquote"),
        E::FormattedDate(d) => {
            let format = if d.relative {
                "r".to_owned()
            } else {
                [(d.day_of_week, 'w'), (d.short_date, 'd'), (d.long_date, 'D'), (d.short_time, 't'), (d.long_time, 'T')]
                    .into_iter()
                    .filter_map(|(on, c)| on.then_some(c))
                    .collect()
            };
            bot::MessageEntity {
                unix_time: Some(d.date.into()),
                date_time_format: (!format.is_empty()).then_some(format),
                ..base("date_time")
            }
        }
        _ => return None,
    })
}

pub fn entities(list: Option<&Vec<enums::MessageEntity>>, users: &Entities<'_>) -> Option<Vec<bot::MessageEntity>> {
    let converted: Vec<_> = list?.iter().filter_map(|e| entity(e, users)).collect();
    (!converted.is_empty()).then_some(converted)
}
