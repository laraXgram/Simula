//! Formatting entities as tdlib models them while parsing (offsets in UTF-16 units).

use simula_tl::{enums, types};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    Bold,
    Italic,
    Underline,
    Strikethrough,
    Spoiler,
    Code,
    Pre,
    PreCode(String),
    TextUrl(String),
    /// `tg://user?id=…`: a mention of a user by id (needs an access hash on the wire).
    MentionName(i64),
    CustomEmoji(i64),
    BlockQuote,
    ExpandableBlockQuote,
    /// `tg://time?unix=…&format=…`.
    FormattedDate { unix: i32, format: DateFormat },
}

impl Kind {
    /// tdlib's `MessageEntity::get_type_priority`, the tie-break when offset and length match.
    fn priority(&self) -> u8 {
        match self {
            Kind::BlockQuote | Kind::ExpandableBlockQuote => 0,
            Kind::PreCode(_) => 10,
            Kind::Pre => 11,
            Kind::Code => 20,
            Kind::FormattedDate { .. } => 30,
            Kind::TextUrl(_) | Kind::MentionName(_) => 49,
            Kind::Bold => 90,
            Kind::Italic => 91,
            Kind::Underline => 92,
            Kind::Strikethrough => 93,
            Kind::Spoiler => 94,
            Kind::CustomEmoji(_) => 99,
        }
    }
}

/// Display flags of a formatted date (tdlib's `FormattedDate::get_date_flags`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DateFormat {
    pub relative: bool,
    pub day_of_week: bool,
    pub short_date: bool,
    pub long_date: bool,
    pub short_time: bool,
    pub long_time: bool,
}

impl DateFormat {
    /// `r`/`R` alone means relative; otherwise any of `t T d D w W`.
    pub fn parse(format: &str) -> Result<Self, String> {
        if format == "r" || format == "R" {
            return Ok(Self { relative: true, ..Self::default() });
        }
        let mut flags = Self::default();
        for c in format.chars() {
            match c {
                't' => flags.short_time = true,
                'T' => flags.long_time = true,
                'd' => flags.short_date = true,
                'D' => flags.long_date = true,
                'w' | 'W' => flags.day_of_week = true,
                _ => return Err("Invalid date format used".into()),
            }
        }
        Ok(flags)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entity {
    pub kind: Kind,
    pub offset: i32,
    pub length: i32,
}

impl Entity {
    pub fn new(kind: Kind, offset: i32, length: i32) -> Self {
        Self { kind, offset, length }
    }
}

/// tdlib's order: by offset, then longer first, then by type priority.
pub fn sort(entities: &mut [Entity]) {
    entities.sort_by(|a, b| {
        a.offset
            .cmp(&b.offset)
            .then(b.length.cmp(&a.length))
            .then(a.kind.priority().cmp(&b.kind.priority()))
    });
}

/// The TL entity for `entity`. Mentions by id need an `InputUser` from the caller.
pub fn to_tl(entity: &Entity, input_user: impl Fn(i64) -> Option<enums::InputUser>) -> Option<enums::MessageEntity> {
    let (offset, length) = (entity.offset, entity.length);
    Some(match &entity.kind {
        Kind::Bold => types::MessageEntityBold { offset, length }.into(),
        Kind::Italic => types::MessageEntityItalic { offset, length }.into(),
        Kind::Underline => types::MessageEntityUnderline { offset, length }.into(),
        Kind::Strikethrough => types::MessageEntityStrike { offset, length }.into(),
        Kind::Spoiler => types::MessageEntitySpoiler { offset, length }.into(),
        Kind::Code => types::MessageEntityCode { offset, length }.into(),
        Kind::Pre => types::MessageEntityPre { offset, length, language: String::new() }.into(),
        Kind::PreCode(language) => types::MessageEntityPre { offset, length, language: language.clone() }.into(),
        Kind::TextUrl(url) => types::MessageEntityTextUrl { offset, length, url: url.clone() }.into(),
        Kind::MentionName(user_id) => {
            types::InputMessageEntityMentionName { offset, length, user_id: input_user(*user_id)? }.into()
        }
        Kind::CustomEmoji(document_id) => {
            types::MessageEntityCustomEmoji { offset, length, document_id: *document_id }.into()
        }
        Kind::BlockQuote => types::MessageEntityBlockquote { collapsed: false, offset, length }.into(),
        Kind::ExpandableBlockQuote => types::MessageEntityBlockquote { collapsed: true, offset, length }.into(),
        Kind::FormattedDate { unix, format } => types::MessageEntityFormattedDate {
            relative: format.relative,
            day_of_week: format.day_of_week,
            short_date: format.short_date,
            long_date: format.long_date,
            short_time: format.short_time,
            long_time: format.long_time,
            offset,
            length,
            date: *unix,
        }
        .into(),
    })
}
