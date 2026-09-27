//! `MarkdownV2`, ported from tdlib's `parse_markdown_v2`.

use super::bytes::{at, is_space, lossy, utf16_units};
use super::entity::{self, DateFormat, Entity, Kind};
use super::link;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Open {
    Bold,
    Italic,
    Underline,
    Strikethrough,
    Spoiler,
    Code,
    Pre,
    PreCode,
    TextUrl,
    CustomEmoji,
    BlockQuote,
}

impl Open {
    fn name(self) -> &'static str {
        match self {
            Open::Bold => "Bold",
            Open::Italic => "Italic",
            Open::Underline => "Underline",
            Open::Strikethrough => "Strikethrough",
            Open::Spoiler => "Spoiler",
            Open::Code => "Code",
            Open::Pre => "Pre",
            Open::PreCode => "PreCode",
            Open::TextUrl => "TextUrl",
            Open::CustomEmoji => "CustomEmoji",
            Open::BlockQuote => "BlockQuote",
        }
    }
}

struct Info {
    open: Open,
    argument: String,
    entity_offset: i32,
    entity_byte_offset: usize,
    entity_begin_pos: usize,
}

fn reserved(c: u8) -> String {
    format!("Character '{}' is reserved and must be escaped with the preceding '\\'", c as char)
}

fn unclosed(info: &Info) -> String {
    format!("Can't find end of {} entity at byte offset {}", info.open.name(), info.entity_byte_offset)
}

/// Read `(url)` starting at `i` (just past `(`), with `\` escapes. Returns the URL and the
/// index of the closing `)`.
fn read_url(t: &[u8], mut i: usize) -> Result<(String, usize), String> {
    let begin = i;
    let mut url = Vec::new();
    while i < t.len() && t[i] != b')' {
        if t[i] == b'\\' && (1..=126).contains(&at(t, i + 1)) {
            url.push(t[i + 1]);
            i += 2;
            continue;
        }
        url.push(t[i]);
        i += 1;
    }
    if at(t, i) != b')' {
        return Err(format!("Can't find end of a URL at byte offset {begin}"));
    }
    Ok((lossy(&url), i))
}

pub fn parse(text: &str) -> Result<(String, Vec<Entity>), String> {
    let t = text.as_bytes();
    let size = t.len();
    let mut out: Vec<u8> = Vec::with_capacity(size);
    let mut entities = Vec::new();
    let mut utf16 = 0i32;
    let mut nested: Vec<Info> = Vec::new();
    let mut have_blockquote = false;
    let mut can_start_blockquote = true;
    let mut i = 0;

    while i < size {
        let c = t[i];
        if c == b'\\' && (1..=126).contains(&at(t, i + 1)) {
            i += 1;
            utf16 += 1;
            out.push(t[i]);
            if t[i] != b'\r' {
                can_start_blockquote = t[i] == b'\n';
            }
            i += 1;
            continue;
        }

        let reserved_characters: &[u8] = match nested.last().map(|n| n.open) {
            Some(Open::Code | Open::Pre | Open::PreCode) => b"`",
            _ => b"_*[]()~`>#+-=|{}.!\n",
        };
        if !reserved_characters.contains(&c) {
            if c & 0xc0 != 0x80 {
                utf16 += utf16_units(c);
                if c != b'\r' {
                    can_start_blockquote = false;
                }
            }
            out.push(c);
            i += 1;
            continue;
        }

        let is_end_of_an_entity = match nested.last() {
            None => false,
            Some(_) if have_blockquote && c == b'\n' && (i + 1 == size || t[i + 1] != b'>') => true,
            Some(top) => match top.open {
                Open::Bold => c == b'*',
                Open::Italic => c == b'_' && at(t, i + 1) != b'_',
                Open::Code => c == b'`',
                Open::Pre | Open::PreCode => c == b'`' && at(t, i + 1) == b'`' && at(t, i + 2) == b'`',
                Open::TextUrl | Open::CustomEmoji => c == b']',
                Open::Underline => c == b'_' && at(t, i + 1) == b'_',
                Open::Strikethrough => c == b'~',
                Open::Spoiler => c == b'|' && at(t, i + 1) == b'|',
                Open::BlockQuote => false,
            },
        };

        if !is_end_of_an_entity {
            // The beginning of an entity.
            let entity_byte_offset = i;
            let mut argument = String::new();
            let open = match c {
                b'_' if at(t, i + 1) == b'_' => {
                    i += 1;
                    Some(Open::Underline)
                }
                b'_' => Some(Open::Italic),
                b'*' => Some(Open::Bold),
                b'~' => Some(Open::Strikethrough),
                b'|' if at(t, i + 1) == b'|' => {
                    i += 1;
                    Some(Open::Spoiler)
                }
                b'[' => Some(Open::TextUrl),
                b'`' if at(t, i + 1) == b'`' && at(t, i + 2) == b'`' => {
                    i += 3;
                    let mut open = Open::Pre;
                    let mut language_end = i;
                    while !is_space(at(t, language_end)) && at(t, language_end) != b'`' {
                        language_end += 1;
                    }
                    if i != language_end && language_end < size && at(t, language_end) != b'`' {
                        open = Open::PreCode;
                        argument = lossy(&t[i..language_end]);
                        i = language_end;
                    }
                    // Skip one new line at the beginning of the text.
                    if matches!(at(t, i), b'\n' | b'\r') {
                        if matches!(at(t, i + 1), b'\n' | b'\r') && at(t, i) != at(t, i + 1) {
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }
                    i -= 1;
                    Some(open)
                }
                b'`' => Some(Open::Code),
                b'!' if at(t, i + 1) == b'[' => {
                    i += 1;
                    Some(Open::CustomEmoji)
                }
                b'\n' => {
                    utf16 += 1;
                    out.push(b'\n');
                    can_start_blockquote = true;
                    None
                }
                b'>' if can_start_blockquote => {
                    if have_blockquote {
                        None
                    } else {
                        have_blockquote = true;
                        Some(Open::BlockQuote)
                    }
                }
                _ => return Err(reserved(c)),
            };
            if let Some(open) = open {
                nested.push(Info { open, argument, entity_offset: utf16, entity_byte_offset, entity_begin_pos: out.len() });
            }
        } else {
            // The end of an entity.
            let mut expandable = false;
            let top = nested.last().expect("an entity is open");
            if c == b'\n' && top.open != Open::BlockQuote {
                let spoiler_just_opened = top.entity_byte_offset + 2 == i
                    || (top.entity_byte_offset + 3 == i && out.last() == Some(&b'\r'));
                if top.open != Open::Spoiler || !spoiler_just_opened {
                    return Err(unclosed(top));
                }
                nested.pop();
                let below = nested.last().expect("a blockquote is open");
                if below.open != Open::BlockQuote {
                    return Err(unclosed(below));
                }
                expandable = true;
            }

            let top = nested.last_mut().expect("an entity is open");
            let argument = std::mem::take(&mut top.argument);
            let (entity_offset, entity_begin_pos, open) = (top.entity_offset, top.entity_begin_pos, top.open);
            let mut skip = utf16 == entity_offset;
            let kind = match open {
                Open::Bold => Some(Kind::Bold),
                Open::Italic => Some(Kind::Italic),
                Open::Code => Some(Kind::Code),
                Open::Strikethrough => Some(Kind::Strikethrough),
                Open::Underline => {
                    i += 1;
                    Some(Kind::Underline)
                }
                Open::Spoiler => {
                    i += 1;
                    Some(Kind::Spoiler)
                }
                Open::Pre => {
                    i += 2;
                    Some(Kind::Pre)
                }
                Open::PreCode => {
                    i += 2;
                    Some(Kind::PreCode(argument))
                }
                Open::TextUrl => {
                    let url = if at(t, i + 1) != b'(' {
                        // The text itself is the URL.
                        lossy(&out[entity_begin_pos..])
                    } else {
                        let (url, end) = read_url(t, i + 2)?;
                        i = end;
                        url
                    };
                    match link::user_id(&url) {
                        Some(user) => Some(Kind::MentionName(user)),
                        None => match link::checked(&url) {
                            Some(url) => Some(Kind::TextUrl(url)),
                            None => {
                                skip = true;
                                None
                            }
                        },
                    }
                }
                Open::CustomEmoji => {
                    if at(t, i + 1) != b'(' {
                        return Err("The entity must contain a tg://emoji or tg://time URL".into());
                    }
                    let (url, end) = read_url(t, i + 2)?;
                    i = end;
                    if let Some(id) = link::custom_emoji_id(&url) {
                        Some(Kind::CustomEmoji(id))
                    } else if let Some((unix, format)) = link::formatted_date(&url) {
                        Some(Kind::FormattedDate { unix, format: DateFormat::parse(&format)? })
                    } else {
                        return Err("Invalid tg://emoji or tg://time URL specified".into());
                    }
                }
                Open::BlockQuote => {
                    have_blockquote = false;
                    out.push(t[i]);
                    can_start_blockquote = true;
                    utf16 += 1;
                    skip = false;
                    Some(if expandable { Kind::ExpandableBlockQuote } else { Kind::BlockQuote })
                }
            };
            if !skip && let Some(kind) = kind {
                entities.push(Entity::new(kind, entity_offset, utf16 - entity_offset));
            }
            nested.pop();
        }
        i += 1;
    }

    if have_blockquote {
        let mut kind = Kind::BlockQuote;
        if let Some(top) = nested.last()
            && top.open == Open::Spoiler
            && top.entity_byte_offset + 2 == size
        {
            nested.pop();
            kind = Kind::ExpandableBlockQuote;
        }
        if let Some(top) = nested.last()
            && top.open == Open::BlockQuote
        {
            let length = utf16 - top.entity_offset;
            if length != 0 {
                entities.push(Entity::new(kind, top.entity_offset, length));
            }
            nested.pop();
        }
    }
    if let Some(top) = nested.last() {
        return Err(unclosed(top));
    }

    entity::sort(&mut entities);
    Ok((String::from_utf8(out).map_err(|_| "Strings must be encoded in UTF-8".to_owned())?, entities))
}
