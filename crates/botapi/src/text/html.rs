//! `HTML`, ported from tdlib's `parse_html` and `decode_html_entity`.

use super::bytes::{at, is_space, lossy, push_char, utf16_units};
use super::entity::{self, DateFormat, Entity, Kind};
use super::link;

const TAGS: &[&str] = &[
    "a", "b", "strong", "i", "em", "s", "strike", "del", "u", "ins", "tg-spoiler", "tg-emoji", "tg-time", "span",
    "pre", "code", "blockquote",
];

struct Info {
    tag: String,
    argument: String,
    date: i32,
    entity_offset: i32,
    entity_begin_pos: usize,
}

/// `&lt;`, `&gt;`, `&amp;`, `&quot;` and numeric references. Returns the code point and
/// the position after the reference.
fn decode_entity(t: &[u8], pos: usize) -> Option<(u32, usize)> {
    let mut end = pos + 1;
    let code: u32 = if at(t, pos + 1) == b'#' {
        end += 1;
        let mut value: u64 = 0;
        if at(t, pos + 2) == b'x' {
            end += 1;
            while at(t, end).is_ascii_hexdigit() {
                value = (value * 16 + u64::from((at(t, end) as char).to_digit(16)?)).min(u64::from(u32::MAX));
                end += 1;
            }
        } else {
            while at(t, end).is_ascii_digit() {
                value = (value * 10 + u64::from(at(t, end) - b'0')).min(u64::from(u32::MAX));
                end += 1;
            }
        }
        if value == 0 || value >= 0x10ffff || end - pos >= 10 {
            return None;
        }
        value as u32
    } else {
        while at(t, end).is_ascii_alphabetic() {
            end += 1;
        }
        match &t[pos + 1..end] {
            b"lt" => '<' as u32,
            b"gt" => '>' as u32,
            b"amp" => '&' as u32,
            b"quot" => '"' as u32,
            _ => return None,
        }
    };
    let next = if at(t, end) == b';' { end + 1 } else { end };
    Some((code, next))
}

fn lower(bytes: &[u8]) -> String {
    lossy(bytes).to_ascii_lowercase()
}

pub fn parse(text: &str) -> Result<(String, Vec<Entity>), String> {
    let t = text.as_bytes();
    let size = t.len();
    let mut out: Vec<u8> = Vec::with_capacity(size);
    let mut entities: Vec<Entity> = Vec::new();
    // Entity argument per index of `entities`, for the <pre><code class=…> merge.
    let mut code_arguments: Vec<String> = Vec::new();
    let mut utf16 = 0i32;
    let mut invalid_surrogate = false;
    let mut nested: Vec<Info> = Vec::new();
    let mut i = 0;

    while i < size {
        let c = t[i];
        if c == b'&'
            && let Some((code, next)) = decode_entity(t, i)
        {
            utf16 += 1 + i32::from(code > 0xffff);
            if (0xd800..=0xdfff).contains(&code) {
                invalid_surrogate = true;
            } else {
                push_char(&mut out, code);
            }
            i = next;
            continue;
        }
        if c != b'<' {
            utf16 += utf16_units(c);
            out.push(c);
            i += 1;
            continue;
        }

        let begin_pos = i;
        i += 1;
        if at(t, i) != b'/' {
            // The beginning of an entity.
            while !is_space(at(t, i)) && at(t, i) != b'>' {
                i += 1;
            }
            if at(t, i) == 0 {
                return Err(format!("Unclosed start tag at byte offset {begin_pos}"));
            }
            let tag = lower(&t[begin_pos + 1..i]);
            if !TAGS.contains(&tag.as_str()) {
                return Err(format!("Unsupported start tag \"{tag}\" at byte offset {begin_pos}"));
            }

            let mut argument = String::new();
            let mut date = 0;
            while at(t, i) != b'>' {
                while at(t, i) != 0 && is_space(at(t, i)) {
                    i += 1;
                }
                if at(t, i) == b'>' {
                    break;
                }
                let attribute_begin = i;
                while !is_space(at(t, i)) && !matches!(at(t, i), b'=' | b'>' | b'/' | b'"' | b'\'') {
                    i += 1;
                }
                let attribute = lossy(&t[attribute_begin..i]);
                if attribute.is_empty() {
                    return Err(format!("Empty attribute name in the tag \"{tag}\" at byte offset {begin_pos}"));
                }
                while at(t, i) != 0 && is_space(at(t, i)) {
                    i += 1;
                }
                if at(t, i) != b'=' {
                    if at(t, i) == 0 {
                        return Err(format!("Unclosed start tag \"{tag}\" at byte offset {begin_pos}"));
                    }
                    if tag == "blockquote" && attribute == "expandable" {
                        argument = "1".into();
                    }
                    continue;
                }
                i += 1;
                while at(t, i) != 0 && is_space(at(t, i)) {
                    i += 1;
                }
                if at(t, i) == 0 {
                    return Err(format!("Unclosed start tag \"{tag}\" at byte offset {begin_pos}"));
                }

                let value = if at(t, i) != b'\'' && at(t, i) != b'"' {
                    // A name token: letters, digits, periods, hyphens; case-insensitive.
                    let token_begin = i;
                    while at(t, i).is_ascii_alphanumeric() || matches!(at(t, i), b'.' | b'-') {
                        i += 1;
                    }
                    if !is_space(at(t, i)) && at(t, i) != b'>' {
                        return Err(format!("Unexpected end of name token at byte offset {token_begin}"));
                    }
                    lower(&t[token_begin..i])
                } else {
                    // A string literal.
                    let end_character = t[i];
                    i += 1;
                    let mut value = Vec::new();
                    while at(t, i) != end_character && at(t, i) != 0 {
                        if t[i] == b'&'
                            && let Some((code, next)) = decode_entity(t, i)
                        {
                            push_char(&mut value, code);
                            i = next;
                            continue;
                        }
                        value.push(t[i]);
                        i += 1;
                    }
                    if at(t, i) == end_character {
                        i += 1;
                    }
                    lossy(&value)
                };
                if at(t, i) == 0 {
                    return Err(format!("Unclosed start tag at byte offset {begin_pos}"));
                }

                match (tag.as_str(), attribute.as_str()) {
                    ("a", "href") | ("tg-emoji", "emoji-id") | ("tg-time", "format") => argument = value,
                    ("code", "class") if value.starts_with("language-") => argument = value[9..].to_owned(),
                    ("span", "class") if value.starts_with("tg-") => argument = value[3..].to_owned(),
                    ("blockquote", "expandable") => argument = "1".into(),
                    ("tg-time", "unix") => date = value.parse().unwrap_or(0),
                    _ => {}
                }
            }

            if tag == "span" && argument != "spoiler" {
                return Err(format!("Tag \"span\" must have class \"tg-spoiler\" at byte offset {begin_pos}"));
            }
            nested.push(Info { tag, argument, date, entity_offset: utf16, entity_begin_pos: out.len() });
        } else {
            // The end of an entity.
            if nested.is_empty() {
                return Err(format!("Unexpected end tag at byte offset {begin_pos}"));
            }
            while !is_space(at(t, i)) && at(t, i) != b'>' {
                i += 1;
            }
            let end_tag = lower(&t[begin_pos + 2..i]);
            while is_space(at(t, i)) && at(t, i) != 0 {
                i += 1;
            }
            if at(t, i) != b'>' {
                return Err(format!("Unclosed end tag at byte offset {begin_pos}"));
            }
            let top = nested.last_mut().expect("checked above");
            if !end_tag.is_empty() && end_tag != top.tag {
                return Err(format!(
                    "Unmatched end tag at byte offset {begin_pos}, expected \"</{}>\", found \"</{end_tag}>\"",
                    top.tag
                ));
            }

            if utf16 > top.entity_offset {
                let offset = top.entity_offset;
                let length = utf16 - offset;
                let same_span = |e: &Entity| e.offset == offset && e.length == length;
                let kind = match top.tag.as_str() {
                    "i" | "em" => Some(Kind::Italic),
                    "b" | "strong" => Some(Kind::Bold),
                    "s" | "strike" | "del" => Some(Kind::Strikethrough),
                    "u" | "ins" => Some(Kind::Underline),
                    "tg-spoiler" | "span" => Some(Kind::Spoiler),
                    "tg-emoji" => match top.argument.parse::<i64>() {
                        Ok(id) if id != 0 => Some(Kind::CustomEmoji(id)),
                        _ => return Err("Invalid custom emoji identifier specified".into()),
                    },
                    "tg-time" => {
                        let format = DateFormat::parse(&top.argument)?;
                        (top.date > 0).then_some(Kind::FormattedDate { unix: top.date, format })
                    }
                    "a" => {
                        let url = if top.argument.is_empty() {
                            lossy(&out[top.entity_begin_pos..])
                        } else {
                            std::mem::take(&mut top.argument)
                        };
                        match link::user_id(&url) {
                            Some(user) => Some(Kind::MentionName(user)),
                            None => link::checked(&url).map(Kind::TextUrl),
                        }
                    }
                    "pre" => {
                        // <pre><code class="language-x">…</code></pre> → one PreCode.
                        match (entities.last_mut(), code_arguments.last()) {
                            (Some(last), Some(language))
                                if last.kind == Kind::Code && same_span(last) && !language.is_empty() =>
                            {
                                last.kind = Kind::PreCode(language.clone());
                                None
                            }
                            _ => Some(Kind::Pre),
                        }
                    }
                    "code" => match entities.last_mut() {
                        Some(last) if last.kind == Kind::Pre && same_span(last) && !top.argument.is_empty() => {
                            last.kind = Kind::PreCode(std::mem::take(&mut top.argument));
                            None
                        }
                        _ => {
                            entities.push(Entity::new(Kind::Code, offset, length));
                            code_arguments.push(std::mem::take(&mut top.argument));
                            None
                        }
                    },
                    "blockquote" if !top.argument.is_empty() => Some(Kind::ExpandableBlockQuote),
                    "blockquote" => Some(Kind::BlockQuote),
                    _ => None,
                };
                if let Some(kind) = kind {
                    entities.push(Entity::new(kind, offset, length));
                    code_arguments.push(String::new());
                }
            }
            nested.pop();
        }
        i += 1;
    }
    if let Some(top) = nested.last() {
        return Err(format!("Can't find end tag corresponding to start tag \"{}\"", top.tag));
    }
    if invalid_surrogate {
        return Err(
            "Text contains invalid Unicode characters after decoding HTML entities, check for unmatched surrogate code units"
                .into(),
        );
    }
    entity::sort(&mut entities);
    Ok((String::from_utf8(out).map_err(|_| "Strings must be encoded in UTF-8".to_owned())?, entities))
}
