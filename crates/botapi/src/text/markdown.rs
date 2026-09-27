//! Legacy `Markdown`, ported from tdlib's `parse_markdown`.

use super::bytes::{at, is_space, lossy, utf16_units};
use super::entity::{Entity, Kind};
use super::link;

pub fn parse(text: &str) -> Result<(String, Vec<Entity>), String> {
    let t = text.as_bytes();
    let size = t.len();
    let mut out = Vec::with_capacity(size);
    let mut entities = Vec::new();
    let mut utf16 = 0i32;
    let mut i = 0;

    while i < size {
        let c = t[i];
        if c == b'\\' && matches!(at(t, i + 1), b'_' | b'*' | b'`' | b'[') {
            i += 1;
            out.push(t[i]);
            utf16 += 1;
            i += 1;
            continue;
        }
        if !matches!(c, b'_' | b'*' | b'`' | b'[') {
            utf16 += utf16_units(c);
            out.push(c);
            i += 1;
            continue;
        }

        // The beginning of an entity.
        let begin_pos = i;
        let end_character = if c == b'[' { b']' } else { c };
        let mut is_pre = false;
        i += 1;

        let mut language = String::new();
        if c == b'`' && at(t, i) == b'`' && at(t, i + 1) == b'`' {
            i += 2;
            is_pre = true;
            let mut language_end = i;
            while !is_space(at(t, language_end)) && at(t, language_end) != b'`' {
                language_end += 1;
            }
            if i != language_end && language_end < size && at(t, language_end) != b'`' {
                language = lossy(&t[i..language_end]);
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
        }

        let entity_offset = utf16;
        while i < size && (t[i] != end_character || (is_pre && !(at(t, i + 1) == b'`' && at(t, i + 2) == b'`'))) {
            utf16 += utf16_units(t[i]);
            out.push(t[i]);
            i += 1;
        }
        if i == size {
            return Err(format!("Can't find end of the entity starting at byte offset {begin_pos}"));
        }

        if entity_offset != utf16 {
            let length = utf16 - entity_offset;
            let kind = match c {
                b'_' => Some(Kind::Italic),
                b'*' => Some(Kind::Bold),
                b'[' => {
                    let url = if at(t, i + 1) != b'(' {
                        // The text itself is the URL.
                        lossy(&t[begin_pos + 1..i])
                    } else {
                        i += 2;
                        let start = i;
                        while i < size && t[i] != b')' {
                            i += 1;
                        }
                        lossy(&t[start..i])
                    };
                    match link::user_id(&url) {
                        Some(user) => Some(Kind::MentionName(user)),
                        None => link::checked(&url).map(Kind::TextUrl),
                    }
                }
                _ if is_pre && language.is_empty() => Some(Kind::Pre),
                _ if is_pre => Some(Kind::PreCode(language)),
                _ => Some(Kind::Code),
            };
            if let Some(kind) = kind {
                entities.push(Entity::new(kind, entity_offset, length));
            }
        }
        if is_pre {
            i += 2;
        }
        i += 1;
    }
    Ok((String::from_utf8(out).map_err(|_| "Strings must be encoded in UTF-8".to_owned())?, entities))
}
