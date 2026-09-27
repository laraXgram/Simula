//! The part of tdlib's `fix_formatted_text` that changes what is sent: invalid entities are
//! dropped, trailing whitespace is trimmed, leading spaces and newlines are trimmed up to
//! the first entity, and an empty result is an error.

use super::Formatted;
use super::entity::{self, Entity};

/// The fixed text and how many leading characters were trimmed (quotes shift by that).
pub fn fix(text: String, mut entities: Vec<Entity>) -> Result<(Formatted, i32), &'static str> {
    let total = utf16_len(&text);
    entities.retain(|e| e.offset >= 0 && e.length > 0 && e.offset + e.length <= total);
    entity::sort(&mut entities);

    // Right trim: drop trailing whitespace and entities past the last visible character.
    let Some((last_pos, last_char)) = text.char_indices().rev().find(|(_, c)| !c.is_whitespace())
    else {
        return Err("Text must be non-empty");
    };
    let end = last_pos + last_char.len_utf8();
    let visible = utf16_len(&text[..end]);
    let mut text = text[..end].to_owned();
    entities.retain(|e| e.offset < visible);
    for e in &mut entities {
        e.length = e.length.min(visible - e.offset);
    }
    entity::sort(&mut entities);

    // Left trim: spaces and newlines, but never into an entity.
    let first_entity = entities.first().map_or(i32::MAX, |e| e.offset);
    let trimmed = text
        .bytes()
        .take_while(|&b| b == b' ' || b == b'\n')
        .count()
        .min(first_entity.max(0) as usize);
    if trimmed > 0 {
        text.drain(..trimmed);
        for e in &mut entities {
            e.offset -= trimmed as i32;
        }
    }
    Ok((Formatted { text, entities }, trimmed as i32))
}

fn utf16_len(s: &str) -> i32 {
    s.encode_utf16().count() as i32
}
