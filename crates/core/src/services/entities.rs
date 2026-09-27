//! Automatic entities (mentions, bot commands, hashtags, cashtags, URLs, emails), ported
//! from tdlib's `MessageEntity.cpp` (`match_mentions`, `match_bot_commands`, …) so
//! messages carry the same entities as on Telegram. Offsets are in UTF-16 code units.

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::services::peers;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Mention,
    BotCommand,
    Hashtag,
    Cashtag,
    Url,
    Email,
}

/// A detected entity as a byte range of the text.
#[derive(Debug, Clone, Copy)]
struct Found {
    kind: Kind,
    start: usize,
    end: usize,
}

/// Mentions shorter than 4 characters are only valid for these (tdlib's list).
const SHORT_USERNAMES: &[&str] = &["gif", "nft", "pic", "ufc", "vid"];

/// Entities Telegram adds by itself. Bot commands only count in chats with a bot.
pub fn detect(text: &str, with_bot_commands: bool) -> Vec<enums::MessageEntity> {
    let chars: Vec<(usize, char)> = text.char_indices().collect();
    let mut found = Vec::new();
    found.extend(mentions(text, &chars));
    if with_bot_commands {
        found.extend(bot_commands(text, &chars));
    }
    found.extend(hashtags(text, &chars));
    found.extend(cashtags(text, &chars));
    found.extend(urls(text));

    // Overlaps: the earliest (then longest) wins, as after tdlib's fix-up.
    found.sort_by_key(|f| (f.start, std::cmp::Reverse(f.end)));
    let mut kept: Vec<Found> = Vec::new();
    for f in found {
        if kept.last().is_none_or(|last| f.start >= last.end) {
            kept.push(f);
        }
    }
    kept.into_iter().map(|f| to_tl(text, f)).collect()
}

/// Explicit entities win; detected ones overlapping them are dropped (tdlib's
/// `merge_entities`).
pub fn merge(
    explicit: Vec<enums::MessageEntity>,
    detected: Vec<enums::MessageEntity>,
) -> Vec<enums::MessageEntity> {
    let spans: Vec<(i32, i32)> = explicit.iter().map(span).collect();
    let mut all = explicit;
    all.extend(detected.into_iter().filter(|d| {
        let (start, end) = span(d);
        spans.iter().all(|&(s, e)| end <= s || start >= e)
    }));
    all.sort_by_key(|e| span(e).0);
    all
}

/// Client-sent entities as stored: a mention by `InputUser` becomes a mention by user id,
/// after checking the sender may address that user.
pub fn resolve_input(ctx: &Ctx<'_>, sender: crate::ids::UserId, entities: Vec<enums::MessageEntity>) -> RpcResult<Vec<enums::MessageEntity>> {
    entities
        .into_iter()
        .map(|entity| match entity {
            enums::MessageEntity::InputMessageEntityMentionName(e) => {
                let user = peers::resolve_input_user(ctx, sender, &e.user_id)
                    .map_err(|_| RpcError::bad_request("ENTITY_MENTION_USER_INVALID"))?;
                Ok(types::MessageEntityMentionName { offset: e.offset, length: e.length, user_id: user.0 }.into())
            }
            other => Ok(other),
        })
        .collect()
}

fn span(entity: &enums::MessageEntity) -> (i32, i32) {
    (entity.offset(), entity.offset() + entity.length())
}

fn to_tl(text: &str, f: Found) -> enums::MessageEntity {
    let offset = utf16_len(&text[..f.start]);
    let length = utf16_len(&text[f.start..f.end]);
    match f.kind {
        Kind::Mention => types::MessageEntityMention { offset, length }.into(),
        Kind::BotCommand => types::MessageEntityBotCommand { offset, length }.into(),
        Kind::Hashtag => types::MessageEntityHashtag { offset, length }.into(),
        Kind::Cashtag => types::MessageEntityCashtag { offset, length }.into(),
        Kind::Url => types::MessageEntityUrl { offset, length }.into(),
        Kind::Email => types::MessageEntityEmail { offset, length }.into(),
    }
}

pub fn utf16_len(s: &str) -> i32 {
    s.encode_utf16().count() as i32
}

fn is_word(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn is_ascii_word(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn is_hashtag_letter(c: char) -> bool {
    c == '_'
        || c == '\u{200c}'
        || c == '\u{b7}'
        || ('\u{d80}'..='\u{dff}').contains(&c)
        || c.is_alphanumeric()
}

/// Character before / at a char index, if any.
fn prev(chars: &[(usize, char)], i: usize) -> Option<char> {
    i.checked_sub(1).map(|j| chars[j].1)
}

fn at(chars: &[(usize, char)], i: usize) -> Option<char> {
    chars.get(i).map(|&(_, c)| c)
}

fn byte(text: &str, chars: &[(usize, char)], i: usize) -> usize {
    chars.get(i).map_or(text.len(), |&(b, _)| b)
}

/// Advance over ASCII word characters from char index `i`.
fn scan_ascii_word(chars: &[(usize, char)], mut i: usize) -> usize {
    while at(chars, i).is_some_and(is_ascii_word) {
        i += 1;
    }
    i
}

fn mentions(text: &str, chars: &[(usize, char)]) -> Vec<Found> {
    let mut out = Vec::new();
    for (i, &(start, c)) in chars.iter().enumerate() {
        if c != '@' || prev(chars, i).is_some_and(is_word) {
            continue;
        }
        let end = scan_ascii_word(chars, i + 1);
        let size = end - i - 1;
        if !(2..=32).contains(&size) || at(chars, end).is_some_and(is_word) {
            continue;
        }
        let name = &text[byte(text, chars, i + 1)..byte(text, chars, end)];
        if size < 4 && !SHORT_USERNAMES.contains(&name.to_ascii_lowercase().as_str()) {
            continue;
        }
        out.push(Found {
            kind: Kind::Mention,
            start,
            end: byte(text, chars, end),
        });
    }
    out
}

fn bot_commands(text: &str, chars: &[(usize, char)]) -> Vec<Found> {
    let blocks = |c: char| is_word(c) || matches!(c, '/' | '<' | '>');
    let mut out = Vec::new();
    for (i, &(start, c)) in chars.iter().enumerate() {
        if c != '/' || prev(chars, i).is_some_and(blocks) {
            continue;
        }
        let mut end = scan_ascii_word(chars, i + 1);
        if !(1..=64).contains(&(end - i - 1)) {
            continue;
        }
        if at(chars, end) == Some('@') {
            let username_end = scan_ascii_word(chars, end + 1);
            if !(3..=32).contains(&(username_end - end - 1)) {
                continue;
            }
            end = username_end;
        }
        if at(chars, end).is_some_and(blocks) {
            continue;
        }
        out.push(Found {
            kind: Kind::BotCommand,
            start,
            end: byte(text, chars, end),
        });
    }
    out
}

fn hashtags(text: &str, chars: &[(usize, char)]) -> Vec<Found> {
    let mut out = Vec::new();
    for (i, &(start, c)) in chars.iter().enumerate() {
        if c != '#' || prev(chars, i).is_some_and(is_hashtag_letter) {
            continue;
        }
        let mut ptr = i + 1;
        let mut size = 0;
        let mut end = None;
        let mut was_letter = false;
        while let Some(c) = at(chars, ptr).filter(|&c| is_hashtag_letter(c)) {
            ptr += 1;
            if size == 255 {
                end = Some(ptr);
            }
            if size != 256 {
                was_letter |= c.is_alphabetic();
                size += 1;
            }
        }
        let mut end = end.unwrap_or(ptr);
        if size < 1 {
            continue;
        }
        if end == ptr && at(chars, ptr) == Some('@') {
            let mut username_end = ptr + 1;
            while username_end - ptr < 33 && at(chars, username_end).is_some_and(is_ascii_word) {
                username_end += 1;
            }
            let length = username_end - ptr - 1;
            if length >= 3 {
                ptr = username_end;
                end = username_end;
            }
        }
        if at(chars, ptr) == Some('#') || !was_letter {
            continue;
        }
        out.push(Found {
            kind: Kind::Hashtag,
            start,
            end: byte(text, chars, end),
        });
    }
    out
}

fn cashtags(text: &str, chars: &[(usize, char)]) -> Vec<Found> {
    let mut out = Vec::new();
    for (i, &(start, c)) in chars.iter().enumerate() {
        if c != '$' || prev(chars, i).is_some_and(|p| is_hashtag_letter(p) || p == '$') {
            continue;
        }
        let begin = i + 1;
        let mut ptr = begin;
        if text[byte(text, chars, begin)..].starts_with("1INCH") {
            ptr += 5;
        } else {
            while at(chars, ptr).is_some_and(|c| c.is_ascii_uppercase()) {
                ptr += 1;
            }
        }
        if !(1..=8).contains(&(ptr - begin)) {
            continue;
        }
        let mut end = ptr;
        if at(chars, ptr) == Some('@') {
            let username_end = scan_ascii_word(chars, ptr + 1);
            if (3..=32).contains(&(username_end - ptr - 1)) {
                end = username_end;
                ptr = username_end;
            }
        }
        if at(chars, ptr).is_some_and(|c| is_hashtag_letter(c) || c == '$') {
            continue;
        }
        out.push(Found {
            kind: Kind::Cashtag,
            start,
            end: byte(text, chars, end),
        });
    }
    out
}

/// Scheme URLs (`http(s)://`, `tg://`), `www.` links and emails. tdlib also finds bare
/// domains using a TLD list; that part is not ported yet.
fn urls(text: &str) -> Vec<Found> {
    let mut out = Vec::new();
    let mut offset = 0;
    for word in text.split_inclusive(char::is_whitespace) {
        let token = word.trim_end_matches(char::is_whitespace);
        let token =
            token.trim_end_matches(['.', ',', ';', ':', '!', '?', ')', ']', '}', '\'', '"']);
        let lower = token.to_ascii_lowercase();
        let lead = token.len() - token.trim_start_matches(['(', '[', '{', '\'', '"']).len();
        let body = &lower[lead..];
        let start = offset + lead;
        let end = offset + token.len();
        if ["http://", "https://", "tg://", "www."]
            .iter()
            .any(|p| body.starts_with(p) && body.len() > p.len())
        {
            out.push(Found {
                kind: Kind::Url,
                start,
                end,
            });
        } else if is_email(&token[lead..]) {
            out.push(Found {
                kind: Kind::Email,
                start,
                end,
            });
        }
        offset += word.len();
    }
    out
}

fn is_email(s: &str) -> bool {
    let Some((local, domain)) = s.split_once('@') else {
        return false;
    };
    let local_ok = !local.is_empty()
        && local
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "._%+-".contains(c));
    let Some((host, tld)) = domain.rsplit_once('.') else {
        return false;
    };
    local_ok
        && !host.is_empty()
        && host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.')
        && tld.len() >= 2
        && tld.chars().all(|c| c.is_ascii_alphabetic())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn texts(text: &str, kind: Kind) -> Vec<String> {
        let chars: Vec<(usize, char)> = text.char_indices().collect();
        let found = match kind {
            Kind::Mention => mentions(text, &chars),
            Kind::BotCommand => bot_commands(text, &chars),
            Kind::Hashtag => hashtags(text, &chars),
            Kind::Cashtag => cashtags(text, &chars),
            Kind::Url | Kind::Email => urls(text).into_iter().filter(|f| f.kind == kind).collect(),
        };
        found
            .into_iter()
            .map(|f| text[f.start..f.end].to_owned())
            .collect()
    }

    // Cases from tdlib's td/test/message_entities.cpp.

    #[test]
    fn tdlib_mentions() {
        assert_eq!(texts("@mention", Kind::Mention), ["@mention"]);
        assert_eq!(texts(" @mention ", Kind::Mention), ["@mention"]);
        assert!(texts("@abc @xyz @abc @xyz @xxx@yyy @ttt", Kind::Mention).is_empty());
        assert!(texts("no@mention", Kind::Mention).is_empty());
        assert!(texts("@n", Kind::Mention).is_empty());
        assert_eq!(
            texts("@abcdefghijklmnopqrstuvwxyz123456", Kind::Mention).len(),
            1
        );
        assert!(texts("@abcdefghijklmnopqrstuvwxyz1234567", Kind::Mention).is_empty());
        assert!(texts("нет@mention", Kind::Mention).is_empty());
        assert_eq!(texts("@gif @vid", Kind::Mention), ["@gif", "@vid"]);
    }

    #[test]
    fn tdlib_bot_commands() {
        assert_eq!(texts("/abc", Kind::BotCommand), ["/abc"]);
        assert_eq!(texts(" /abc ", Kind::BotCommand), ["/abc"]);
        assert_eq!(texts("/a@abc", Kind::BotCommand), ["/a@abc"]);
        assert!(texts("/a@b", Kind::BotCommand).is_empty());
        assert!(texts("/@bfdsa", Kind::BotCommand).is_empty());
        assert!(texts("/test/", Kind::BotCommand).is_empty());
    }

    #[test]
    fn tdlib_hashtags() {
        for none in ["", "#", "##", "###", " я#a ", " #a# ", " #123 ", " #123a# "] {
            assert!(texts(none, Kind::Hashtag).is_empty(), "{none:?}");
        }
        assert_eq!(texts(" #я ", Kind::Hashtag), ["#я"]);
        assert_eq!(texts(" #123a ", Kind::Hashtag), ["#123a"]);
        assert_eq!(texts("#a#b #c #d", Kind::Hashtag), ["#c", "#d"]);
        assert_eq!(texts("#test@ab", Kind::Hashtag), ["#test"]);
        assert_eq!(texts("#test@abc", Kind::Hashtag), ["#test@abc"]);
        assert_eq!(texts("#te·st", Kind::Hashtag), ["#te·st"]);
        assert!(texts(&format!(" #{}", "1".repeat(256)), Kind::Hashtag).is_empty());
        let long = format!("#{}a", "1".repeat(255));
        assert_eq!(texts(&format!(" {long}"), Kind::Hashtag), [long]);
    }

    #[test]
    fn tdlib_cashtags() {
        for none in ["$", "$$", "$a", " $я ", "$abc", "$ABС", "$АBC"] {
            assert!(texts(none, Kind::Cashtag).is_empty(), "{none:?}");
        }
        assert_eq!(texts("$A", Kind::Cashtag), ["$A"]);
        assert_eq!(texts("$ABCDEFG", Kind::Cashtag), ["$ABCDEFG"]);
        assert!(texts("$ABCDEFGHI", Kind::Cashtag).is_empty());
        assert_eq!(texts("$1INCH", Kind::Cashtag), ["$1INCH"]);
    }

    #[test]
    fn urls_and_emails() {
        assert_eq!(
            texts("see https://example.com/a?b=1.", Kind::Url),
            ["https://example.com/a?b=1"]
        );
        assert_eq!(texts("(www.telegram.org)", Kind::Url), ["www.telegram.org"]);
        assert_eq!(
            texts("mail me: a.b@example.co.uk", Kind::Email),
            ["a.b@example.co.uk"]
        );
    }

    #[test]
    fn offsets_are_utf16() {
        // "😀" is two UTF-16 units; "سلام " is five.
        let entities = detect("😀 سلام /start", true);
        let value = serde_json::to_value(&entities[0]).unwrap();
        assert_eq!(
            (
                value["_"].as_str(),
                value["offset"].as_i64(),
                value["length"].as_i64()
            ),
            (Some("messageEntityBotCommand"), Some(8), Some(6))
        );
    }

    #[test]
    fn commands_need_a_bot_and_explicit_entities_win() {
        assert!(detect("/start", false).is_empty());
        let explicit: Vec<enums::MessageEntity> = vec![
            types::MessageEntityBold {
                offset: 0,
                length: 6,
            }
            .into(),
        ];
        let merged = merge(explicit, detect("/start now", true));
        assert_eq!(merged.len(), 1);
    }
}
