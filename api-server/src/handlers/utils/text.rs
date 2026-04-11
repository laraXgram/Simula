use serde_json::{json, Value};
use std::collections::HashMap;

pub fn parse_optional_formatted_text(
    text: Option<&str>,
    parse_mode: Option<&str>,
    explicit_entities: Option<Value>,
) -> (Option<String>, Option<Value>) {
    match text {
        Some(raw) if !raw.is_empty() => {
            let (plain, entities) = parse_formatted_text(raw, parse_mode, explicit_entities);
            (Some(plain), entities)
        }
        _ => (None, None),
    }
}

pub fn parse_formatted_text(
    text: &str,
    parse_mode: Option<&str>,
    explicit_entities: Option<Value>,
) -> (String, Option<Value>) {
    if let Some(entities) = explicit_entities {
        return (text.to_string(), Some(entities));
    }

    match parse_mode.map(|v| v.to_ascii_lowercase()) {
        Some(mode) if mode == "html" => {
            let (clean, entities) = parse_html_entities(text);
            (clean, entities_value(entities))
        }
        Some(mode) if mode == "markdown" || mode == "markdownv2" => {
            let (clean, entities) = parse_markdown_entities(text, mode == "markdownv2");
            (clean, entities_value(entities))
        }
        _ => (text.to_string(), None),
    }
}

pub fn merge_auto_message_entities(text: &str, entities: Option<Value>) -> Option<Value> {
    let mut merged_entities = match entities {
        Some(Value::Array(items)) => items,
        Some(other) => vec![other],
        None => Vec::new(),
    };

    let mut occupied = collect_occupied_entity_ranges(&merged_entities);
    append_auto_entities(
        text,
        &mut merged_entities,
        &mut occupied,
        "bot_command",
        find_auto_bot_command_spans,
    );
    append_auto_entities(
        text,
        &mut merged_entities,
        &mut occupied,
        "mention",
        find_auto_mention_spans,
    );
    append_auto_entities(
        text,
        &mut merged_entities,
        &mut occupied,
        "hashtag",
        find_auto_hashtag_spans,
    );
    append_auto_entities(
        text,
        &mut merged_entities,
        &mut occupied,
        "cashtag",
        find_auto_cashtag_spans,
    );

    merged_entities.sort_by_key(|entity| {
        entity
            .get("offset")
            .and_then(Value::as_u64)
            .unwrap_or_default()
    });

    entities_value(merged_entities)
}

pub fn collect_occupied_entity_ranges(entities: &[Value]) -> Vec<(usize, usize)> {
    entities
        .iter()
        .filter_map(|entity| {
            let offset = entity.get("offset").and_then(Value::as_i64)?;
            let length = entity.get("length").and_then(Value::as_i64)?;
            if offset < 0 || length <= 0 {
                return None;
            }
            let start = offset as usize;
            Some((start, start + length as usize))
        })
        .collect()
}

pub fn range_is_free(occupied: &[(usize, usize)], start: usize, end: usize) -> bool {
    occupied
        .iter()
        .all(|(range_start, range_end)| end <= *range_start || start >= *range_end)
}

pub fn append_auto_entities(
    text: &str,
    entities: &mut Vec<Value>,
    occupied: &mut Vec<(usize, usize)>,
    entity_type: &str,
    detector: fn(&str) -> Vec<(usize, usize)>,
) {
    for (start_byte, end_byte) in detector(text) {
        let start = utf16_len(&text[..start_byte]);
        let length = utf16_len(&text[start_byte..end_byte]);
        if length == 0 {
            continue;
        }
        let end = start + length;
        if !range_is_free(occupied, start, end) {
            continue;
        }

        entities.push(json!({
            "type": entity_type,
            "offset": start,
            "length": length,
        }));
        occupied.push((start, end));
    }
}

pub fn find_auto_bot_command_spans(text: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    for (start, ch) in text.char_indices() {
        if ch != '/' {
            continue;
        }
        if let Some(end) = match_bot_command_at(text, start) {
            spans.push((start, end));
        }
    }
    spans
}

pub fn find_auto_mention_spans(text: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    for (start, ch) in text.char_indices() {
        if ch != '@' {
            continue;
        }
        if let Some(end) = match_mention_at(text, start) {
            spans.push((start, end));
        }
    }
    spans
}

pub fn find_auto_hashtag_spans(text: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    for (start, ch) in text.char_indices() {
        if ch != '#' {
            continue;
        }
        if let Some(end) = match_hashtag_at(text, start) {
            spans.push((start, end));
        }
    }
    spans
}

pub fn find_auto_cashtag_spans(text: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    for (start, ch) in text.char_indices() {
        if ch != '$' {
            continue;
        }
        if let Some(end) = match_cashtag_at(text, start) {
            spans.push((start, end));
        }
    }
    spans
}

pub fn match_bot_command_at(text: &str, start: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut cursor = start + 1;
    if cursor >= bytes.len() || !bytes[cursor].is_ascii_alphabetic() {
        return None;
    }

    cursor += 1;
    while cursor < bytes.len()
        && is_ascii_entity_word_byte(bytes[cursor])
        && (cursor - (start + 1)) < 32
    {
        cursor += 1;
    }

    let mut end = cursor;
    if cursor < bytes.len() && bytes[cursor] == b'@' {
        let mut username_cursor = cursor + 1;
        let mut username_len = 0usize;

        while username_cursor < bytes.len()
            && is_ascii_entity_word_byte(bytes[username_cursor])
            && username_len < 32
        {
            username_cursor += 1;
            username_len += 1;
        }

        if username_len >= 5 {
            end = username_cursor;
        }
    }

    Some(end)
}

pub fn match_mention_at(text: &str, start: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut cursor = start + 1;
    let mut len = 0usize;

    while cursor < bytes.len() && is_ascii_entity_word_byte(bytes[cursor]) && len < 32 {
        cursor += 1;
        len += 1;
    }

    if len == 0 {
        return None;
    }

    Some(cursor)
}

pub fn match_hashtag_at(text: &str, start: usize) -> Option<usize> {
    let mut count = 0usize;
    let mut end = start + 1;

    for (rel, ch) in text[start + 1..].char_indices() {
        if !is_hashtag_char(ch) || count >= 64 {
            break;
        }
        count += 1;
        end = start + 1 + rel + ch.len_utf8();
    }

    if count == 0 {
        return None;
    }

    Some(end)
}

pub fn match_cashtag_at(text: &str, start: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut cursor = start + 1;
    let mut left_len = 0usize;

    while cursor < bytes.len() && bytes[cursor].is_ascii_alphabetic() && left_len < 8 {
        cursor += 1;
        left_len += 1;
    }

    if left_len == 0 {
        return None;
    }

    let mut end = cursor;
    if cursor < bytes.len() && bytes[cursor] == b'_' {
        let mut right_cursor = cursor + 1;
        let mut right_len = 0usize;

        while right_cursor < bytes.len() && bytes[right_cursor].is_ascii_alphabetic() && right_len < 8
        {
            right_cursor += 1;
            right_len += 1;
        }

        if right_len > 0 {
            end = right_cursor;
        }
    }

    Some(end)
}

pub fn is_ascii_entity_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

pub fn is_hashtag_char(ch: char) -> bool {
    ch == '_' || ch.is_alphanumeric()
}

pub fn utf16_span_to_byte_range(text: &str, offset: usize, length: usize) -> Option<(usize, usize)> {
    let target_end = offset.checked_add(length)?;
    let mut utf16_pos = 0usize;
    let mut start_byte = None;
    let mut end_byte = None;

    for (byte_idx, ch) in text.char_indices() {
        if start_byte.is_none() && utf16_pos == offset {
            start_byte = Some(byte_idx);
        }
        if utf16_pos == target_end {
            end_byte = Some(byte_idx);
            break;
        }

        utf16_pos += ch.len_utf16();
        if utf16_pos > target_end {
            return None;
        }
    }

    if start_byte.is_none() && utf16_pos == offset {
        start_byte = Some(text.len());
    }
    if end_byte.is_none() && utf16_pos == target_end {
        end_byte = Some(text.len());
    }

    match (start_byte, end_byte) {
        (Some(start), Some(end)) if start <= end => Some((start, end)),
        _ => None,
    }
}

pub fn entities_value(entities: Vec<Value>) -> Option<Value> {
    if entities.is_empty() {
        None
    } else {
        Some(Value::Array(entities))
    }
}

pub fn parse_html_entities(text: &str) -> (String, Vec<Value>) {
    let mut out = String::new();
    let mut entities = Vec::new();
    let mut stack: Vec<(String, usize, Option<String>, bool)> = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'<' {
            if let Some(end) = text[i..].find('>') {
                let end_idx = i + end;
                let raw_tag = &text[i + 1..end_idx];
                let tag = raw_tag.trim();

                let is_close = tag.starts_with('/');
                let lower = tag.to_ascii_lowercase();

                if is_close {
                    let name = lower.trim_start_matches('/').trim();
                    let wanted = match name {
                        "b" | "strong" => Some("bold"),
                        "i" | "em" => Some("italic"),
                        "u" | "ins" => Some("underline"),
                        "s" | "strike" | "del" => Some("strikethrough"),
                        "span" => Some("spoiler"),
                        "tg-spoiler" => Some("spoiler"),
                        "blockquote" => Some("blockquote"),
                        "tg-emoji" => Some("custom_emoji"),
                        "tg-time" => Some("date_time"),
                        "code" => Some("code"),
                        "pre" => Some("pre"),
                        "a" => Some("text_link"),
                        _ => None,
                    };

                    if let Some(target) = wanted {
                        if let Some(pos) = stack.iter().rposition(|(kind, _, _, _)| kind == target) {
                            let (_, start, extra, is_expandable) = stack.remove(pos);
                            let len = utf16_len(&out).saturating_sub(start);
                            if len > 0 {
                                let mut entity = json!({
                                    "type": if target == "blockquote" && is_expandable {
                                        "expandable_blockquote"
                                    } else {
                                        target
                                    },
                                    "offset": start,
                                    "length": len,
                                });
                                if let Some(extra) = extra {
                                    if target == "text_link" {
                                        entity["url"] = Value::String(extra);
                                    } else if target == "custom_emoji" {
                                        entity["custom_emoji_id"] = Value::String(extra);
                                    } else if target == "date_time" {
                                        let unix = extra
                                            .split(';')
                                            .find_map(|seg| seg.strip_prefix("unix:"))
                                            .and_then(|v| v.parse::<i64>().ok())
                                            .unwrap_or(0);
                                        entity["unix_time"] = Value::from(unix);
                                        if let Some(fmt) = extra
                                            .split(';')
                                            .find_map(|seg| seg.strip_prefix("format:"))
                                        {
                                            entity["date_time_format"] = Value::String(fmt.to_string());
                                        }
                                    } else if target == "pre" {
                                        if let Some(lang) = extra.strip_prefix("lang:") {
                                            entity["language"] = Value::String(lang.to_string());
                                        }
                                    }
                                }
                                entities.push(entity);
                            }
                        }
                    }
                } else {
                    let mut parts = lower.split_whitespace();
                    let name = parts.next().unwrap_or("");
                    let kind = match name {
                        "b" | "strong" => Some("bold"),
                        "i" | "em" => Some("italic"),
                        "u" | "ins" => Some("underline"),
                        "s" | "strike" | "del" => Some("strikethrough"),
                        "span" if has_css_class(tag, "tg-spoiler") => Some("spoiler"),
                        "tg-spoiler" => Some("spoiler"),
                        "blockquote" => Some("blockquote"),
                        "tg-emoji" => Some("custom_emoji"),
                        "tg-time" => Some("date_time"),
                        "code" => Some("code"),
                        "pre" => Some("pre"),
                        "a" => Some("text_link"),
                        _ => None,
                    };

                    if let Some(entity_type) = kind {
                        if entity_type == "code" {
                            if let Some(language) = extract_code_language(tag) {
                                if let Some((_, _, pre_extra, _)) = stack
                                    .iter_mut()
                                    .rev()
                                    .find(|(kind, _, _, _)| kind == "pre")
                                {
                                    *pre_extra = Some(format!("lang:{}", language));
                                    i = end_idx + 1;
                                    continue;
                                }
                            }
                        }

                        let start = utf16_len(&out);
                        let expandable = entity_type == "blockquote" && lower.contains("expandable");
                        let url = if entity_type == "text_link" { extract_href(tag) } else { None };
                        let extra = if entity_type == "custom_emoji" {
                            extract_attr(tag, "emoji-id").map(|v| format!("custom_emoji_id:{}", v))
                        } else if entity_type == "date_time" {
                            extract_attr(tag, "unix").map(|unix| {
                                let mut payload = format!("unix:{}", unix);
                                if let Some(fmt) = extract_attr(tag, "format") {
                                    payload.push_str(&format!(";format:{}", fmt));
                                }
                                payload
                            })
                        } else {
                            None
                        };
                        if let Some(payload) = extra {
                            let stored = if let Some(v) = payload.strip_prefix("custom_emoji_id:") {
                                v.to_string()
                            } else {
                                payload
                            };
                            stack.push((entity_type.to_string(), start, Some(stored), expandable));
                        } else {
                            stack.push((entity_type.to_string(), start, url, expandable));
                        }
                    }
                }

                i = end_idx + 1;
                continue;
            }
        }

        if bytes[i] == b'&' {
            if let Some(end) = text[i..].find(';') {
                let end_idx = i + end;
                let entity = &text[i..=end_idx];
                if let Some(decoded) = decode_html_entity(entity) {
                    out.push_str(decoded);
                    i = end_idx + 1;
                    continue;
                }
            }
        }

        if let Some(ch) = text[i..].chars().next() {
            out.push(ch);
            i += ch.len_utf8();
        } else {
            break;
        }
    }

    entities.sort_by_key(|entity| {
        entity
            .get("offset")
            .and_then(Value::as_u64)
            .unwrap_or_default()
    });

    (out, entities)
}

pub fn parse_markdown_entities(text: &str, markdown_v2: bool) -> (String, Vec<Value>) {
    let mut out = String::new();
    let mut entities = Vec::new();
    let mut stack: HashMap<&str, Vec<usize>> = HashMap::new();
    let mut i = 0;
    let mut line_start = true;

    while i < text.len() {
        if text[i..].starts_with("```") {
            if let Some((advance, code_text, language)) = parse_markdown_pre_block(&text[i..]) {
                let start = utf16_len(&out);
                out.push_str(&code_text);
                let len = utf16_len(&code_text);
                if len > 0 {
                    let mut entity = json!({
                        "type": "pre",
                        "offset": start,
                        "length": len,
                    });
                    if let Some(lang) = language {
                        entity["language"] = Value::String(lang);
                    }
                    entities.push(entity);
                }
                i += advance;
                continue;
            }
        }

        if markdown_v2 && text[i..].starts_with("![") {
            if let Some((advance, label, url)) = parse_markdown_media_link(&text[i..]) {
                let start = utf16_len(&out);
                out.push_str(&label);
                let len = utf16_len(&label);
                if len > 0 {
                    if let Some(id) = extract_query_param(&url, "id") {
                        if url.starts_with("tg://emoji") {
                            entities.push(json!({
                                "type": "custom_emoji",
                                "offset": start,
                                "length": len,
                                "custom_emoji_id": id,
                            }));
                        } else if url.starts_with("tg://time") {
                            let mut entity = json!({
                                "type": "date_time",
                                "offset": start,
                                "length": len,
                                "unix_time": extract_query_param(&url, "unix")
                                    .and_then(|v| v.parse::<i64>().ok())
                                    .unwrap_or(0),
                            });
                            if let Some(fmt) = extract_query_param(&url, "format") {
                                entity["date_time_format"] = Value::String(fmt);
                            }
                            entities.push(entity);
                        }
                    } else if url.starts_with("tg://time") {
                        let mut entity = json!({
                            "type": "date_time",
                            "offset": start,
                            "length": len,
                            "unix_time": extract_query_param(&url, "unix")
                                .and_then(|v| v.parse::<i64>().ok())
                                .unwrap_or(0),
                        });
                        if let Some(fmt) = extract_query_param(&url, "format") {
                            entity["date_time_format"] = Value::String(fmt);
                        }
                        entities.push(entity);
                    }
                }
                i += advance;
                continue;
            }
        }

        if markdown_v2 && text[i..].starts_with('\\') {
            let next_start = i + 1;
            if next_start < text.len() {
                if let Some(ch) = text[next_start..].chars().next() {
                    out.push(ch);
                    line_start = ch == '\n';
                    i = next_start + ch.len_utf8();
                    continue;
                }
            }
            i += 1;
            continue;
        }

        if markdown_v2 && line_start && (text[i..].starts_with('>') || text[i..].starts_with("**>")) {
            let mut start_shift = 1;
            let mut forced_expandable = false;
            if text[i..].starts_with("**>") {
                start_shift = 3;
                forced_expandable = true;
            }
            let line_end = text[i..].find('\n').map(|v| i + v).unwrap_or(text.len());
            let raw_line = &text[i + start_shift..line_end];
            let trimmed_line = raw_line.trim_start();
            let is_expandable = forced_expandable || trimmed_line.trim_end().ends_with("||");
            let content = if is_expandable {
                trimmed_line.trim_end().trim_end_matches("||").trim_end()
            } else {
                trimmed_line
            };

            let start = utf16_len(&out);
            out.push_str(content);
            let len = utf16_len(content);
            if len > 0 {
                entities.push(json!({
                    "type": if is_expandable { "expandable_blockquote" } else { "blockquote" },
                    "offset": start,
                    "length": len,
                }));
            }

            if line_end < text.len() {
                out.push('\n');
                i = line_end + 1;
                line_start = true;
            } else {
                i = line_end;
                line_start = false;
            }
            continue;
        }

        if text[i..].starts_with('[') {
            if let Some((advance, link_text, link_url)) = parse_markdown_link(&text[i..]) {
                let start = utf16_len(&out);
                out.push_str(&link_text);
                let len = utf16_len(&link_text);
                if len > 0 {
                    entities.push(json!({
                        "type": "text_link",
                        "offset": start,
                        "length": len,
                        "url": link_url,
                    }));
                }
                i += advance;
                continue;
            }
        }

        let mut matched = false;
        for (token, entity_type) in markdown_tokens(markdown_v2) {
            if !text[i..].starts_with(token) {
                continue;
            }

            matched = true;
            let start = utf16_len(&out);
            let entry = stack.entry(token).or_default();
            if let Some(open_start) = entry.pop() {
                let len = start.saturating_sub(open_start);
                if len > 0 {
                    entities.push(json!({
                        "type": entity_type,
                        "offset": open_start,
                        "length": len,
                    }));
                }
            } else {
                entry.push(start);
            }

            i += token.len();
            break;
        }

        if matched {
            continue;
        }

        if let Some(ch) = text[i..].chars().next() {
            out.push(ch);
            line_start = ch == '\n';
            i += ch.len_utf8();
        } else {
            break;
        }
    }

    entities.sort_by_key(|entity| {
        entity
            .get("offset")
            .and_then(Value::as_u64)
            .unwrap_or_default()
    });

    (out, entities)
}

pub fn parse_markdown_pre_block(input: &str) -> Option<(usize, String, Option<String>)> {
    if !input.starts_with("```") {
        return None;
    }

    let rest = &input[3..];
    let mut language = None;
    let mut content_start = 3;

    if let Some(line_end) = rest.find('\n') {
        let header = rest[..line_end].trim();
        if !header.is_empty() {
            language = Some(header.to_string());
        }
        content_start = 3 + line_end + 1;
    }

    let body = &input[content_start..];
    let close_rel = body.find("```")?;
    let close_abs = content_start + close_rel;
    let content = &input[content_start..close_abs];
    let advance = close_abs + 3;

    Some((advance, content.to_string(), language))
}

pub fn markdown_tokens(markdown_v2: bool) -> Vec<(&'static str, &'static str)> {
    if markdown_v2 {
        vec![
            ("||", "spoiler"),
            ("__", "underline"),
            ("*", "bold"),
            ("_", "italic"),
            ("~", "strikethrough"),
            ("`", "code"),
        ]
    } else {
        vec![("*", "bold"), ("_", "italic"), ("`", "code")]
    }
}

pub fn parse_markdown_link(input: &str) -> Option<(usize, String, String)> {
    let close_text = input.find(']')?;
    let rest = &input[close_text + 1..];
    if !rest.starts_with('(') {
        return None;
    }
    let close_url = rest.find(')')?;
    let text = &input[1..close_text];
    let url = &rest[1..close_url];
    let advance = close_text + 1 + close_url + 1;
    Some((advance, text.to_string(), url.to_string()))
}

pub fn parse_markdown_media_link(input: &str) -> Option<(usize, String, String)> {
    if !input.starts_with("![") {
        return None;
    }
    let close_label = input.find(']')?;
    let rest = &input[close_label + 1..];
    if !rest.starts_with('(') {
        return None;
    }
    let close_url = rest.find(')')?;
    let label = &input[2..close_label];
    let url = &rest[1..close_url];
    let advance = close_label + 1 + close_url + 1;
    Some((advance, label.to_string(), url.to_string()))
}

pub fn utf16_len(text: &str) -> usize {
    text.encode_utf16().count()
}

pub fn extract_href(tag: &str) -> Option<String> {
    extract_attr(tag, "href")
}

pub fn extract_attr(tag: &str, attr: &str) -> Option<String> {
    let lower = tag.to_ascii_lowercase();
    let needle = format!("{}=", attr.to_ascii_lowercase());
    let attr_pos = lower.find(&needle)?;
    let raw = &tag[attr_pos + needle.len()..].trim_start();
    if let Some(rest) = raw.strip_prefix('"') {
        let end = rest.find('"')?;
        return Some(rest[..end].to_string());
    }
    if let Some(rest) = raw.strip_prefix('\'') {
        let end = rest.find('\'')?;
        return Some(rest[..end].to_string());
    }

    let end = raw.find(char::is_whitespace).unwrap_or(raw.len());
    Some(raw[..end].to_string())
}

pub fn has_css_class(tag: &str, class_name: &str) -> bool {
    extract_attr(tag, "class")
        .map(|v| {
            v.split_whitespace()
                .any(|part| part.eq_ignore_ascii_case(class_name))
        })
        .unwrap_or(false)
}

pub fn extract_code_language(tag: &str) -> Option<String> {
    let class_attr = extract_attr(tag, "class")?;
    class_attr
        .split_whitespace()
        .find_map(|part| part.strip_prefix("language-"))
        .map(|v| v.to_string())
}

pub fn extract_query_param(url: &str, key: &str) -> Option<String> {
    let query = url.split('?').nth(1)?;
    for part in query.split('&') {
        let mut seg = part.splitn(2, '=');
        let k = seg.next()?.trim();
        let v = seg.next().unwrap_or("").trim();
        if k.eq_ignore_ascii_case(key) {
            return Some(v.to_string());
        }
    }
    None
}

pub fn decode_html_entity(entity: &str) -> Option<&'static str> {
    match entity {
        "&lt;" => Some("<"),
        "&gt;" => Some(">"),
        "&amp;" => Some("&"),
        "&quot;" => Some("\""),
        "&#39;" => Some("'"),
        "&apos;" => Some("'"),
        _ => None,
    }
}

pub fn entity_text_by_utf16_span<'a>(text: &'a str, offset: usize, length: usize) -> Option<&'a str> {
    let (start, end) = utf16_span_to_byte_range(text, offset, length)?;
    text.get(start..end)
}