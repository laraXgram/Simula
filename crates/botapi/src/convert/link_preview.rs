//! `link_preview_options` of a received text message, as tdlib derives it. Simula never
//! builds link previews (it is offline), which is what Telegram shows when a preview
//! can't be fetched: a text with a link and no preview reports `is_disabled`.

use simula_botapi_types::types as bot;
use simula_tl::{enums, types};

pub fn options(m: &types::Message) -> Option<bot::LinkPreviewOptions> {
    let has_preview = matches!(m.media, Some(enums::MessageMedia::WebPage(_)));
    let disabled = !has_preview && first_url(&m.message, m.entities.as_deref()).is_some();
    (disabled || m.invert_media).then(|| bot::LinkPreviewOptions {
        is_disabled: disabled.then_some(true),
        show_above_text: m.invert_media.then_some(true),
        ..Default::default()
    })
}

/// tdlib's `get_first_url`: the first link that could have a preview.
fn first_url(text: &str, entities: Option<&[enums::MessageEntity]>) -> Option<String> {
    let skipped = |url: &str| {
        let scheme: String = url.chars().take(8).collect::<String>().to_lowercase();
        scheme == "ton:" || scheme.starts_with("tg:") || scheme == "ftp:" || scheme == "tonsite:"
    };
    for entity in entities.unwrap_or_default() {
        match entity {
            enums::MessageEntity::Url(e) if e.length > 4 => {
                let url = utf16_slice(text, e.offset, e.length);
                let plain_domain = !url.contains(['/', '?', '#']);
                if !skipped(&url) && !plain_domain {
                    return Some(url);
                }
            }
            enums::MessageEntity::TextUrl(e) if !skipped(&e.url) => return Some(e.url.clone()),
            _ => {}
        }
    }
    None
}

fn utf16_slice(text: &str, offset: i32, length: i32) -> String {
    let units: Vec<u16> = text.encode_utf16().collect();
    let start = (offset.max(0) as usize).min(units.len());
    let end = (start + length.max(0) as usize).min(units.len());
    String::from_utf16_lossy(&units[start..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(offset: i32, length: i32) -> enums::MessageEntity {
        types::MessageEntityUrl { offset, length }.into()
    }

    #[test]
    fn only_links_that_could_have_a_preview_count() {
        let text = "see example.com/page";
        assert_eq!(
            first_url(text, Some(&[url(4, 16)])).as_deref(),
            Some("example.com/page")
        );
        // A bare domain never gets a preview.
        assert_eq!(first_url("see example.com", Some(&[url(4, 11)])), None);
        let tg: enums::MessageEntity = types::MessageEntityTextUrl {
            offset: 0,
            length: 1,
            url: "tg://resolve?domain=x".into(),
        }
        .into();
        assert_eq!(first_url("x", Some(&[tg])), None);
    }
}
