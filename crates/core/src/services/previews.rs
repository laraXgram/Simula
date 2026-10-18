//! Link previews. Telegram fetches the first link of a message and attaches a
//! `messageMediaWebPage`. Simula is offline, so it attaches a preview built from the URL
//! alone — the address, a display URL and the host as site name, no title or image.
//! Clients show it as a preview card; bots see what they see when a preview exists.
//! A message sent with `no_webpage` gets none.

use sha2::{Digest, Sha256};
use simula_tl::{enums, types};

/// The preview for a message's text, if it has a link that would get one.
pub fn for_text(text: &str, entities: &[enums::MessageEntity]) -> Option<enums::MessageMedia> {
    let url = first_link(text, entities)?;
    let with_scheme = if url.contains("://") {
        url.clone()
    } else {
        format!("http://{url}")
    };
    let rest = with_scheme.split_once("://").map_or("", |(_, rest)| rest);
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host = authority
        .rsplit('@')
        .next()
        .unwrap_or("")
        .split(':')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    if host.is_empty() {
        return None;
    }
    let display_url = format!(
        "{}{}",
        host.trim_start_matches("www."),
        rest[authority.len()..].trim_end_matches('/')
    );
    let webpage = types::WebPage {
        id: page_id(&with_scheme),
        url: with_scheme,
        display_url,
        r#type: Some("article".into()),
        site_name: Some(host.trim_start_matches("www.").to_owned()),
        ..Default::default()
    };
    Some(
        types::MessageMediaWebPage {
            webpage: webpage.into(),
            ..Default::default()
        }
        .into(),
    )
}

/// The first link a preview could be made for, like the server's choice: explicit text
/// links and visible URLs, skipping `tg:`, `ton:`, `tonsite:` and `ftp:` links.
fn first_link(text: &str, entities: &[enums::MessageEntity]) -> Option<String> {
    let skipped = |url: &str| {
        let lower = url.to_ascii_lowercase();
        ["tg:", "ton:", "tonsite:", "ftp:"]
            .iter()
            .any(|scheme| lower.starts_with(scheme))
    };
    let mut sorted: Vec<&enums::MessageEntity> = entities.iter().collect();
    sorted.sort_by_key(|e| e.offset());
    sorted.into_iter().find_map(|entity| {
        let url = match entity {
            enums::MessageEntity::Url(e) => utf16_slice(text, e.offset, e.length),
            enums::MessageEntity::TextUrl(e) => e.url.clone(),
            _ => return None,
        };
        (!url.is_empty() && !skipped(&url)).then_some(url)
    })
}

fn utf16_slice(text: &str, offset: i32, length: i32) -> String {
    let units: Vec<u16> = text.encode_utf16().collect();
    let start = (offset.max(0) as usize).min(units.len());
    let end = (start + length.max(0) as usize).min(units.len());
    String::from_utf16_lossy(&units[start..end])
}

/// Telegram's web page ids are stable per page.
fn page_id(url: &str) -> i64 {
    let digest = Sha256::digest(url.as_bytes());
    i64::from_le_bytes(digest[..8].try_into().expect("sha256 is 32 bytes")) & i64::MAX
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(text: &str, entities: &[enums::MessageEntity]) -> Option<types::WebPage> {
        match for_text(text, entities)? {
            enums::MessageMedia::WebPage(media) => match media.webpage {
                enums::WebPage::WebPage(page) => Some(*page),
                _ => None,
            },
            _ => None,
        }
    }

    #[test]
    fn previews_come_from_the_first_link() {
        let url: enums::MessageEntity = types::MessageEntityUrl {
            offset: 4,
            length: 26,
        }
        .into();
        let found = page("see www.Example.com/docs/page/ now", &[url]).unwrap();
        assert_eq!(found.url, "http://www.Example.com/docs/page/");
        assert_eq!(found.display_url, "example.com/docs/page");
        assert_eq!(found.site_name.as_deref(), Some("example.com"));
        assert_eq!(found.id, page_id("http://www.Example.com/docs/page/"));

        let tg: enums::MessageEntity = types::MessageEntityTextUrl {
            offset: 0,
            length: 1,
            url: "tg://resolve?domain=x".into(),
        }
        .into();
        assert!(page("x", &[tg]).is_none());
        assert!(page("no links", &[]).is_none());
    }
}
