//! Link normalization for text links, ported from tdlib's `LinkManager::check_link_impl`
//! and `parse_url`: default `http://`, lowercase host, `/` as the minimal path, spaces
//! percent-encoded. Invalid links make the entity disappear (tdlib returns "").

/// `tg://user?id=123` → 123.
pub fn user_id(link: &str) -> Option<i64> {
    tg_params(link, "user")?
        .into_iter()
        .find(|(k, _)| *k == "id")
        .and_then(|(_, v)| strict_int(v))
        .filter(|&id| id > 0)
}

/// `tg://emoji?id=…` → the custom emoji id.
pub fn custom_emoji_id(link: &str) -> Option<i64> {
    tg_params(link, "emoji")?
        .into_iter()
        .find(|(k, _)| *k == "id")
        .and_then(|(_, v)| strict_int(v))
        .filter(|&id| id != 0)
}

/// `tg://time?unix=…&format=…` → (unix time, format letters).
pub fn formatted_date(link: &str) -> Option<(i32, String)> {
    let mut unix = 0;
    let mut format = String::new();
    for (key, value) in tg_params(link, "time")? {
        match key {
            "unix" => {
                unix = strict_int(value)
                    .and_then(|v| i32::try_from(v).ok())
                    .filter(|&v| v > 0)?
            }
            "format" => format = value.to_owned(),
            _ => {}
        }
    }
    (unix != 0).then_some((unix, format))
}

/// Parameters of `tg://<host>[/]?<query>[#fragment]` when the host matches exactly
/// (case-insensitive), like tdlib's `check_tg_url_host`.
fn tg_params<'a>(link: &'a str, host: &str) -> Option<Vec<(&'a str, &'a str)>> {
    let rest = strip_prefix_ignore_case(link, "tg:")?;
    let rest = rest.strip_prefix("//").unwrap_or(rest);
    let rest = rest.split('#').next().unwrap_or("");
    let host_end = rest.find(['/', '?']).unwrap_or(rest.len());
    if !rest[..host_end].eq_ignore_ascii_case(host) {
        return None;
    }
    let query = rest[host_end..].trim_start_matches('/');
    let query = query.strip_prefix('?').unwrap_or(query);
    Some(
        query
            .split('&')
            .filter(|p| !p.is_empty())
            .map(|p| p.split_once('=').unwrap_or((p, "")))
            .collect(),
    )
}

/// tdlib's `to_integer_safe`: optional minus, digits, no leading zeros.
fn strict_int(value: &str) -> Option<i64> {
    let digits = value.strip_prefix('-').unwrap_or(value);
    let valid = !digits.is_empty()
        && digits.bytes().all(|b| b.is_ascii_digit())
        && (digits == "0" || !digits.starts_with('0'));
    valid.then(|| value.parse().ok()).flatten()
}

/// tdlib's `get_checked_link`: the normalized link, or `None` when it is not a valid link.
pub fn checked(link: &str) -> Option<String> {
    check_impl(link, false, false).ok()
}

/// tdlib's `LinkManager::check_link`: the normalized link, or tdlib's error
/// ("URL 'x' is invalid: Wrong HTTP URL"). Buttons prefix it with their own name.
pub fn check(link: &str, http_only: bool, https_only: bool) -> Result<String, String> {
    check_impl(link, http_only, https_only)
        .map_err(|error| format!("URL '{link}' is invalid: {error}"))
}

/// tdlib's `check_link_impl`.
fn check_impl(link: &str, http_only: bool, https_only: bool) -> Result<String, &'static str> {
    for scheme in ["tg", "ton", "tonsite"] {
        let Some(rest) = strip_prefix_ignore_case(link, &format!("{scheme}:")) else {
            continue;
        };
        let rest = rest.strip_prefix("//").unwrap_or(rest);
        let url = parse_url(rest)?;
        if https_only {
            return Err("Only HTTPS links are allowed");
        }
        if http_only {
            return Err("Only HTTP links are allowed");
        }
        if rest.to_ascii_lowercase().starts_with("http://")
            || url.https
            || !url.userinfo.is_empty()
            || url.port != 0
            || url.host.starts_with('[')
        {
            return Err(if scheme == "tg" {
                "Wrong tg URL"
            } else {
                "Wrong ton URL"
            });
        }
        let query = url
            .query
            .strip_prefix("/?")
            .map(|q| format!("?{q}"))
            .unwrap_or(url.query);
        let host_ok = url.host.chars().all(|c| {
            c.is_ascii_alphanumeric() || c == '-' || c == '_' || (scheme == "tonsite" && c == '.')
        });
        if !host_ok {
            return Err("Unallowed characters in URL host");
        }
        return Ok(format!("{scheme}://{}{query}", url.host));
    }
    let url = parse_url(link)?;
    if https_only && !url.https {
        return Err("Only HTTPS links are allowed");
    }
    if !url.host.contains('.') && !url.host.starts_with('[') {
        return Err("Wrong HTTP URL");
    }
    let mut out = String::from(if url.https { "https://" } else { "http://" });
    if !url.userinfo.is_empty() {
        out.push_str(&url.userinfo);
        out.push('@');
    }
    out.push_str(&url.host);
    if url.port != 0 {
        out.push_str(&format!(":{}", url.port));
    }
    out.push_str(&url.query);
    Ok(out)
}

/// tdlib's `HttpUrl`: the parts of an http(s) URL; `query` is the path and query.
pub(crate) struct Url {
    pub https: bool,
    pub userinfo: String,
    pub host: String,
    /// 0 when not given.
    pub port: u16,
    pub query: String,
}

/// tdlib's `parse_url` (plain `host/path` means `http://`).
pub(crate) fn parse_url(url: &str) -> Result<Url, &'static str> {
    let scheme_end = url
        .find(|c: char| ":/?#@[]".contains(c))
        .unwrap_or(url.len());
    let (https, rest) = match url[scheme_end..].strip_prefix("://") {
        Some(rest) => match url[..scheme_end].to_ascii_lowercase().as_str() {
            "http" => (false, rest),
            "https" => (true, rest),
            _ => return Err("Unsupported URL protocol"),
        },
        None => (false, url),
    };
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, query) = rest.split_at(authority_end);

    let colon = authority
        .rfind([':', ']', '@'])
        .filter(|&i| authority.as_bytes()[i] == b':' && i > 0);
    let (userinfo_host, port) = match colon {
        Some(i) => {
            let digits = authority[i + 1..].trim_start_matches('0');
            let port: u32 = if digits.is_empty() {
                0
            } else {
                digits
                    .parse()
                    .map_err(|_| "Wrong port number specified in the URL")?
            };
            if port == 0 || port > 65535 {
                return Err("Wrong port number specified in the URL");
            }
            (&authority[..i], port as u16)
        }
        None => (authority, 0),
    };
    let (userinfo, host) = match userinfo_host.rfind('@') {
        Some(i) => (&userinfo_host[..i], &userinfo_host[i + 1..]),
        None => ("", userinfo_host),
    };
    if host.is_empty() {
        return Err("URL host is empty");
    }
    if host == "." {
        return Err("Host is invalid");
    }
    let host = host.to_ascii_lowercase();
    if !host.starts_with('[') {
        check_part(&host, false)?;
        check_part(userinfo, true)?;
    }

    let query = query.trim_end();
    let mut normalized = String::new();
    if !query.starts_with('/') {
        normalized.push('/');
    }
    for c in query.chars() {
        if (c as u32) <= 0x20 {
            normalized.push_str(&format!("%{:02X}", c as u32));
        } else {
            normalized.push(c);
        }
    }
    Ok(Url {
        https,
        userinfo: userinfo.to_owned(),
        host,
        port,
        query: normalized,
    })
}

/// Characters RFC 3986 allows in a host or userinfo, plus plain UTF-8.
fn check_part(part: &str, allow_colon: bool) -> Result<(), &'static str> {
    let bytes = part.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c.is_ascii_alphanumeric()
            || b".-_!$,~*'();&+=".contains(&c)
            || (allow_colon && c == b':')
            || c >= 128
        {
            i += 1;
        } else if c == b'%'
            && bytes.get(i + 1).is_some_and(u8::is_ascii_hexdigit)
            && bytes.get(i + 2).is_some_and(u8::is_ascii_hexdigit)
        {
            i += 3;
        } else {
            return Err("Disallowed character in URL");
        }
    }
    Ok(())
}

fn strip_prefix_ignore_case<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    (s.len() >= prefix.len() && s[..prefix.len()].eq_ignore_ascii_case(prefix))
        .then(|| &s[prefix.len()..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_like_tdlib() {
        assert_eq!(
            checked("https://example.com").as_deref(),
            Some("https://example.com/")
        );
        assert_eq!(
            checked("Example.COM/Path?q=1").as_deref(),
            Some("http://example.com/Path?q=1")
        );
        assert_eq!(
            checked("http://a.b:8080/x y").as_deref(),
            Some("http://a.b:8080/x%20y")
        );
        assert_eq!(
            checked("tg://resolve?domain=durov").as_deref(),
            Some("tg://resolve?domain=durov")
        );
        assert_eq!(
            checked("localhost"),
            None,
            "a host without a dot is not an HTTP link"
        );
        assert_eq!(checked("ftp://example.com"), None);
        assert_eq!(checked("http://example.com:99999"), None);
    }

    #[test]
    fn check_reports_tdlib_errors() {
        assert_eq!(
            check("example", false, false).unwrap_err(),
            "URL 'example' is invalid: Wrong HTTP URL"
        );
        assert_eq!(
            check("http://example.com", true, true).unwrap_err(),
            "URL 'http://example.com' is invalid: Only HTTPS links are allowed"
        );
        assert_eq!(
            check("tg://resolve?domain=x", true, false).unwrap_err(),
            "URL 'tg://resolve?domain=x' is invalid: Only HTTP links are allowed"
        );
        assert_eq!(
            check("ftp://example.com", false, false).unwrap_err(),
            "URL 'ftp://example.com' is invalid: Unsupported URL protocol"
        );
        assert_eq!(
            check("https://Example.com/app", true, true).as_deref(),
            Ok("https://example.com/app")
        );
    }

    #[test]
    fn user_links() {
        assert_eq!(user_id("tg://user?id=123456"), Some(123456));
        assert_eq!(user_id("TG:user?id=5"), Some(5));
        assert_eq!(user_id("tg://user?id=abc"), None);
    }
}
