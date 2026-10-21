//! Files named by URL. Telegram's servers download them; Simula is offline, so the
//! gateway fetches them itself over plain HTTP (an `https://` URL is fetched over HTTP
//! from the same host and port, as webhooks are delivered), which reaches files served
//! locally. Failures read like the server's: "failed to get HTTP URL content".

use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper::Request;
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;

const TIMEOUT: Duration = Duration::from_secs(30);
/// Telegram downloads at most 20 MB from a URL (5 MB for photos).
pub const MAX_URL_FILE_SIZE: usize = 20 << 20;

/// The file at `url` and its name (the URL path's last segment).
pub async fn get(url: &str) -> Result<(String, Bytes), ()> {
    let rest = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .ok_or(())?;
    let default_port = if url.starts_with("https://") { 443 } else { 80 };
    let (authority, path) = match rest.find(['/', '?', '#']) {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let path = path.split('#').next().unwrap_or("/");
    let path = if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    };
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) if !host.ends_with(']') || authority.starts_with('[') => {
            (host.to_owned(), port.parse().map_err(|_| ())?)
        }
        _ => (authority.to_owned(), default_port),
    };
    let exchange = async {
        let stream = TcpStream::connect((host.trim_matches(['[', ']']), port))
            .await
            .map_err(|_| ())?;
        let (mut sender, connection) = http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|_| ())?;
        tokio::spawn(connection);
        let request = Request::get(&path)
            .header("Host", authority)
            .body(Empty::<Bytes>::new())
            .map_err(|_| ())?;
        let response = sender.send_request(request).await.map_err(|_| ())?;
        if !response.status().is_success() {
            return Err(());
        }
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(|_| ())?
            .to_bytes();
        if body.len() > MAX_URL_FILE_SIZE {
            return Err(());
        }
        Ok(body)
    };
    let body = tokio::time::timeout(TIMEOUT, exchange)
        .await
        .map_err(|_| ())??;
    let name = path
        .split('?')
        .next()
        .and_then(|p| p.rsplit('/').next())
        .unwrap_or_default()
        .to_owned();
    Ok((name, body))
}
