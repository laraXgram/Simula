//! A minimal HTTP/1.1 client for webhook deliveries: one POST per connection, with
//! telegram-bot-api's failure texts ("Connection refused", "Read timeout expired", …).

use std::net::SocketAddr;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::Request;
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// telegram-bot-api's outbound webhook connections time out after 60 seconds.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Where deliveries go: the URL's parts plus the address it resolved to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub host: String,
    pub port: u16,
    /// Path and query, starting with `/`.
    pub path: String,
    pub userinfo: String,
    pub address: SocketAddr,
}

pub struct Response {
    pub status: u16,
    pub reason: String,
    pub body: Bytes,
    /// `Retry-After`, in seconds.
    pub retry_after: Option<i32>,
}

/// POST `body` as JSON. Errors are what telegram-bot-api reports as `last_error_message`.
pub async fn post(target: &Target, secret_token: &str, body: Vec<u8>) -> Result<Response, String> {
    let stream =
        match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(target.address)).await {
            Err(_) => return Err("Connection timed out".into()),
            Ok(Err(error)) => return Err(connect_error(&error)),
            Ok(Ok(stream)) => stream,
        };
    let (mut sender, connection) = http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|_| "Webhook connection closed".to_owned())?;
    tokio::spawn(connection);

    let host = if target.port == 80 {
        target.host.clone()
    } else {
        format!("{}:{}", target.host, target.port)
    };
    let mut request = Request::post(&target.path)
        .header("Host", host)
        .header("Content-Type", "application/json")
        .header("Content-Length", body.len());
    if !target.userinfo.is_empty() {
        let credentials = STANDARD.encode(target.userinfo.as_bytes());
        request = request.header("Authorization", format!("Basic {credentials}"));
    }
    if !secret_token.is_empty() {
        request = request.header("X-Telegram-Bot-Api-Secret-Token", secret_token);
    }
    let request = request
        .body(Full::new(Bytes::from(body)))
        .map_err(|_| "URL is too long".to_owned())?;

    let exchange = async {
        let response = sender
            .send_request(request)
            .await
            .map_err(|_| "Webhook connection closed".to_owned())?;
        let status = response.status();
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse().ok());
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(|_| "Webhook connection closed".to_owned())?
            .to_bytes();
        Ok(Response {
            status: status.as_u16(),
            reason: status.canonical_reason().unwrap_or("").to_owned(),
            body,
            retry_after,
        })
    };
    tokio::time::timeout(READ_TIMEOUT, exchange)
        .await
        .map_err(|_| "Read timeout expired".to_owned())?
}

fn connect_error(error: &std::io::Error) -> String {
    match error.kind() {
        std::io::ErrorKind::ConnectionRefused => "Connection refused".into(),
        std::io::ErrorKind::ConnectionReset => "Connection reset by peer".into(),
        std::io::ErrorKind::TimedOut => "Connection timed out".into(),
        _ => format!("Can't connect to the webhook: {error}"),
    }
}
