//! Webhooks, as telegram-bot-api runs them (`Client::process_set_webhook_query`,
//! `do_set_webhook`, `WebhookActor`). While a webhook is set, updates are POSTed to it
//! and `getUpdates` answers 409.
//!
//! URLs follow telegram-bot-api's **local mode**: plain HTTP, any port and any host
//! (localhost included) are accepted, because bots under test run locally.
//!
//! Simula has no TLS client. `https://` URLs are accepted and delivered over plain HTTP
//! to the same host and port, as if TLS were terminated in front of the bot — which is
//! how frameworks run webhooks locally (Telegraf registers `https://` but serves HTTP).

pub mod client;
mod sender;

use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use simula_botapi_types::types as bot;
use tokio::task::AbortHandle;

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult};
use crate::text::link;

const DEFAULT_MAX_CONNECTIONS: i64 = 40;
const MAX_MAX_CONNECTIONS: i64 = 100;
const MAX_SECRET_TOKEN_LENGTH: usize = 256;

/// A bot's webhook, if set, and the last delivery error.
#[derive(Default)]
pub struct State {
    config: Option<Config>,
    pub last_error: Option<(i32, String)>,
    worker: Option<AbortHandle>,
    /// setWebhook with a URL is accepted at most once per second.
    next_set: Option<Instant>,
}

impl State {
    pub fn is_set(&self) -> bool {
        self.config.is_some()
    }

    /// Stop delivering and forget the webhook.
    fn close(&mut self) -> bool {
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
        self.last_error = None;
        self.config.take().is_some()
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Config {
    url: String,
    max_connections: i64,
    secret_token: String,
    /// The address given with `ip_address`, if any.
    fixed_ip: Option<String>,
    ip_address: String,
}

/// What `setWebhook` asks for; an empty `url` is `deleteWebhook`.
#[derive(Default)]
pub struct Request {
    pub url: String,
    pub ip_address: Option<String>,
    pub max_connections: Option<i64>,
    pub allowed_updates: Option<Vec<String>>,
    pub drop_pending_updates: bool,
    pub secret_token: Option<String>,
}

/// Set or delete the webhook; the result is the response's `description`.
pub async fn set(ctx: &BotCtx, request: Request) -> BotResult<&'static str> {
    let state = ctx.state();
    let setting = !request.url.is_empty();
    if setting {
        let mut webhook = state.webhook.lock().expect("webhook lock");
        let now = Instant::now();
        if webhook.next_set.is_some_and(|next| now < next) {
            let mut error = BotError::new(429, "Too Many Requests: retry after 1");
            error.retry_after = Some(1);
            return Err(error);
        }
        webhook.next_set = Some(now + Duration::from_secs(1));
    }
    let max_connections = request
        .max_connections
        .unwrap_or(DEFAULT_MAX_CONNECTIONS)
        .clamp(1, MAX_MAX_CONNECTIONS);
    let secret_token = request.secret_token.clone().unwrap_or_default();
    let fixed_ip = request.ip_address.clone().filter(|ip| !ip.is_empty());

    // Nothing changes: only allowed_updates are taken.
    {
        let webhook = state.webhook.lock().expect("webhook lock");
        let unchanged = match &webhook.config {
            None => !setting,
            Some(c) => {
                c.url == request.url
                    && c.max_connections == max_connections
                    && c.secret_token == secret_token
                    && c.fixed_ip == fixed_ip
            }
        };
        if unchanged && !request.drop_pending_updates {
            drop(webhook);
            if let Some(allowed) = request.allowed_updates {
                state.queue.lock().expect("queue lock").set_allowed(allowed);
            }
            return Ok(if setting {
                "Webhook is already set"
            } else {
                "Webhook is already deleted"
            });
        }
    }

    // The old webhook goes first, even if the new URL turns out to be invalid.
    let was_set = state.webhook.lock().expect("webhook lock").close();
    state.notify.notify_waiters();
    if request.drop_pending_updates {
        state.queue.lock().expect("queue lock").clear();
    }
    if !setting {
        return Ok(if was_set {
            "Webhook was deleted"
        } else {
            "Webhook is already deleted"
        });
    }

    let with_scheme = if request.url.contains("://") {
        request.url.clone()
    } else {
        format!("https://{}", request.url)
    };
    let url = link::parse_url(&with_scheme)
        .map_err(|_| BotError::bad_request("invalid webhook URL specified"))?;
    if secret_token.len() > MAX_SECRET_TOKEN_LENGTH {
        return Err(BotError::bad_request("secret token is too long"));
    }
    if !secret_token
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(BotError::bad_request(
            "secret token contains illegal characters",
        ));
    }
    // Verification: the host must resolve (or the given IP address be valid).
    let default_port = if url.https { 443 } else { 80 };
    let port = if url.port == 0 {
        default_port
    } else {
        url.port
    };
    let address = match &fixed_ip {
        Some(ip) => ip
            .parse::<IpAddr>()
            .map(|ip| SocketAddr::new(ip, port))
            .map_err(|_| BotError::bad_request("bad webhook: Invalid IP address specified"))?,
        None => resolve(&url.host, port).await.map_err(|reason| {
            BotError::bad_request(format!("bad webhook: Failed to resolve host: {reason}"))
        })?,
    };

    let config = Config {
        url: request.url.clone(),
        max_connections,
        secret_token: secret_token.clone(),
        fixed_ip,
        ip_address: address.ip().to_string(),
    };
    let settings = sender::Settings {
        target: client::Target {
            host: url.host.clone(),
            port,
            path: url.query.clone(),
            userinfo: url.userinfo.clone(),
            address,
        },
        secret_token,
        max_connections: max_connections as usize,
    };
    if let Some(allowed) = request.allowed_updates {
        state.queue.lock().expect("queue lock").set_allowed(allowed);
    }
    let worker = sender::spawn(&ctx.gateway, ctx.bot.clone(), settings);
    let mut webhook = state.webhook.lock().expect("webhook lock");
    webhook.close();
    webhook.config = Some(config);
    webhook.worker = Some(worker);
    Ok("Webhook was set")
}

async fn resolve(host: &str, port: u16) -> Result<SocketAddr, String> {
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let mut addresses = tokio::net::lookup_host((host, port))
        .await
        .map_err(|error| error.to_string())?;
    addresses
        .next()
        .ok_or_else(|| "no addresses found".to_owned())
}

/// `getWebhookInfo`, field by field as telegram-bot-api's `JsonWebhookInfo`.
pub fn info(ctx: &BotCtx) -> bot::WebhookInfo {
    let state = ctx.state();
    let queue = state.queue.lock().expect("queue lock");
    let webhook = state.webhook.lock().expect("webhook lock");
    let config = webhook.config.as_ref();
    bot::WebhookInfo {
        url: config.map(|c| c.url.clone()).unwrap_or_default(),
        has_custom_certificate: false,
        pending_update_count: queue.len() as i64,
        ip_address: config.map(|c| c.ip_address.clone()),
        last_error_date: webhook
            .last_error
            .as_ref()
            .map(|(date, _)| i64::from(*date)),
        last_error_message: webhook
            .last_error
            .as_ref()
            .map(|(_, message)| message.clone()),
        last_synchronization_error_date: None,
        max_connections: config.map(|c| c.max_connections),
        allowed_updates: queue.allowed().map(<[String]>::to_vec),
    }
}
