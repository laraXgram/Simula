//! Delivery to a set webhook, after telegram-bot-api's `WebhookActor`:
//! - updates of one queue (a chat, a user's callback queries) go out in order, one at a
//!   time; different queues in parallel, up to `max_connections`;
//! - a failed update is retried at once, then after 2, 4, 8… seconds, capped at a random
//!   60–120 s; `Retry-After` is honoured (up to an hour); an update whose next attempt
//!   would fall after its expiry is dropped;
//! - a 2xx answer may carry a method call (`{"method": "sendMessage", …}`), which runs as
//!   the bot, except `getX`, `setWebhook`, `deleteWebhook`, `close` and `logOut`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::Duration;

use serde_json::Value;
use simula_botapi_types::types as bot;
use simula_core::Account;
use tokio::task::{AbortHandle, JoinSet};
use tokio::time::Instant;

use super::client;
use crate::ctx::BotCtx;
use crate::state::Gateway;

const MAX_RETRY_AFTER: u64 = 3600;
const MAX_RESEND_DELAY: u64 = 60;
/// Updates are kept (and retried) for 24 hours.
const UPDATE_LIFETIME_MS: i64 = 24 * 60 * 60 * 1000;

pub struct Settings {
    pub target: client::Target,
    pub secret_token: String,
    pub max_connections: usize,
}

pub fn spawn(gateway: &Arc<Gateway>, bot: Account, settings: Settings) -> AbortHandle {
    let gateway = Arc::downgrade(gateway);
    tokio::spawn(run(gateway, bot, settings)).abort_handle()
}

#[derive(Default)]
struct Retry {
    fails: u32,
    /// Seconds; doubles after every failure but the first.
    delay: u64,
    wake_at: Option<Instant>,
}

async fn run(gateway: Weak<Gateway>, bot: Account, settings: Settings) {
    let mut in_flight: JoinSet<(i64, Result<client::Response, String>)> = JoinSet::new();
    let mut sending: HashMap<i64, i64> = HashMap::new(); // update_id → queue
    let mut retries: HashMap<i64, Retry> = HashMap::new();
    loop {
        let Some(gw) = gateway.upgrade() else {
            return;
        };
        let state = gw.bot_state(bot.id);
        let notified = state.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        // The first pending update of each queue is the only one that may go out.
        let now = Instant::now();
        let pending = state.queue.lock().expect("queue lock").pending();
        let mut busy: HashSet<i64> = sending.values().copied().collect();
        let mut next_wake: Option<Instant> = None;
        for (_, update) in pending {
            let queue = queue_of(&update);
            if !busy.insert(queue) {
                continue;
            }
            if let Some(wake_at) = retries.get(&update.update_id).and_then(|r| r.wake_at)
                && wake_at > now
            {
                next_wake = Some(next_wake.map_or(wake_at, |w| w.min(wake_at)));
                continue;
            }
            if sending.len() >= settings.max_connections {
                break;
            }
            sending.insert(update.update_id, queue);
            let body = serde_json::to_vec(&update).unwrap_or_default();
            let target = settings.target.clone();
            let secret = settings.secret_token.clone();
            let id = update.update_id;
            in_flight.spawn(async move { (id, client::post(&target, &secret, body).await) });
        }
        // Only the bot's state is held while waiting, not the gateway.
        drop(gw);

        let sleep = async {
            match next_wake {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            Some(done) = in_flight.join_next() => {
                let Ok((id, result)) = done else { continue };
                sending.remove(&id);
                if let Some(gw) = gateway.upgrade() {
                    finish(&gw, &bot, id, result, &mut retries);
                }
            }
            () = &mut notified => {}
            () = sleep => {}
        }
    }
}

fn finish(
    gateway: &Arc<Gateway>,
    bot: &Account,
    id: i64,
    result: Result<client::Response, String>,
    retries: &mut HashMap<i64, Retry>,
) {
    let state = gateway.bot_state(bot.id);
    let (error, retry_after) = match result {
        Ok(response) if (200..300).contains(&response.status) => {
            retries.remove(&id);
            state.queue.lock().expect("queue lock").remove(id);
            reply_in_response(gateway, bot, &response.body);
            return;
        }
        Ok(response) => (
            format!(
                "Wrong response from the webhook: {} {}",
                response.status, response.reason
            ),
            response.retry_after.unwrap_or(0).max(0) as u64,
        ),
        Err(error) => (error, 0),
    };
    let now_s = gateway.sandbox.clock().now();
    state.webhook.lock().expect("webhook lock").last_error = Some((now_s, error));

    let retry = retries.entry(id).or_insert(Retry {
        delay: 1,
        ..Retry::default()
    });
    let retry_after = retry_after.min(MAX_RETRY_AFTER);
    let mut effective = retry_after;
    if retry_after == 0 && retry.fails > 0 {
        let cap = rand::random_range(MAX_RESEND_DELAY..=MAX_RESEND_DELAY * 2);
        retry.delay = (retry.delay * 2).min(cap);
        effective = retry.delay;
    }
    let queued_at = state
        .queue
        .lock()
        .expect("queue lock")
        .pending()
        .into_iter()
        .find(|(_, u)| u.update_id == id)
        .map(|(at, _)| at);
    let now_ms = gateway.sandbox.clock().now_ms();
    if queued_at.is_none_or(|at| now_ms + (effective as i64) * 1000 > at + UPDATE_LIFETIME_MS) {
        retries.remove(&id);
        state.queue.lock().expect("queue lock").remove(id);
        return;
    }
    retry.fails += 1;
    retry.wake_at = Some(Instant::now() + Duration::from_secs(effective));
}

/// Run the method call a webhook answered with, as the bot. Its result goes nowhere.
fn reply_in_response(gateway: &Arc<Gateway>, bot: &Account, body: &[u8]) {
    let Ok(Value::Object(params)) = serde_json::from_slice::<Value>(body) else {
        return;
    };
    let Some(method) = params
        .get("method")
        .and_then(Value::as_str)
        .map(str::to_owned)
    else {
        return;
    };
    let lower = method.to_ascii_lowercase();
    if lower.is_empty()
        || lower.starts_with("get")
        || ["deletewebhook", "setwebhook", "close", "logout"].contains(&lower.as_str())
    {
        return;
    }
    let ctx = BotCtx::new(gateway.clone(), bot.clone());
    tokio::spawn(async move {
        if let Err(error) = crate::http::execute_json(ctx, &method, params).await {
            tracing::debug!(%method, description = %error.description, "method in webhook response failed");
        }
    });
}

/// The queue an update waits in: its chat for messages, the sender for callback queries
/// (telegram-bot-api's webhook queue ids); anything else goes alone.
fn queue_of(update: &bot::Update) -> i64 {
    let chat = [&update.message, &update.edited_message]
        .into_iter()
        .flatten()
        .map(|m| m.chat.id)
        .next();
    if let Some(chat) = chat {
        return chat;
    }
    if let Some(query) = &update.callback_query {
        return query.from.id + (3 << 33);
    }
    i64::MIN + update.update_id
}
