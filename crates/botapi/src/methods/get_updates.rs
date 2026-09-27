//! `getUpdates`: long polling over the bot's queue.

use std::sync::atomic::Ordering;
use std::time::Duration;

use simula_botapi_types::{methods, types as bot};
use tokio::time::Instant;

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult};

/// telegram-bot-api clamps rather than rejects these.
const MAX_LIMIT: i64 = 100;
const MAX_TIMEOUT_SECS: i64 = 50;

pub async fn handle(ctx: BotCtx, params: methods::GetUpdates) -> BotResult<Vec<bot::Update>> {
    let state = ctx.state();
    let limit = params.limit.unwrap_or(MAX_LIMIT).clamp(1, MAX_LIMIT) as usize;
    let timeout = Duration::from_secs(params.timeout.unwrap_or(0).clamp(0, MAX_TIMEOUT_SECS) as u64);

    // A newer poll takes over; the older one returns 409.
    let poll = state.polls.fetch_add(1, Ordering::SeqCst) + 1;
    state.notify.notify_waiters();
    {
        let mut queue = state.queue.lock().expect("queue lock");
        if let Some(offset) = params.offset {
            queue.confirm(offset);
        }
        if let Some(allowed) = params.allowed_updates {
            queue.set_allowed(allowed);
        }
    }

    let deadline = Instant::now() + timeout;
    loop {
        let notified = state.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        if state.polls.load(Ordering::SeqCst) != poll {
            return Err(BotError::conflict(
                "terminated by other getUpdates request; make sure that only one bot instance is running",
            ));
        }
        let batch = state.queue.lock().expect("queue lock").peek(limit);
        if !batch.is_empty() || Instant::now() >= deadline {
            return Ok(batch);
        }
        tokio::select! {
            () = &mut notified => {}
            () = tokio::time::sleep_until(deadline) => {}
        }
    }
}
