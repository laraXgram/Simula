//! Server-sent event streams: everything that happens, or one account's updates.

use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use simula_core::{Sandbox, UserId};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};

/// `GET /events` — RPC calls, update deliveries, account changes.
pub async fn all(
    State(sandbox): State<Arc<Sandbox>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = BroadcastStream::new(sandbox.subscribe_events()).filter_map(|event| {
        let event = event.ok()?;
        Some(Ok(Event::default().json_data(&*event).unwrap_or_default()))
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// `GET /accounts/{id}/updates` — the JSON-TL `Updates` this account receives.
pub async fn account_updates(
    State(sandbox): State<Arc<Sandbox>>,
    Path(id): Path<i64>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let account = UserId(id);
    let stream = BroadcastStream::new(sandbox.subscribe_updates()).filter_map(move |batch| {
        let batch = batch.ok()?;
        if batch.account != account {
            return None;
        }
        Some(Ok(Event::default()
            .json_data(&batch.updates)
            .unwrap_or_default()))
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}
