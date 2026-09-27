//! Delivery of the core's update batches to bot queues. Runs synchronously inside the
//! sandbox's publish step, so when a call returns, bots can already fetch its updates.

use std::sync::{Arc, Weak};

use simula_core::AccountUpdates;

use crate::convert;
use crate::state::Gateway;

pub fn attach(gateway: &Arc<Gateway>) {
    let weak: Weak<Gateway> = Arc::downgrade(gateway);
    gateway.sandbox.add_listener(Arc::new(move |batch| {
        if let Some(gateway) = weak.upgrade() {
            deliver(&gateway, batch);
        }
    }));
}

fn deliver(gateway: &Gateway, batch: &AccountUpdates) {
    // The gateway's own calls already returned their results.
    if !batch.is_bot || batch.origin == Some(gateway.session) {
        return;
    }
    let state = gateway.bot_state(batch.account);
    let now = gateway.sandbox.clock().now_ms();
    let mut queued = false;
    {
        let mut queue = state.queue.lock().expect("queue lock");
        for update in convert::update::updates(&batch.updates) {
            queued |= queue.push(update, now);
        }
    }
    if queued {
        state.notify.notify_waiters();
    }
}
