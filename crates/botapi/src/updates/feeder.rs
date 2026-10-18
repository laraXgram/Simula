//! Delivery of the core's update batches to bot queues. The sandbox awaits this before
//! the call that produced the batch returns, so bots can already fetch its updates.

use std::sync::{Arc, Weak};

use simula_core::AccountUpdates;

use crate::convert;
use crate::present;
use crate::state::Gateway;

pub fn attach(gateway: &Arc<Gateway>) {
    let weak: Weak<Gateway> = Arc::downgrade(gateway);
    gateway.sandbox.add_listener(Arc::new(move |batch| {
        let weak = weak.clone();
        Box::pin(async move {
            if let Some(gateway) = weak.upgrade() {
                deliver(&gateway, &batch).await;
            }
        })
    }));
}

async fn deliver(gateway: &Gateway, batch: &AccountUpdates) {
    // The gateway's own calls already returned their results.
    if !batch.is_bot || batch.origin == Some(gateway.session) {
        return;
    }
    let simula_tl::enums::Updates::Updates(updates) = &batch.updates else {
        return;
    };
    let wanted = convert::update::wanted(&batch.updates);
    let context = present::context(
        gateway,
        batch.account,
        wanted,
        &updates.users,
        &updates.chats,
    )
    .await;
    let converted = convert::update::updates(&batch.updates, &context.entities(), batch.account.0);

    let state = gateway.bot_state(batch.account);
    let now = gateway.sandbox.clock().now_ms();
    let mut queued = false;
    {
        let mut queue = state.queue.lock().expect("queue lock");
        for update in converted {
            queued |= queue.push(update, now);
        }
    }
    if queued {
        state.notify.notify_waiters();
    }
}
