//! Calls answered later by another account: a user's `messages.getBotCallbackAnswer`
//! waits for the bot's `messages.setBotCallbackAnswer` (inline and pre-checkout queries
//! will too). The asking handler registers the wait inside its transaction, so it exists
//! before the bot can see the query; the sandbox awaits it after commit, off the sandbox
//! thread, so the answering call can run; answers are handed over only after the
//! answering transaction commits.

use std::collections::HashMap;
use std::sync::Mutex;

use tokio::sync::oneshot;

use crate::error::{RpcError, RpcResult};
use crate::ids::UserId;
use crate::rpc::Reply;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WaitKey {
    /// A callback query only the bot it was sent to can answer.
    CallbackQuery { bot: UserId, query_id: i64 },
}

pub type Answer = RpcResult<Box<dyn Reply>>;

/// Open waits of a sandbox, shared by its thread and the calls awaiting answers.
#[derive(Default)]
pub struct Waits {
    pending: Mutex<HashMap<WaitKey, oneshot::Sender<Answer>>>,
}

impl Waits {
    pub(crate) fn register(&self, key: WaitKey) -> oneshot::Receiver<Answer> {
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().expect("waits lock").insert(key, sender);
        receiver
    }

    pub fn is_pending(&self, key: WaitKey) -> bool {
        self.pending.lock().expect("waits lock").contains_key(&key)
    }

    pub(crate) fn resolve(&self, key: WaitKey, answer: Answer) {
        if let Some(sender) = self.pending.lock().expect("waits lock").remove(&key) {
            let _ = sender.send(answer);
        }
    }

    pub(crate) fn cancel(&self, key: WaitKey) {
        self.pending.lock().expect("waits lock").remove(&key);
    }
}

/// A call's result that another account will provide.
pub struct Deferred {
    pub key: WaitKey,
    pub(crate) receiver: oneshot::Receiver<Answer>,
    /// What the caller gets if nobody answers in time.
    pub timeout_error: RpcError,
}
