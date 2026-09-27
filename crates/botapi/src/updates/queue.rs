//! One bot's pending updates, with `getUpdates` semantics.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;

use simula_botapi_types::types as bot;
use tokio::sync::Notify;

/// Telegram keeps unconfirmed updates for 24 hours.
const RETENTION_MS: i64 = 24 * 60 * 60 * 1000;

/// Kinds a bot doesn't get unless it asks for them in `allowed_updates`.
const OPT_IN_KINDS: &[&str] = &["chat_member", "message_reaction", "message_reaction_count"];

pub struct UpdateQueue {
    next_id: i64,
    items: VecDeque<(i64, bot::Update)>,
    /// Last `allowed_updates` the bot sent; `None` or empty means the default set.
    allowed: Option<Vec<String>>,
}

impl Default for UpdateQueue {
    fn default() -> Self {
        // A fresh bot's first update_id is an arbitrary large number, not 1.
        Self {
            next_id: rand::random_range(100_000_000..900_000_000),
            items: VecDeque::new(),
            allowed: None,
        }
    }
}

impl UpdateQueue {
    /// Queue an update if the bot accepts its kind. Returns whether it was queued.
    pub fn push(&mut self, mut update: bot::Update, now_ms: i64) -> bool {
        if !self.accepts(update.kind().unwrap_or_default()) {
            return false;
        }
        update.update_id = self.next_id;
        self.next_id += 1;
        self.items.push_back((now_ms, update));
        while self
            .items
            .front()
            .is_some_and(|(at, _)| now_ms - at > RETENTION_MS)
        {
            self.items.pop_front();
        }
        true
    }

    /// Forget confirmed updates: ids below `offset`, or all but the last `-offset`.
    pub fn confirm(&mut self, offset: i64) {
        if offset < 0 {
            let keep = offset.unsigned_abs() as usize;
            while self.items.len() > keep {
                self.items.pop_front();
            }
        } else {
            while self
                .items
                .front()
                .is_some_and(|(_, u)| u.update_id < offset)
            {
                self.items.pop_front();
            }
        }
    }

    pub fn peek(&self, limit: usize) -> Vec<bot::Update> {
        self.items
            .iter()
            .take(limit)
            .map(|(_, u)| u.clone())
            .collect()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn set_allowed(&mut self, allowed: Vec<String>) {
        self.allowed = Some(allowed);
    }

    fn accepts(&self, kind: &str) -> bool {
        match &self.allowed {
            Some(list) if !list.is_empty() => list.iter().any(|k| k == kind),
            _ => !OPT_IN_KINDS.contains(&kind),
        }
    }
}

/// Per-bot delivery state shared by the feeder and `getUpdates`.
#[derive(Default)]
pub struct BotState {
    pub queue: Mutex<UpdateQueue>,
    /// Woken when updates arrive or a newer `getUpdates` starts.
    pub notify: Notify,
    /// Increments per `getUpdates`; an older poll that sees a newer value yields (409).
    pub polls: AtomicU64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message() -> bot::Update {
        bot::Update {
            message: Some(Box::default()),
            ..Default::default()
        }
    }

    #[test]
    fn ids_increase_and_offsets_confirm() {
        let mut queue = UpdateQueue::default();
        for _ in 0..3 {
            queue.push(message(), 0);
        }
        let ids: Vec<i64> = queue.peek(10).iter().map(|u| u.update_id).collect();
        assert_eq!(ids[1], ids[0] + 1);
        queue.confirm(ids[1]);
        assert_eq!(queue.peek(10).len(), 2);
        queue.confirm(-1);
        assert_eq!(queue.peek(10)[0].update_id, ids[2]);
    }

    #[test]
    fn allowed_updates_filter_and_default_opt_ins() {
        let mut queue = UpdateQueue::default();
        let reaction = bot::Update {
            message_reaction: Some(Box::default()),
            ..Default::default()
        };
        assert!(!queue.push(reaction.clone(), 0), "reactions are opt-in");
        queue.set_allowed(vec!["message_reaction".into()]);
        assert!(queue.push(reaction, 0));
        assert!(!queue.push(message(), 0), "not in the allowed list");
    }

    #[test]
    fn old_updates_expire() {
        let mut queue = UpdateQueue::default();
        queue.push(message(), 0);
        queue.push(message(), RETENTION_MS + 1);
        assert_eq!(queue.len(), 1);
    }
}
