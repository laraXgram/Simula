//! Updates produced during one transaction, grouped per recipient. They are rendered
//! for each recipient (users with that recipient's access hashes) before commit and
//! published after it.

use std::collections::{BTreeMap, BTreeSet};

use simula_tl::{enums, types};

use crate::ctx::{Ctx, SessionId};
use crate::error::RpcResult;
use crate::ids::UserId;
use crate::render;

/// Updates for one account, ready to deliver.
#[derive(Debug, Clone)]
pub struct AccountUpdates {
    pub account: UserId,
    /// The session whose call caused these updates; it already got them as the call's
    /// result, so it should skip them. Other sessions of the account receive them.
    pub origin: Option<SessionId>,
    pub updates: enums::Updates,
}

#[derive(Debug, Default)]
pub struct Outbox {
    pending: BTreeMap<UserId, Pending>,
}

#[derive(Debug, Default)]
struct Pending {
    updates: Vec<enums::Update>,
    users: BTreeSet<UserId>,
    origin: Option<SessionId>,
}

impl Outbox {
    /// Queue `update` for `account`, noting which users it mentions.
    pub fn push(
        &mut self,
        account: UserId,
        update: enums::Update,
        users: impl IntoIterator<Item = UserId>,
        origin: Option<SessionId>,
    ) {
        let pending = self.pending.entry(account).or_default();
        pending.updates.push(update);
        pending.users.extend(users);
        pending.origin = pending.origin.or(origin);
    }

    /// Users mentioned so far in `account`'s updates.
    pub fn users_for(&self, account: UserId) -> BTreeSet<UserId> {
        self.pending.get(&account).map(|p| p.users.clone()).unwrap_or_default()
    }

    pub fn render(self, ctx: &Ctx<'_>) -> RpcResult<Vec<AccountUpdates>> {
        let date = ctx.now();
        self.pending
            .into_iter()
            .map(|(account, pending)| {
                let users = render::users(ctx, account, &pending.users)?;
                Ok(AccountUpdates {
                    account,
                    origin: pending.origin,
                    updates: types::Updates { updates: pending.updates, users, chats: vec![], date, seq: 0 }
                        .into(),
                })
            })
            .collect()
    }
}
