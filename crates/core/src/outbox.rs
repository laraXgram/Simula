//! Updates produced during one transaction, grouped per recipient. They are rendered
//! for each recipient (users with that recipient's access hashes) before commit and
//! published after it.

use std::collections::{BTreeMap, BTreeSet};

use simula_tl::{enums, types};

use crate::ctx::{Ctx, SessionId};
use crate::error::RpcResult;
use crate::ids::{ChannelId, ChatId, UserId};
use crate::render;

/// Updates for one account, ready to deliver.
#[derive(Debug, Clone)]
pub struct AccountUpdates {
    pub account: UserId,
    /// Whether the recipient is a bot (the Bot API gateway delivers these).
    pub is_bot: bool,
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
    /// Each update with the session that caused it, if it is that session's own result.
    updates: Vec<(enums::Update, Option<SessionId>)>,
    users: BTreeSet<UserId>,
    chats: BTreeSet<ChatId>,
    channels: BTreeSet<ChannelId>,
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
        pending.updates.push((update, origin));
        pending.users.extend(users);
    }

    /// Note that `account`'s updates mention basic group `chat` (it gets the chat object).
    pub fn mention_chat(&mut self, account: UserId, chat: ChatId) {
        self.pending.entry(account).or_default().chats.insert(chat);
    }

    /// Note that `account`'s updates mention `channel` (it gets the channel object).
    pub fn mention_channel(&mut self, account: UserId, channel: ChannelId) {
        self.pending
            .entry(account)
            .or_default()
            .channels
            .insert(channel);
    }

    /// Supergroups and channels mentioned so far in `account`'s updates.
    pub fn channels_for(&self, account: UserId) -> BTreeSet<ChannelId> {
        self.pending
            .get(&account)
            .map(|p| p.channels.clone())
            .unwrap_or_default()
    }

    /// Basic groups mentioned so far in `account`'s updates.
    pub fn chats_for(&self, account: UserId) -> BTreeSet<ChatId> {
        self.pending
            .get(&account)
            .map(|p| p.chats.clone())
            .unwrap_or_default()
    }

    /// The updates queued so far for `account`, for a call that returns them as its result.
    pub fn updates_for(&self, account: UserId) -> Vec<enums::Update> {
        self.pending
            .get(&account)
            .map(|p| p.updates.iter().map(|(update, _)| update.clone()).collect())
            .unwrap_or_default()
    }

    /// Users mentioned so far in `account`'s updates.
    pub fn users_for(&self, account: UserId) -> BTreeSet<UserId> {
        self.pending
            .get(&account)
            .map(|p| p.users.clone())
            .unwrap_or_default()
    }

    /// One batch per account and origin, in the order the updates happened: updates a
    /// session caused for itself are kept apart from those all sessions should get.
    pub fn render(self, ctx: &Ctx<'_>) -> RpcResult<Vec<AccountUpdates>> {
        let date = ctx.now();
        let mut batches = Vec::new();
        for (account, pending) in self.pending {
            let users = render::users(ctx, account, &pending.users)?;
            let mut chats = render::chats(ctx, account, &pending.chats)?;
            chats.extend(render::channels(ctx, account, &pending.channels)?);
            let is_bot = ctx.account(account)?.is_bot;
            let mut groups: Vec<(Option<SessionId>, Vec<enums::Update>)> = Vec::new();
            for (update, origin) in pending.updates {
                match groups.last_mut() {
                    Some((last, list)) if *last == origin => list.push(update),
                    _ => groups.push((origin, vec![update])),
                }
            }
            for (origin, updates) in groups {
                batches.push(AccountUpdates {
                    account,
                    is_bot,
                    origin,
                    updates: types::Updates {
                        updates,
                        users: users.clone(),
                        chats: chats.clone(),
                        date,
                        seq: 0,
                    }
                    .into(),
                });
            }
        }
        Ok(batches)
    }
}
