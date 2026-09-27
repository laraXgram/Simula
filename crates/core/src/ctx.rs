//! What a handler works with: the transaction, the clock, the caller, and the outbox.

use std::path::Path;
use std::sync::Arc;

use rusqlite::Connection;

use crate::clock::Clock;
use crate::error::{RpcError, RpcResult};
use crate::ids::{Peer, UserId, access_hash};
use crate::outbox::{AccountUpdates, Outbox};
use crate::store::{self, accounts::Account};

/// One client connection of an account (a UI tab, a test, the Bot API gateway).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SessionId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Caller {
    pub account: UserId,
    pub session: Option<SessionId>,
}

pub struct Ctx<'a> {
    pub db: &'a Connection,
    pub clock: &'a Clock,
    secret: &'a [u8; 32],
    caller: Option<Caller>,
    pub out: Outbox,
}

impl Ctx<'_> {
    pub fn now(&self) -> i32 {
        self.clock.now()
    }

    /// The account the call runs as; admin operations have none.
    pub fn caller(&self) -> RpcResult<Caller> {
        self.caller.ok_or_else(RpcError::auth_key_unregistered)
    }

    pub fn account(&self, id: UserId) -> RpcResult<Account> {
        store::accounts::get(self.db, id)?.ok_or_else(RpcError::user_id_invalid)
    }

    pub fn access_hash(&self, viewer: UserId, target: Peer) -> i64 {
        access_hash(self.secret, viewer, target)
    }

    /// Session to tag updates for `account` with, if the caller is that account.
    pub fn origin_for(&self, account: UserId) -> Option<crate::ctx::SessionId> {
        self.caller
            .filter(|c| c.account == account)
            .and_then(|c| c.session)
    }
}

/// A sandbox's state, owned by its actor thread.
pub struct World {
    conn: Connection,
    clock: Arc<Clock>,
    secret: [u8; 32],
}

impl World {
    pub fn open(path: Option<&Path>, clock: Arc<Clock>) -> rusqlite::Result<Self> {
        let conn = store::open(path)?;
        let secret = match store::meta::get(&conn, "secret")? {
            Some(hex) => decode_hex(&hex),
            None => {
                let secret: [u8; 32] = rand::random();
                store::meta::set(&conn, "secret", &encode_hex(&secret))?;
                secret
            }
        };
        Ok(Self {
            conn,
            clock,
            secret,
        })
    }

    /// Run `f` in one transaction. On success the transaction commits and the updates it
    /// produced come back rendered per recipient; on error nothing is kept.
    pub fn transact<R>(
        &mut self,
        caller: Option<Caller>,
        f: impl FnOnce(&mut Ctx<'_>) -> RpcResult<R>,
    ) -> RpcResult<(R, Vec<AccountUpdates>)> {
        let tx = self.conn.transaction()?;
        let mut ctx = Ctx {
            db: &tx,
            clock: &self.clock,
            secret: &self.secret,
            caller,
            out: Outbox::default(),
        };
        let result = f(&mut ctx)?;
        let outbox = std::mem::take(&mut ctx.out);
        let updates = outbox.render(&ctx)?;
        drop(ctx);
        tx.commit()?;
        Ok((result, updates))
    }

    pub fn secret(&self) -> [u8; 32] {
        self.secret
    }

    /// Read-only access outside a handler (inspection, lookups).
    pub fn connection(&self) -> &Connection {
        &self.conn
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn decode_hex(hex: &str) -> [u8; 32] {
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(hex.get(i * 2..i * 2 + 2).unwrap_or("00"), 16).unwrap_or(0);
    }
    out
}
