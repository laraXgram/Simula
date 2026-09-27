//! The gateway for one sandbox: authentication and per-bot delivery state.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use simula_core::{Account, Sandbox, SessionId, UserId};

use crate::error::{BotError, BotResult};
use crate::updates::{feeder, queue::BotState};

pub struct Gateway {
    pub sandbox: Arc<Sandbox>,
    /// Session tag for every call the gateway makes on behalf of bots.
    pub session: SessionId,
    bots: Mutex<HashMap<UserId, Arc<BotState>>>,
}

impl Gateway {
    /// Create the gateway and attach it to the sandbox's update stream.
    pub fn new(sandbox: Arc<Sandbox>) -> Arc<Self> {
        let gateway = Arc::new(Self {
            sandbox,
            session: SessionId(rand::random()),
            bots: Mutex::default(),
        });
        feeder::attach(&gateway);
        gateway
    }

    /// `<id>:<secret>` → the bot. Telegram answers 404 for malformed tokens and 401 for
    /// well-formed unknown ones.
    pub async fn authenticate(&self, token: &str) -> BotResult<Account> {
        let well_formed = token
            .split_once(':')
            .is_some_and(|(id, secret)| id.parse::<i64>().is_ok() && !secret.is_empty());
        if !well_formed {
            return Err(BotError::not_found());
        }
        match self.sandbox.account_by_token(token.to_owned()).await {
            Ok(Some(account)) if account.is_bot => Ok(account),
            Ok(_) => Err(BotError::unauthorized()),
            Err(error) => Err(BotError::internal(error)),
        }
    }

    pub fn bot_state(&self, bot: UserId) -> Arc<BotState> {
        self.bots
            .lock()
            .expect("bots lock")
            .entry(bot)
            .or_default()
            .clone()
    }
}
