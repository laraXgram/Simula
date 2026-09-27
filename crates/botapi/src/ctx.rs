//! What a method handler gets: the gateway and the authenticated bot.

use std::sync::Arc;

use simula_core::{Account, Caller, Peer};
use simula_tl::{Function, RemoteCall, enums};

use crate::error::{BotError, BotResult, ErrorContext};
use crate::state::Gateway;
use crate::updates::queue::BotState;

pub struct BotCtx {
    pub gateway: Arc<Gateway>,
    pub bot: Account,
}

impl BotCtx {
    pub fn caller(&self) -> Caller {
        Caller { account: self.bot.id, session: Some(self.gateway.session) }
    }

    /// Run a TL function as the bot; MTProto errors are translated like tdlib would.
    pub async fn call<F>(&self, call: F, context: ErrorContext) -> BotResult<F::Return>
    where
        F: RemoteCall + Into<Function>,
        F::Return: 'static,
    {
        self.gateway.sandbox.call(self.caller(), call).await.map_err(|e| BotError::from_rpc(&e, context))
    }

    pub fn input_peer(&self, peer: Peer) -> enums::InputPeer {
        self.gateway.sandbox.input_peer(self.bot.id, peer)
    }

    pub fn state(&self) -> Arc<BotState> {
        self.gateway.bot_state(self.bot.id)
    }
}
