//! What a method handler gets: the gateway and the authenticated bot.

use std::sync::{Arc, Mutex};

use simula_core::{Account, Caller, Peer};
use simula_tl::{Function, RemoteCall, enums, types};

use crate::error::{BotError, BotResult, ErrorContext};
use crate::state::Gateway;
use crate::text::{self, Entity};
use crate::updates::queue::BotState;

pub struct BotCtx {
    pub gateway: Arc<Gateway>,
    pub bot: Account,
    /// The `description` of a successful response ("Webhook was set"), if the method
    /// gives one.
    description: Arc<Mutex<Option<&'static str>>>,
}

impl BotCtx {
    pub fn new(gateway: Arc<Gateway>, bot: Account) -> Self {
        Self {
            gateway,
            bot,
            description: Arc::default(),
        }
    }

    pub fn describe(&self, description: &'static str) {
        *self.description.lock().expect("description lock") = Some(description);
    }

    /// A handle to read the description after the handler consumed the context.
    pub fn description_slot(&self) -> Arc<Mutex<Option<&'static str>>> {
        self.description.clone()
    }

    pub fn caller(&self) -> Caller {
        Caller {
            account: self.bot.id,
            session: Some(self.gateway.session),
        }
    }

    /// Run a TL function as the bot; MTProto errors are translated like tdlib would.
    pub async fn call<F>(&self, call: F, context: ErrorContext) -> BotResult<F::Return>
    where
        F: RemoteCall + Into<Function>,
        F::Return: 'static,
    {
        self.gateway
            .sandbox
            .call(self.caller(), call)
            .await
            .map_err(|e| BotError::from_rpc(&e, context))
    }

    pub fn input_peer(&self, peer: Peer) -> enums::InputPeer {
        self.gateway.sandbox.input_peer(self.bot.id, peer)
    }

    /// A user as tdlib addresses users it has no access hash for: by id with hash 0,
    /// which the server accepts from a bot for users it has met.
    pub fn input_user(&self, user_id: i64) -> enums::InputUser {
        if user_id == self.bot.id.0 {
            return enums::InputUser::InputUserSelf;
        }
        types::InputUser {
            user_id,
            access_hash: 0,
        }
        .into()
    }

    /// Parsed entities as sent in a TL call.
    pub fn tl_entities(&self, entities: &[Entity]) -> Vec<enums::MessageEntity> {
        entities
            .iter()
            .filter_map(|e| text::entity::to_tl(e, |id| Some(self.input_user(id))))
            .collect()
    }

    pub fn state(&self) -> Arc<BotState> {
        self.gateway.bot_state(self.bot.id)
    }
}
