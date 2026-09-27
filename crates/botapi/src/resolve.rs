//! Bot API `chat_id` → peer. Like tdlib, unknown chats are "chat not found" before any
//! request reaches the server.

use simula_botapi_types::ChatId;
use simula_core::{Peer, store};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult};

pub async fn chat(ctx: &BotCtx, chat_id: &ChatId) -> BotResult<Peer> {
    let not_found = || BotError::bad_request("chat not found");
    match chat_id {
        ChatId::Id(id) => {
            let peer = Peer::from_bot_api_id(*id).ok_or_else(not_found)?;
            if let Peer::User(user) = peer {
                ctx.gateway
                    .sandbox
                    .account(user)
                    .await
                    .map_err(BotError::internal)?
                    .ok_or_else(not_found)?;
            }
            Ok(peer)
        }
        ChatId::Username(name) => {
            let name = name.clone();
            let account = ctx
                .gateway
                .sandbox
                .run(move |world| store::accounts::by_username(world.connection(), &name))
                .await
                .map_err(BotError::internal)?;
            account.map(|a| Peer::User(a.id)).ok_or_else(not_found)
        }
    }
}
