//! Bot API `chat_id` → peer. Like tdlib, unknown chats are "chat not found" before any
//! request reaches the server.

use simula_botapi_types::ChatId;
use simula_core::{Peer, store};
use simula_tl::{enums, functions, types};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};

/// What the caller is about to do in the chat (telegram-bot-api's `AccessRights`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    Read,
    /// Sending, editing, deleting.
    Write,
}

pub async fn chat(ctx: &BotCtx, chat_id: &ChatId) -> BotResult<Peer> {
    chat_for(ctx, chat_id, Access::Read).await
}

/// The chat, after telegram-bot-api's `check_chat_access` for basic groups: a group the
/// bot left reads fine but can't be written to, nor one it was removed from.
pub async fn chat_for(ctx: &BotCtx, chat_id: &ChatId, access: Access) -> BotResult<Peer> {
    let peer = peer(ctx, chat_id).await?;
    if let Peer::Chat(chat) = peer {
        check_group(ctx, chat, access).await?;
    }
    Ok(peer)
}

async fn check_group(ctx: &BotCtx, chat: simula_core::ChatId, access: Access) -> BotResult<()> {
    let not_found = || BotError::bad_request("chat not found");
    let found = ctx
        .call(
            functions::messages::GetChats { id: vec![chat.0] },
            ErrorContext::Plain,
        )
        .await?;
    let enums::messages::Chats::Chats(found) = found else {
        return Err(not_found());
    };
    let write = access == Access::Write;
    match found.chats.first() {
        Some(enums::Chat::Chat(c)) if c.deactivated && write => {
            Err(BotError::forbidden("the group chat was deleted"))
        }
        Some(enums::Chat::Chat(c)) if c.left && write => {
            Err(BotError::forbidden("bot is not a member of the group chat"))
        }
        Some(enums::Chat::Chat(_)) => Ok(()),
        // Removed members get `chatForbidden`; it's unknown if the bot never was in it.
        Some(enums::Chat::Forbidden(_)) if knows(ctx, chat).await? => {
            if write {
                Err(BotError::forbidden("bot was kicked from the group chat"))
            } else {
                Ok(())
            }
        }
        _ => Err(not_found()),
    }
}

/// Whether the bot has any history in the group (it was a member once).
async fn knows(ctx: &BotCtx, chat: simula_core::ChatId) -> BotResult<bool> {
    let history = ctx
        .call(
            functions::messages::GetHistory {
                peer: types::InputPeerChat { chat_id: chat.0 }.into(),
                limit: 1,
                ..Default::default()
            },
            ErrorContext::Plain,
        )
        .await?;
    Ok(match history {
        enums::messages::Messages::Messages(m) => !m.messages.is_empty(),
        enums::messages::Messages::Slice(m) => !m.messages.is_empty(),
        _ => false,
    })
}

async fn peer(ctx: &BotCtx, chat_id: &ChatId) -> BotResult<Peer> {
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
