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

/// The chat, after telegram-bot-api's `check_chat_access`: a group the bot left reads
/// fine but can't be written to, nor one it was removed from; supergroups and channels
/// the bot isn't in are readable only when public, and banned bots get nothing.
pub async fn chat_for(ctx: &BotCtx, chat_id: &ChatId, access: Access) -> BotResult<Peer> {
    let peer = peer(ctx, chat_id).await?;
    match peer {
        Peer::Chat(chat) => check_group(ctx, chat, access).await?,
        Peer::Channel(_) => {
            channel(ctx, peer, access).await?;
        }
        Peer::User(_) => {}
    }
    Ok(peer)
}

/// The supergroup or channel as the bot sees it, after the access checks.
pub async fn channel(ctx: &BotCtx, peer: Peer, access: Access) -> BotResult<types::Channel> {
    let not_found = || BotError::bad_request("chat not found");
    let found = ctx
        .call(
            functions::channels::GetChannels {
                id: vec![ctx.input_channel(peer)],
            },
            ErrorContext::Plain,
        )
        .await
        .map_err(|_| not_found())?;
    let enums::messages::Chats::Chats(found) = found else {
        return Err(not_found());
    };
    let kind = |broadcast: bool| if broadcast { "channel" } else { "supergroup" };
    match found.chats.into_iter().next() {
        Some(enums::Chat::ChannelForbidden(c)) => Err(BotError::forbidden(format!(
            "bot was kicked from the {} chat",
            kind(c.broadcast)
        ))),
        Some(enums::Chat::Channel(c)) => {
            let public = c.username.is_some();
            let needs_membership = !public || access == Access::Write;
            if c.left && needs_membership {
                return Err(BotError::forbidden(format!(
                    "bot is not a member of the {} chat",
                    kind(c.broadcast)
                )));
            }
            Ok(*c)
        }
        _ => Err(not_found()),
    }
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
        Some(enums::Chat::Chat(c)) if c.deactivated && write => match &c.migrated_to {
            Some(enums::InputChannel::InputChannel(to)) => Err(BotError {
                migrate_to_chat_id: Some(
                    Peer::Channel(simula_core::ChannelId(to.channel_id)).bot_api_id(),
                ),
                ..BotError::bad_request("group chat was upgraded to a supergroup chat")
            }),
            _ => Err(BotError::forbidden("the group chat was deleted")),
        },
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
        // Public usernames name channels and supergroups (and users, which telegram-bot-api
        // would only find among chats the bot knows).
        ChatId::Username(name) => {
            let name = name.trim_start_matches('@').to_owned();
            let found = ctx
                .gateway
                .sandbox
                .run(move |world| {
                    let db = world.connection();
                    match store::channels::by_username(db, &name) {
                        Ok(Some(channel)) => Ok(Some(Peer::Channel(channel.id))),
                        Ok(None) => store::accounts::by_username(db, &name)
                            .map(|found| found.map(|a| Peer::User(a.id))),
                        Err(error) => Err(error),
                    }
                })
                .await
                .map_err(BotError::internal)?;
            found.ok_or_else(not_found)
        }
    }
}
