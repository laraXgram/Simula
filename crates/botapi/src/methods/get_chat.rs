//! `getChat`

use simula_botapi_types::{methods, types as bot};
use simula_core::Peer;
use simula_tl::{enums, functions};

use crate::convert::{self, permissions};
use crate::ctx::BotCtx;
use crate::error::{BotResult, ErrorContext};
use crate::members;
use crate::pins;
use crate::present;
use crate::resolve;

/// Telegram's default reaction limit per message.
const MAX_REACTIONS: i64 = 11;

pub async fn handle(ctx: BotCtx, params: methods::GetChat) -> BotResult<bot::ChatFullInfo> {
    let peer = resolve::chat(&ctx, &params.chat_id).await?;
    let mut info = info(&ctx, peer).await?;
    info.pinned_message = pinned_message(&ctx, peer).await?;
    Ok(info)
}

/// The newest pinned message, without its replied message.
async fn pinned_message(ctx: &BotCtx, peer: Peer) -> BotResult<Option<Box<bot::Message>>> {
    let Some(id) = pins::pinned_id(ctx, peer).await? else {
        return Ok(None);
    };
    let context = present::context(&ctx.gateway, ctx.bot.id, vec![(peer, id)], &[], &[]).await;
    let entities = context.entities();
    Ok(entities
        .message(peer, id)
        .map(|m| Box::new(convert::message::without_reply(m, &entities))))
}

async fn info(ctx: &BotCtx, peer: Peer) -> BotResult<bot::ChatFullInfo> {
    // tdlib's default accent colour for chats without one.
    let accent = peer.bot_api_id().rem_euclid(7);
    match peer {
        Peer::User(id) => {
            let full = ctx
                .call(
                    functions::users::GetFullUser {
                        id: ctx.input_user(id.0),
                    },
                    ErrorContext::Plain,
                )
                .await?;
            let enums::users::UserFull::UserFull(full) = full;
            let enums::UserFull::UserFull(info) = &full.full_user;
            let found = full.users.iter().find_map(|u| match u {
                enums::User::User(u) if u.id == id.0 => Some(u),
                _ => None,
            });
            Ok(bot::ChatFullInfo {
                id: id.0,
                r#type: "private".into(),
                first_name: found.and_then(|u| u.first_name.clone()),
                last_name: found.and_then(|u| u.last_name.clone()),
                username: found.and_then(|u| u.username.clone()),
                bio: info.about.clone().filter(|a| !a.is_empty()),
                accent_color_id: accent,
                max_reaction_count: MAX_REACTIONS,
                accepted_gift_types: bot::AcceptedGiftTypes {
                    unlimited_gifts: true,
                    limited_gifts: true,
                    unique_gifts: true,
                    premium_subscription: true,
                    ..Default::default()
                },
                ..Default::default()
            })
        }
        Peer::Chat(chat) => {
            let group = members::group(ctx, chat).await?;
            let defaults = group.chats.iter().find_map(|c| match c {
                enums::Chat::Chat(c) => c.default_banned_rights.clone(),
                _ => None,
            });
            Ok(bot::ChatFullInfo {
                id: peer.bot_api_id(),
                r#type: "group".into(),
                title: Some(group.title.clone()),
                description: (!group.about.is_empty()).then(|| group.about.clone()),
                invite_link: group.invite_link.clone(),
                permissions: Some(Box::new(match defaults {
                    Some(enums::ChatBannedRights::ChatBannedRights(r)) => {
                        permissions::from_banned(&r)
                    }
                    None => everything_allowed(),
                })),
                accent_color_id: accent,
                max_reaction_count: MAX_REACTIONS,
                ..Default::default()
            })
        }
        Peer::Channel(_) => {
            let channel = members::channel(ctx, peer).await?;
            let info = &channel.info;
            Ok(bot::ChatFullInfo {
                id: peer.bot_api_id(),
                r#type: if info.broadcast {
                    "channel"
                } else {
                    "supergroup"
                }
                .into(),
                title: Some(info.title.clone()),
                username: info.username.clone(),
                is_forum: info.forum && !info.broadcast,
                description: (!channel.full.about.is_empty()).then(|| channel.full.about.clone()),
                invite_link: match &channel.full.exported_invite {
                    Some(enums::ExportedChatInvite::ChatInviteExported(i)) => Some(i.link.clone()),
                    _ => None,
                },
                permissions: match &info.default_banned_rights {
                    Some(enums::ChatBannedRights::ChatBannedRights(r)) if !info.broadcast => {
                        Some(Box::new(permissions::from_banned(r)))
                    }
                    _ => None,
                },
                has_protected_content: info.noforwards,
                join_to_send_messages: info.join_to_send,
                join_by_request: info.join_request,
                accent_color_id: accent,
                max_reaction_count: MAX_REACTIONS,
                ..Default::default()
            })
        }
    }
}

/// A basic group's default permissions: members may do everything.
fn everything_allowed() -> bot::ChatPermissions {
    let yes = Some(true);
    bot::ChatPermissions {
        can_send_messages: yes,
        can_send_audios: yes,
        can_send_documents: yes,
        can_send_photos: yes,
        can_send_videos: yes,
        can_send_video_notes: yes,
        can_send_voice_notes: yes,
        can_send_polls: yes,
        can_send_other_messages: yes,
        can_add_web_page_previews: yes,
        can_react_to_messages: yes,
        can_edit_tag: yes,
        can_change_info: yes,
        can_invite_users: yes,
        can_pin_messages: yes,
        can_manage_topics: yes,
    }
}
