//! `getChat`

use simula_botapi_types::{methods, types as bot};
use simula_core::Peer;
use simula_tl::{enums, functions};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};
use crate::members;
use crate::resolve;

/// Telegram's default reaction limit per message.
const MAX_REACTIONS: i64 = 11;

pub async fn handle(ctx: BotCtx, params: methods::GetChat) -> BotResult<bot::ChatFullInfo> {
    let peer = resolve::chat(&ctx, &params.chat_id).await?;
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
            let group = members::group(&ctx, chat).await?;
            Ok(bot::ChatFullInfo {
                id: peer.bot_api_id(),
                r#type: "group".into(),
                title: Some(group.title.clone()),
                description: (!group.about.is_empty()).then(|| group.about.clone()),
                permissions: Some(Box::new(everything_allowed())),
                accent_color_id: accent,
                max_reaction_count: MAX_REACTIONS,
                ..Default::default()
            })
        }
        Peer::Channel(_) => Err(BotError::not_implemented("channels and supergroups")),
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
