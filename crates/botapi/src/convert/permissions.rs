//! TL rights ⇄ Bot API permissions. Restrictions are rights taken away
//! (`chatBannedRights`), permissions are what is allowed (`ChatPermissions`); admin
//! rights map onto `ChatMemberAdministrator` and `promoteChatMember`'s flags, as tdlib's
//! `RestrictedRights` and `AdministratorRights` do.

use simula_botapi_types::{methods, types as bot};
use simula_tl::types;

/// What members may do, given the rights taken from them.
pub fn from_banned(banned: &types::ChatBannedRights) -> bot::ChatPermissions {
    let all = banned.send_messages;
    let media = all || banned.send_media;
    let allowed = |taken: bool| Some(!taken);
    bot::ChatPermissions {
        can_send_messages: allowed(all || banned.send_plain),
        can_send_audios: allowed(media || banned.send_audios),
        can_send_documents: allowed(media || banned.send_docs),
        can_send_photos: allowed(media || banned.send_photos),
        can_send_videos: allowed(media || banned.send_videos),
        can_send_video_notes: allowed(media || banned.send_roundvideos),
        can_send_voice_notes: allowed(media || banned.send_voices),
        can_send_polls: allowed(all || banned.send_polls),
        can_send_other_messages: allowed(
            all || banned.send_stickers
                || banned.send_gifs
                || banned.send_games
                || banned.send_inline,
        ),
        can_add_web_page_previews: allowed(all || banned.embed_links),
        can_react_to_messages: allowed(banned.send_reactions),
        can_edit_tag: allowed(banned.edit_rank),
        can_change_info: allowed(banned.change_info),
        can_invite_users: allowed(banned.invite_users),
        can_pin_messages: allowed(banned.pin_messages),
        can_manage_topics: allowed(banned.manage_topics),
    }
}

/// The rights taken away by `permissions` (omitted ones are not granted). Without
/// `independent`, telegram-bot-api's older rules apply: sending other messages or link
/// previews implies media, and media or polls imply text.
pub fn to_banned(permissions: &bot::ChatPermissions, independent: bool) -> types::ChatBannedRights {
    let flag = |value: Option<bool>| value == Some(true);
    let mut text = flag(permissions.can_send_messages);
    let mut audios = flag(permissions.can_send_audios);
    let mut documents = flag(permissions.can_send_documents);
    let mut photos = flag(permissions.can_send_photos);
    let mut videos = flag(permissions.can_send_videos);
    let mut video_notes = flag(permissions.can_send_video_notes);
    let mut voice_notes = flag(permissions.can_send_voice_notes);
    let polls = flag(permissions.can_send_polls);
    let other = flag(permissions.can_send_other_messages);
    let previews = flag(permissions.can_add_web_page_previews);
    if !independent {
        if other || previews {
            audios = true;
            documents = true;
            photos = true;
            videos = true;
            video_notes = true;
            voice_notes = true;
        }
        if polls || audios || documents || photos || videos || video_notes || voice_notes {
            text = true;
        }
    }
    let no_media = !(audios || documents || photos || videos || video_notes || voice_notes);
    types::ChatBannedRights {
        send_messages: !text && no_media && !polls && !other,
        send_media: no_media,
        send_plain: !text,
        send_audios: !audios,
        send_docs: !documents,
        send_photos: !photos,
        send_videos: !videos,
        send_roundvideos: !video_notes,
        send_voices: !voice_notes,
        send_polls: !polls,
        send_stickers: !other,
        send_gifs: !other,
        send_games: !other,
        send_inline: !other,
        embed_links: !previews,
        send_reactions: !flag(permissions.can_react_to_messages),
        edit_rank: !flag(permissions.can_edit_tag),
        change_info: !flag(permissions.can_change_info),
        invite_users: !flag(permissions.can_invite_users),
        pin_messages: !flag(permissions.can_pin_messages),
        manage_topics: !flag(permissions.can_manage_topics),
        ..Default::default()
    }
}

/// An admin's rights as `ChatMemberAdministrator`. Channel-only and supergroup-only
/// rights are present only in their kind of chat.
pub fn administrator(
    rights: &types::ChatAdminRights,
    broadcast: bool,
    user: bot::User,
    can_be_edited: bool,
    custom_title: Option<String>,
) -> bot::ChatMemberAdministrator {
    let channel_only = |value: bool| broadcast.then_some(value);
    let group_only = |value: bool| (!broadcast).then_some(value);
    bot::ChatMemberAdministrator {
        user,
        can_be_edited,
        is_anonymous: rights.anonymous,
        // Implied by any other administrator right.
        can_manage_chat: true,
        can_delete_messages: rights.delete_messages,
        can_manage_video_chats: rights.manage_call,
        can_restrict_members: rights.ban_users,
        can_promote_members: rights.add_admins,
        can_change_info: rights.change_info,
        can_invite_users: rights.invite_users,
        can_post_stories: rights.post_stories,
        can_edit_stories: rights.edit_stories,
        can_delete_stories: rights.delete_stories,
        can_post_messages: channel_only(rights.post_messages),
        can_edit_messages: channel_only(rights.edit_messages),
        can_pin_messages: group_only(rights.pin_messages),
        can_manage_topics: group_only(rights.manage_topics),
        can_manage_direct_messages: channel_only(rights.manage_direct_messages),
        can_manage_tags: group_only(rights.manage_ranks),
        can_send_welcome_messages: rights.manage_welcome_messages,
        custom_title,
        ..Default::default()
    }
}

/// `promoteChatMember`'s flags as TL admin rights. Any right implies managing the chat.
pub fn promoted(p: &methods::PromoteChatMember) -> types::ChatAdminRights {
    let flag = |value: Option<bool>| value == Some(true);
    let mut rights = types::ChatAdminRights {
        anonymous: flag(p.is_anonymous),
        delete_messages: flag(p.can_delete_messages),
        manage_call: flag(p.can_manage_video_chats),
        ban_users: flag(p.can_restrict_members),
        add_admins: flag(p.can_promote_members),
        change_info: flag(p.can_change_info),
        invite_users: flag(p.can_invite_users),
        post_stories: flag(p.can_post_stories),
        edit_stories: flag(p.can_edit_stories),
        delete_stories: flag(p.can_delete_stories),
        post_messages: flag(p.can_post_messages),
        edit_messages: flag(p.can_edit_messages),
        pin_messages: flag(p.can_pin_messages),
        manage_topics: flag(p.can_manage_topics),
        manage_direct_messages: flag(p.can_manage_direct_messages),
        manage_ranks: flag(p.can_manage_tags),
        manage_welcome_messages: flag(p.can_send_welcome_messages),
        ..Default::default()
    };
    let any = rights != types::ChatAdminRights::default();
    rights.other = flag(p.can_manage_chat) || any;
    rights
}
