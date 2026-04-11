use chrono::Utc;
use rusqlite::{params, OptionalExtension};

use crate::types::ApiError;
use crate::handlers::client::types::chats::SimChatRecord;
use crate::handlers::client::types::groups::GroupRuntimeSettings;

use crate::generated::types::ChatPermissions;

use super::{channels, chats};

pub fn is_active_chat_member_status(status: &str) -> bool {
    matches!(status, "owner" | "admin" | "member" | "restricted")
}

pub fn is_group_admin_or_owner_status(status: &str) -> bool {
    matches!(status, "owner" | "admin")
}

pub fn default_group_permissions() -> ChatPermissions {
    ChatPermissions {
        can_send_messages: Some(true),
        can_send_audios: Some(true),
        can_send_documents: Some(true),
        can_send_photos: Some(true),
        can_send_videos: Some(true),
        can_send_video_notes: Some(true),
        can_send_voice_notes: Some(true),
        can_send_polls: Some(true),
        can_send_other_messages: Some(true),
        can_add_web_page_previews: Some(true),
        can_change_info: Some(false),
        can_invite_users: Some(true),
        can_pin_messages: Some(false),
        can_manage_topics: Some(false),
        can_edit_tag: Some(false),
    }
}
pub fn is_group_owner_status(status: &str) -> bool {
    status == "owner"
}

pub fn normalize_group_membership_status(raw_status: &str) -> Option<&'static str> {
    match raw_status.trim().to_ascii_lowercase().as_str() {
        "admin" | "administrator" => Some("admin"),
        "member" => Some("member"),
        "restricted" => Some("restricted"),
        "left" | "remove" | "removed" => Some("left"),
        _ => None,
    }
}

pub fn build_chat_from_group_record(record: &SimChatRecord) -> Chat {
    Chat {
        id: record.chat_id,
        r#type: record.chat_type.clone(),
        title: record.title.clone(),
        username: record.username.clone(),
        first_name: None,
        last_name: None,
        is_forum: if record.chat_type == "supergroup" {
            Some(record.is_forum)
        } else {
            None
        },
        is_direct_messages: if record.is_direct_messages { Some(true) } else { None },
    }
}

pub fn load_group_runtime_settings(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
) -> Result<Option<GroupRuntimeSettings>, ApiError> {
    let row: Option<(String, i64, Option<String>)> = conn
        .query_row(
            "SELECT chat_type, slow_mode_delay, permissions_json
             FROM sim_chats
             WHERE bot_id = ?1 AND chat_key = ?2",
            params![bot_id, chat_key],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((chat_type, slow_mode_delay, permissions_raw)) = row else {
        return Ok(None);
    };
    if chat_type == "private" {
        return Ok(None);
    }

    let permissions = permissions_raw
        .as_deref()
        .and_then(|raw| serde_json::from_str::<ChatPermissions>(raw).ok())
        .unwrap_or_else(default_group_permissions);

    Ok(Some(GroupRuntimeSettings {
        chat_type,
        slow_mode_delay: slow_mode_delay.max(0),
        permissions,
    }))
}

pub fn handle_sim_create_group(
    state: &Data<AppState>,
    token: &str,
    body: SimCreateGroupRequest,
) -> ApiResult {
    let title = body.title.trim().to_string();
    if title.is_empty() {
        return Err(ApiError::bad_request("title is empty"));
    }

    let chat_type = body
        .chat_type
        .as_deref()
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "supergroup".to_string());
    if !matches!(chat_type.as_str(), "group" | "supergroup" | "channel") {
        return Err(ApiError::bad_request("chat_type must be group, supergroup, or channel"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let owner_record = ensure_user(
        &mut conn,
        body.owner_user_id,
        body.owner_first_name,
        body.owner_username,
    )?;

    let now = Utc::now().timestamp();
    let mut chat_id = if chat_type == "supergroup" || chat_type == "channel" {
        // Keep ids in -100xxxxxxxxxx range for Telegram supergroup/channel parity.
        -((Utc::now().timestamp_millis().abs() % 10_000_000_000) + 1_000_000_000_000)
    } else {
        -((Utc::now().timestamp_millis().abs() % 900_000_000_000) + 100_000)
    };
    loop {
        let exists: Option<i64> = conn
            .query_row(
                "SELECT chat_id FROM sim_chats WHERE bot_id = ?1 AND chat_id = ?2",
                params![bot.id, chat_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;
        if exists.is_none() {
            break;
        }
        chat_id -= 1;
    }

    let chat_key = chat_id.to_string();
    let description = body
        .description
        .as_deref()
        .map(str::trim)
        .map(str::to_string)
        .filter(|v| !v.is_empty());
    let username = body
        .username
        .as_deref()
        .map(sanitize_username)
        .filter(|v| !v.is_empty());
    let is_forum = if chat_type == "supergroup" {
        body.is_forum.unwrap_or(false)
    } else {
        false
    };
    let channel_show_author_signature = if chat_type == "channel" {
        body.show_author_signature.unwrap_or(false)
    } else {
        false
    };
    let message_history_visible = body.message_history_visible.unwrap_or(true);
    let slow_mode_delay = body.slow_mode_delay.unwrap_or(0).max(0);
    let permissions = default_group_permissions();
    let permissions_json = serde_json::to_string(&permissions).map_err(ApiError::internal)?;

    conn.execute(
        "INSERT INTO chats (chat_key, chat_type, title)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(chat_key)
         DO UPDATE SET chat_type = excluded.chat_type, title = excluded.title",
        params![chat_key, chat_type, title],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "INSERT INTO sim_chats
         (bot_id, chat_key, chat_id, chat_type, title, username, description, photo_file_id, is_forum, channel_show_author_signature, message_history_visible, slow_mode_delay, permissions_json, owner_user_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?14)",
        params![
            bot.id,
            chat_key,
            chat_id,
            chat_type,
            title,
            username,
            description,
            if is_forum { 1 } else { 0 },
            if channel_show_author_signature { 1 } else { 0 },
            if message_history_visible { 1 } else { 0 },
            slow_mode_delay,
            permissions_json,
            owner_record.id,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "INSERT INTO sim_chat_members (bot_id, chat_key, user_id, status, role, joined_at, updated_at)
         VALUES (?1, ?2, ?3, 'owner', 'owner', ?4, ?4)",
        params![bot.id, chat_key, owner_record.id, now],
    )
    .map_err(ApiError::internal)?;

    let bot_admin_rights_json = if chat_type == "channel" {
        Some(serde_json::to_string(&channel_admin_rights_full_access()).map_err(ApiError::internal)?)
    } else {
        None
    };

    conn.execute(
        "INSERT INTO sim_chat_members (bot_id, chat_key, user_id, status, role, admin_rights_json, joined_at, updated_at)
         VALUES (?1, ?2, ?3, 'admin', 'admin', ?4, ?5, ?5)",
        params![bot.id, chat_key, bot.id, bot_admin_rights_json, now],
    )
    .map_err(ApiError::internal)?;

    let mut member_users = Vec::<User>::new();
    let owner_user = build_user_from_sim_record(&owner_record, false);
    member_users.push(owner_user.clone());

    if let Some(member_ids) = body.initial_member_ids {
        for member_id in member_ids {
            if member_id == owner_record.id || member_id == bot.id {
                continue;
            }
            let member_record = ensure_user(
                &mut conn,
                Some(member_id),
                Some(format!("User {}", member_id)),
                None,
            )?;
            conn.execute(
                "INSERT INTO sim_chat_members (bot_id, chat_key, user_id, status, role, joined_at, updated_at)
                 VALUES (?1, ?2, ?3, 'member', 'member', ?4, ?4)
                 ON CONFLICT(bot_id, chat_key, user_id)
                 DO UPDATE SET status = 'member', role = 'member', joined_at = COALESCE(sim_chat_members.joined_at, excluded.joined_at), updated_at = excluded.updated_at",
                params![bot.id, chat_key, member_record.id, now],
            )
            .map_err(ApiError::internal)?;
            member_users.push(build_user_from_sim_record(&member_record, false));
        }
    }

    let chat = Chat {
        id: chat_id,
        r#type: chat_type.clone(),
        title: conn
            .query_row(
                "SELECT title FROM sim_chats WHERE bot_id = ?1 AND chat_key = ?2",
                params![bot.id, chat_key],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?
            .flatten(),
        username: conn
            .query_row(
                "SELECT username FROM sim_chats WHERE bot_id = ?1 AND chat_key = ?2",
                params![bot.id, chat_key],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?
            .flatten(),
        first_name: None,
        last_name: None,
        is_forum: if is_forum { Some(true) } else { None },
        is_direct_messages: None,
    };

    let bot_user = build_bot_user(&bot);
    let old_left_bot = to_chat_member(ChatMemberLeft {
        status: "left".to_string(),
        user: bot_user.clone(),
    })?;
    let new_admin_bot = to_chat_member(ChatMemberAdministrator {
        status: "administrator".to_string(),
        user: bot_user,
        can_be_edited: false,
        is_anonymous: false,
        can_manage_chat: true,
        can_delete_messages: true,
        can_manage_video_chats: true,
        can_restrict_members: true,
        can_promote_members: false,
        can_change_info: true,
        can_invite_users: true,
        can_post_stories: true,
        can_edit_stories: true,
        can_delete_stories: true,
        can_post_messages: if chat_type == "channel" { Some(true) } else { None },
        can_edit_messages: if chat_type == "channel" { Some(true) } else { None },
        can_pin_messages: if chat_type == "channel" { None } else { Some(true) },
        can_manage_topics: if chat_type == "supergroup" { Some(is_forum) } else { None },
        can_manage_direct_messages: None,
        can_manage_tags: None,
        custom_title: None,
    })?;

    let my_chat_member_update = Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: Some(ChatMemberUpdated {
            chat: chat.clone(),
            from: owner_user.clone(),
            date: now,
            old_chat_member: old_left_bot,
            new_chat_member: new_admin_bot,
            invite_link: None,
            via_join_request: None,
            via_chat_folder_invite_link: None,
        }),
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    };
    persist_and_dispatch_update(
        state,
        &mut conn,
        token,
        bot.id,
        serde_json::to_value(my_chat_member_update).map_err(ApiError::internal)?,
    )?;

    let old_left_owner = to_chat_member(ChatMemberLeft {
        status: "left".to_string(),
        user: owner_user.clone(),
    })?;
    let new_owner = to_chat_member(ChatMemberOwner {
        status: "creator".to_string(),
        user: owner_user.clone(),
        is_anonymous: false,
        custom_title: None,
    })?;

    let chat_member_update = Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: Some(ChatMemberUpdated {
            chat: chat.clone(),
            from: owner_user.clone(),
            date: now,
            old_chat_member: old_left_owner,
            new_chat_member: new_owner,
            invite_link: None,
            via_join_request: None,
            via_chat_folder_invite_link: None,
        }),
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    };
    persist_and_dispatch_update(
        state,
        &mut conn,
        token,
        bot.id,
        serde_json::to_value(chat_member_update).map_err(ApiError::internal)?,
    )?;

    let mut service_fields = Map::<String, Value>::new();
    if chat.r#type == "channel" {
        service_fields.insert("channel_chat_created".to_string(), Value::Bool(true));
    } else if chat.r#type == "supergroup" {
        service_fields.insert("supergroup_chat_created".to_string(), Value::Bool(true));
    } else {
        service_fields.insert("group_chat_created".to_string(), Value::Bool(true));
    }

    emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &owner_user,
        now,
        service_text_chat_created(&owner_user, &chat.r#type),
        service_fields,
    )?;

    let response = SimCreateGroupResponse {
        chat,
        owner: owner_user,
        members: member_users,
        settings: SimGroupSettingsResponse {
            show_author_signature: channel_show_author_signature,
            paid_star_reactions_enabled: false,
            message_history_visible,
            slow_mode_delay,
            permissions,
        },
    };

    serde_json::to_value(response).map_err(ApiError::internal)
}

pub fn handle_sim_update_group(
    state: &Data<AppState>,
    token: &str,
    body: SimUpdateGroupRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let chat_key = body.chat_id.to_string();
    let existing: Option<(
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        i64,
        i64,
        i64,
        Option<i64>,
        i64,
        i64,
        i64,
        i64,
        Option<String>,
    )> = conn
        .query_row(
            "SELECT chat_type, title, username, description, is_forum, channel_show_author_signature, channel_paid_reactions_enabled, linked_discussion_chat_id, direct_messages_enabled, direct_messages_star_count, message_history_visible, slow_mode_delay, permissions_json
             FROM sim_chats
             WHERE bot_id = ?1 AND chat_key = ?2",
            params![bot.id, &chat_key],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
                    row.get(12)?,
                ))
            },
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((
        chat_type,
        current_title,
        current_username,
        current_description,
        current_is_forum,
        current_show_author_signature,
        current_paid_star_reactions_enabled,
        current_linked_discussion_chat_id,
        current_direct_messages_enabled,
        current_direct_messages_star_count,
        current_message_history_visible,
        current_slow_mode_delay,
        current_permissions_json,
    )) = existing else {
        return Err(ApiError::not_found("group not found"));
    };
    if chat_type == "private" {
        return Err(ApiError::bad_request("cannot edit private chat with group endpoint"));
    }

    let current_permissions = current_permissions_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<ChatPermissions>(raw).ok())
        .unwrap_or_else(default_group_permissions);

    let actor_id = body.user_id.unwrap_or(bot.id);
    let actor = if actor_id == bot.id {
        build_bot_user(&bot)
    } else {
        let actor_record = ensure_user(
            &mut conn,
            Some(actor_id),
            body.actor_first_name,
            body.actor_username,
        )?;
        build_user_from_sim_record(&actor_record, false)
    };

    let actor_status: Option<String> = conn
        .query_row(
            "SELECT status FROM sim_chat_members WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
            params![bot.id, &chat_key, actor.id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let can_change_by_status = matches!(actor_status.as_deref(), Some("owner") | Some("admin"));
    let can_change_by_permission = matches!(actor_status.as_deref(), Some("member"))
        && current_permissions.can_change_info.unwrap_or(false);
    if !(can_change_by_status || can_change_by_permission) {
        return Err(ApiError::bad_request(
            "only owner/admin or members with can_change_info can edit group",
        ));
    }

    let title = body
        .title
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .or_else(|| current_title.clone())
        .unwrap_or_else(|| format!("Group {}", body.chat_id));
    let title_changed = current_title.as_deref() != Some(title.as_str());

    let username = match body.username {
        Some(value) => {
            let normalized = sanitize_username(&value);
            if normalized.is_empty() {
                None
            } else {
                Some(normalized)
            }
        }
        None => current_username.clone(),
    };

    let description = match body.description {
        Some(value) => {
            let trimmed = value.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        }
        None => current_description.clone(),
    };

    let is_forum = if chat_type == "supergroup" {
        body.is_forum.unwrap_or(current_is_forum == 1)
    } else {
        false
    };
    let show_author_signature = if chat_type == "channel" {
        body.show_author_signature
            .unwrap_or(current_show_author_signature == 1)
    } else {
        false
    };
    let paid_star_reactions_enabled = if chat_type == "channel" {
        body.paid_star_reactions_enabled
            .unwrap_or(current_paid_star_reactions_enabled == 1)
    } else {
        if body.paid_star_reactions_enabled.is_some() {
            return Err(ApiError::bad_request(
                "paid_star_reactions_enabled can only be set for channels",
            ));
        }
        false
    };

    let linked_discussion_chat_id = if chat_type == "channel" {
        if let Some(raw_linked_chat_id) = body.linked_chat_id {
            if raw_linked_chat_id == 0 {
                None
            } else {
                if raw_linked_chat_id == body.chat_id {
                    return Err(ApiError::bad_request("linked_chat_id must be different from channel chat_id"));
                }

                let linked_chat_key = raw_linked_chat_id.to_string();
                let linked_chat_type: Option<String> = conn
                    .query_row(
                        "SELECT chat_type
                         FROM sim_chats
                         WHERE bot_id = ?1 AND chat_key = ?2",
                        params![bot.id, &linked_chat_key],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(ApiError::internal)?;

                match linked_chat_type.as_deref() {
                    Some("group") | Some("supergroup") => Some(raw_linked_chat_id),
                    Some("private") | Some("channel") => {
                        return Err(ApiError::bad_request("linked_chat_id must reference a group or supergroup"));
                    }
                    Some(_) => {
                        return Err(ApiError::bad_request("linked_chat_id has unsupported chat type"));
                    }
                    None => {
                        return Err(ApiError::not_found("linked chat not found"));
                    }
                }
            }
        } else {
            current_linked_discussion_chat_id
        }
    } else {
        if body.linked_chat_id.is_some() {
            return Err(ApiError::bad_request("linked_chat_id can only be set for channels"));
        }
        None
    };

    let direct_messages_enabled = if chat_type == "channel" {
        body.direct_messages_enabled
            .unwrap_or(current_direct_messages_enabled == 1)
    } else {
        if body.direct_messages_enabled.is_some() {
            return Err(ApiError::bad_request("direct_messages_enabled can only be set for channels"));
        }
        false
    };

    let direct_messages_star_count = if chat_type == "channel" {
        body.direct_messages_star_count
            .unwrap_or(current_direct_messages_star_count)
            .max(0)
    } else {
        if body.direct_messages_star_count.is_some() {
            return Err(ApiError::bad_request("direct_messages_star_count can only be set for channels"));
        }
        0
    };

    let message_history_visible = body
        .message_history_visible
        .unwrap_or(current_message_history_visible == 1);
    let slow_mode_delay = body
        .slow_mode_delay
        .unwrap_or(current_slow_mode_delay)
        .max(0);
    let permissions = body.permissions.unwrap_or_else(|| current_permissions.clone());
    let permissions_json = serde_json::to_string(&permissions).map_err(ApiError::internal)?;

    let now = Utc::now().timestamp();

    if chat_type == "channel" {
        if let Some(linked_chat_id) = linked_discussion_chat_id {
            conn.execute(
                "UPDATE sim_chats
                 SET linked_discussion_chat_id = NULL,
                     updated_at = ?1
                 WHERE bot_id = ?2
                   AND chat_type = 'channel'
                   AND chat_key <> ?3
                   AND linked_discussion_chat_id = ?4",
                params![now, bot.id, &chat_key, linked_chat_id],
            )
            .map_err(ApiError::internal)?;
        }
    }

    conn.execute(
        "UPDATE chats SET title = ?1 WHERE chat_key = ?2",
        params![&title, &chat_key],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "UPDATE sim_chats
         SET title = ?1,
             username = ?2,
             description = ?3,
             is_forum = ?4,
             channel_show_author_signature = ?5,
             channel_paid_reactions_enabled = ?6,
             linked_discussion_chat_id = ?7,
             direct_messages_enabled = ?8,
             direct_messages_star_count = ?9,
             message_history_visible = ?10,
             slow_mode_delay = ?11,
             permissions_json = ?12,
             updated_at = ?13
         WHERE bot_id = ?14 AND chat_key = ?15",
        params![
            title,
            username,
            description,
            if is_forum { 1 } else { 0 },
            if show_author_signature { 1 } else { 0 },
            if paid_star_reactions_enabled { 1 } else { 0 },
            linked_discussion_chat_id,
            if direct_messages_enabled { 1 } else { 0 },
            direct_messages_star_count,
            if message_history_visible { 1 } else { 0 },
            slow_mode_delay,
            permissions_json,
            now,
            bot.id,
            &chat_key,
        ],
    )
    .map_err(ApiError::internal)?;

    if title_changed {
        let chat = Chat {
            id: body.chat_id,
            r#type: chat_type.clone(),
            title: Some(title.clone()),
            username: username.clone(),
            first_name: None,
            last_name: None,
            is_forum: if is_forum { Some(true) } else { None },
            is_direct_messages: None,
        };

        let mut service_fields = Map::<String, Value>::new();
        service_fields.insert("new_chat_title".to_string(), Value::String(title.clone()));

        emit_service_message_update(
            state,
            &mut conn,
            token,
            bot.id,
            &chat_key,
            &chat,
            &actor,
            now,
            service_text_group_title_changed(&actor, &title),
            service_fields,
        )?;
    }

    Ok(json!({
        "chat": {
            "id": body.chat_id,
            "type": chat_type,
            "title": title,
            "username": username,
            "is_forum": if is_forum { Some(true) } else { None::<bool> }
        },
        "settings": {
            "description": description,
            "show_author_signature": show_author_signature,
            "paid_star_reactions_enabled": paid_star_reactions_enabled,
            "linked_chat_id": linked_discussion_chat_id,
            "direct_messages_enabled": direct_messages_enabled,
            "direct_messages_star_count": direct_messages_star_count,
            "message_history_visible": message_history_visible,
            "slow_mode_delay": slow_mode_delay,
            "permissions": permissions
        }
    }))
}

pub fn handle_sim_delete_group(
    state: &Data<AppState>,
    token: &str,
    body: SimDeleteGroupRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let chat_key = body.chat_id.to_string();
    let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &chat_key)? else {
        return Err(ApiError::not_found("group not found"));
    };
    if sim_chat.chat_type == "private" {
        return Err(ApiError::bad_request("cannot delete private chat with group endpoint"));
    }

    let Some(actor_id) = body.user_id else {
        return Err(ApiError::bad_request("owner user_id is required to delete group"));
    };
    let actor = get_or_create_user(
        &mut conn,
        Some(actor_id),
        body.actor_first_name,
        body.actor_username,
    )?;

    let actor_status = load_chat_member_status(&mut conn, bot.id, &chat_key, actor.id)?;
    if !actor_status
        .as_deref()
        .map(is_group_owner_status)
        .unwrap_or(false)
    {
        return Err(ApiError::bad_request("only group owner can delete this group"));
    }

    conn.execute(
        "DELETE FROM sim_chat_join_requests WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM sim_chat_invite_links WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM message_reactions WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM chat_reply_keyboards WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM callback_queries WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM inline_queries WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM shipping_queries WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM pre_checkout_queries WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM invoices WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM inline_messages WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    let poll_ids: Vec<String> = {
        let mut stmt = conn
            .prepare("SELECT id FROM polls WHERE bot_id = ?1 AND chat_key = ?2")
            .map_err(ApiError::internal)?;
        let rows = stmt
            .query_map(params![bot.id, &chat_key], |row| row.get::<_, String>(0))
            .map_err(ApiError::internal)?;
        let mut ids = Vec::new();
        for row in rows {
            ids.push(row.map_err(ApiError::internal)?);
        }
        ids
    };
    for poll_id in poll_ids {
        conn.execute("DELETE FROM poll_votes WHERE poll_id = ?1", params![poll_id])
            .map_err(ApiError::internal)?;
        conn.execute("DELETE FROM poll_metadata WHERE poll_id = ?1", params![poll_id])
            .map_err(ApiError::internal)?;
    }
    conn.execute(
        "DELETE FROM polls WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "DELETE FROM sim_message_drafts WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "DELETE FROM messages WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    let chat_fragment = format!("\"chat\":{{\"id\":{}", body.chat_id);
    conn.execute(
        "DELETE FROM updates WHERE bot_id = ?1 AND update_json LIKE ?2",
        params![bot.id, format!("%{}%", chat_fragment)],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "DELETE FROM sim_chat_members WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM sim_chats WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM chats WHERE chat_key = ?1", params![&chat_key])
        .map_err(ApiError::internal)?;

    Ok(json!({
        "deleted": true,
        "chat_id": body.chat_id
    }))
}

pub fn join_user_to_group(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot_id: i64,
    sim_chat: &SimChatRecord,
    user: &SimUserRecord,
    old_status: Option<&str>,
    invite_link: Option<ChatInviteLink>,
    via_join_request: Option<bool>,
) -> Result<(), ApiError> {
    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_chat_members (bot_id, chat_key, user_id, status, role, joined_at, updated_at)
         VALUES (?1, ?2, ?3, 'member', 'member', ?4, ?4)
         ON CONFLICT(bot_id, chat_key, user_id)
         DO UPDATE SET status = 'member', role = 'member', joined_at = COALESCE(sim_chat_members.joined_at, excluded.joined_at), updated_at = excluded.updated_at",
        params![bot_id, &sim_chat.chat_key, user.id, now],
    )
    .map_err(ApiError::internal)?;

    let from_user = build_user_from_sim_record(user, false);
    let chat = chat_from_sim_record(sim_chat, user);
    let old_member = chat_member_from_status(old_status.unwrap_or("left"), &from_user)?;
    let new_member = chat_member_from_status("member", &from_user)?;

    let update = Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: Some(ChatMemberUpdated {
            chat: chat.clone(),
            from: from_user.clone(),
            date: now,
            old_chat_member: old_member,
            new_chat_member: new_member,
            invite_link,
            via_join_request,
            via_chat_folder_invite_link: None,
        }),
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    };
    persist_and_dispatch_update(
        state,
        conn,
        token,
        bot_id,
        serde_json::to_value(update).map_err(ApiError::internal)?,
    )?;

    if sim_chat.chat_type != "channel" {
        let mut service_fields = Map::<String, Value>::new();
        service_fields.insert(
            "new_chat_members".to_string(),
            serde_json::to_value(vec![from_user.clone()]).map_err(ApiError::internal)?,
        );
        emit_service_message_update(
            state,
            conn,
            token,
            bot_id,
            &sim_chat.chat_key,
            &chat,
            &from_user,
            now,
            service_text_new_chat_members(&from_user, std::slice::from_ref(&from_user)),
            service_fields,
        )?;
    }

    Ok(())
}

pub fn handle_sim_join_group(
    state: &Data<AppState>,
    token: &str,
    body: SimJoinGroupRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;

    let chat_key = body.chat_id.to_string();
    let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &chat_key)? else {
        return Err(ApiError::not_found("chat not found"));
    };
    if sim_chat.chat_type == "private" {
        return Err(ApiError::bad_request("cannot join private chat"));
    }

    let current_status = load_chat_member_status(&mut conn, bot.id, &chat_key, user.id)?;

    if current_status
        .as_deref()
        .map(is_active_chat_member_status)
        .unwrap_or(false)
    {
        return Ok(json!({
            "joined": false,
            "reason": "already_member",
            "chat_id": body.chat_id,
            "user_id": user.id
        }));
    }
    if current_status.as_deref() == Some("banned") {
        return Err(ApiError::bad_request("user is banned in this chat"));
    }

    let primary_invite_link: Option<String> = conn
        .query_row(
            "SELECT invite_link
             FROM sim_chat_invite_links
             WHERE bot_id = ?1 AND chat_key = ?2 AND is_primary = 1 AND is_revoked = 0
             ORDER BY updated_at DESC
             LIMIT 1",
            params![bot.id, &chat_key],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let primary_invite = if let Some(raw_link) = primary_invite_link {
        if let Some(record) = load_invite_link_record(&mut conn, bot.id, &chat_key, &raw_link)? {
            Some(chat_invite_link_from_record(
                resolve_invite_creator_user(&mut conn, &bot, record.creator_user_id)?,
                &record,
                None,
            ))
        } else {
            None
        }
    } else {
        None
    };

    if primary_invite
        .as_ref()
        .map(|invite| invite.creates_join_request)
        .unwrap_or(false)
    {
        let now = Utc::now().timestamp();
        conn.execute(
            "INSERT INTO sim_chat_join_requests
             (bot_id, chat_key, user_id, invite_link, status, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, 'pending', ?5, ?5)
             ON CONFLICT(bot_id, chat_key, user_id)
             DO UPDATE SET invite_link = excluded.invite_link, status = 'pending', updated_at = excluded.updated_at",
            params![
                bot.id,
                &chat_key,
                user.id,
                primary_invite.as_ref().map(|invite| invite.invite_link.clone()),
                now,
            ],
        )
        .map_err(ApiError::internal)?;

        let from_user = build_user_from_sim_record(&user, false);
        let chat = chat_from_sim_record(&sim_chat, &user);
        let join_request_update = Update {
            update_id: 0,
            message: None,
            edited_message: None,
            channel_post: None,
            edited_channel_post: None,
            business_connection: None,
            business_message: None,
            edited_business_message: None,
            deleted_business_messages: None,
            message_reaction: None,
            message_reaction_count: None,
            inline_query: None,
            chosen_inline_result: None,
            callback_query: None,
            shipping_query: None,
            pre_checkout_query: None,
            purchased_paid_media: None,
            poll: None,
            poll_answer: None,
            my_chat_member: None,
            chat_member: None,
            chat_join_request: Some(ChatJoinRequest {
                chat,
                from: from_user,
                user_chat_id: build_sim_user_chat_id(sim_chat.chat_id, user.id),
                date: now,
                bio: None,
                invite_link: primary_invite,
            }),
            chat_boost: None,
            removed_chat_boost: None,
            managed_bot: None,
        };
        persist_and_dispatch_update(
            state,
            &mut conn,
            token,
            bot.id,
            serde_json::to_value(join_request_update).map_err(ApiError::internal)?,
        )?;

        return Ok(json!({
            "joined": false,
            "pending": true,
            "chat_id": body.chat_id,
            "user_id": user.id
        }));
    }

    join_user_to_group(
        state,
        &mut conn,
        token,
        bot.id,
        &sim_chat,
        &user,
        current_status.as_deref(),
        primary_invite,
        None,
    )?;

    Ok(json!({
        "joined": true,
        "pending": false,
        "chat_id": body.chat_id,
        "user_id": user.id
    }))
}

pub fn handle_sim_leave_group(
    state: &Data<AppState>,
    token: &str,
    body: SimLeaveGroupRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;

    let chat_key = body.chat_id.to_string();
    let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &chat_key)? else {
        return Err(ApiError::not_found("chat not found"));
    };
    if sim_chat.chat_type == "private" {
        return Err(ApiError::bad_request("cannot leave private chat"));
    }

    let current_status: Option<String> = conn
        .query_row(
            "SELECT status FROM sim_chat_members WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
            params![bot.id, chat_key, user.id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some(status) = current_status.clone() else {
        return Ok(json!({
            "left": false,
            "reason": "not_member",
            "chat_id": body.chat_id,
            "user_id": user.id
        }));
    };
    if status == "owner" {
        return Err(ApiError::bad_request("owner cannot leave simulated group"));
    }
    if status == "left" {
        return Ok(json!({
            "left": false,
            "reason": "already_left",
            "chat_id": body.chat_id,
            "user_id": user.id
        }));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chat_members
         SET status = 'left', role = 'left', updated_at = ?4
         WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
        params![bot.id, chat_key, user.id, now],
    )
    .map_err(ApiError::internal)?;

    let from_user = build_user_from_sim_record(&user, false);
    let chat = chat_from_sim_record(&sim_chat, &user);
    let old_member = chat_member_from_status(&status, &from_user)?;
    let new_member = chat_member_from_status("left", &from_user)?;

    let update = Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: Some(ChatMemberUpdated {
            chat: chat.clone(),
            from: from_user.clone(),
            date: now,
            old_chat_member: old_member,
            new_chat_member: new_member,
            invite_link: None,
            via_join_request: None,
            via_chat_folder_invite_link: None,
        }),
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    };
    persist_and_dispatch_update(
        state,
        &mut conn,
        token,
        bot.id,
        serde_json::to_value(update).map_err(ApiError::internal)?,
    )?;

    if sim_chat.chat_type != "channel" {
        let mut service_fields = Map::<String, Value>::new();
        service_fields.insert(
            "left_chat_member".to_string(),
            serde_json::to_value(from_user.clone()).map_err(ApiError::internal)?,
        );
        emit_service_message_update(
            state,
            &mut conn,
            token,
            bot.id,
            &chat_key,
            &chat,
            &from_user,
            now,
            service_text_left_chat_member(&from_user, &from_user),
            service_fields,
        )?;
    }

    Ok(json!({
        "left": true,
        "chat_id": body.chat_id,
        "user_id": user.id
    }))
}

pub fn handle_sim_set_bot_group_membership(
    state: &Data<AppState>,
    token: &str,
    body: SimSetBotGroupMembershipRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let chat_key = body.chat_id.to_string();
    let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &chat_key)? else {
        return Err(ApiError::not_found("group not found"));
    };
    if sim_chat.chat_type == "private" {
        return Err(ApiError::bad_request("cannot update bot membership in private chat"));
    }

    let Some(target_status) = normalize_group_membership_status(&body.status) else {
        return Err(ApiError::bad_request(
            "status must be one of: member, admin, administrator, left, remove",
        ));
    };

    let actor_id = body.actor_user_id.unwrap_or(bot.id);
    let actor = if actor_id == bot.id {
        build_bot_user(&bot)
    } else {
        let actor_record = get_or_create_user(
            &mut conn,
            Some(actor_id),
            body.actor_first_name,
            body.actor_username,
        )?;
        build_user_from_sim_record(&actor_record, false)
    };

    let actor_status = load_chat_member_status(&mut conn, bot.id, &chat_key, actor.id)?;
    if !actor_status
        .as_deref()
        .map(is_group_admin_or_owner_status)
        .unwrap_or(false)
    {
        return Err(ApiError::bad_request(
            "only owner or admin can update bot membership",
        ));
    }

    let old_status = load_chat_member_status(&mut conn, bot.id, &chat_key, bot.id)?
        .unwrap_or_else(|| "left".to_string());

    if old_status == target_status {
        return Ok(json!({
            "changed": false,
            "chat_id": body.chat_id,
            "status": target_status,
        }));
    }

    let now = Utc::now().timestamp();
    let channel_admin_rights_json = if sim_chat.chat_type == "channel" {
        Some(serde_json::to_string(&channel_admin_rights_full_access()).map_err(ApiError::internal)?)
    } else {
        None
    };
    match target_status {
        "admin" => {
            conn.execute(
                "INSERT INTO sim_chat_members (bot_id, chat_key, user_id, status, role, admin_rights_json, joined_at, updated_at)
                 VALUES (?1, ?2, ?3, 'admin', 'admin', ?4, ?5, ?5)
                 ON CONFLICT(bot_id, chat_key, user_id)
                 DO UPDATE SET status = 'admin', role = 'admin', admin_rights_json = excluded.admin_rights_json, joined_at = COALESCE(sim_chat_members.joined_at, excluded.joined_at), updated_at = excluded.updated_at",
                params![bot.id, &chat_key, bot.id, channel_admin_rights_json, now],
            )
            .map_err(ApiError::internal)?;
        }
        "member" => {
            conn.execute(
                "INSERT INTO sim_chat_members (bot_id, chat_key, user_id, status, role, joined_at, updated_at)
                 VALUES (?1, ?2, ?3, 'member', 'member', ?4, ?4)
                 ON CONFLICT(bot_id, chat_key, user_id)
                 DO UPDATE SET status = 'member', role = 'member', admin_rights_json = NULL, joined_at = COALESCE(sim_chat_members.joined_at, excluded.joined_at), updated_at = excluded.updated_at",
                params![bot.id, &chat_key, bot.id, now],
            )
            .map_err(ApiError::internal)?;
        }
        _ => {
            conn.execute(
                "INSERT INTO sim_chat_members (bot_id, chat_key, user_id, status, role, joined_at, updated_at)
                 VALUES (?1, ?2, ?3, 'left', 'left', NULL, ?4)
                 ON CONFLICT(bot_id, chat_key, user_id)
                 DO UPDATE SET status = 'left', role = 'left', admin_rights_json = NULL, updated_at = excluded.updated_at",
                params![bot.id, &chat_key, bot.id, now],
            )
            .map_err(ApiError::internal)?;
        }
    }

    let bot_user = build_bot_user(&bot);
    let chat = Chat {
        id: sim_chat.chat_id,
        r#type: sim_chat.chat_type.clone(),
        title: sim_chat.title.clone(),
        username: sim_chat.username.clone(),
        first_name: None,
        last_name: None,
        is_forum: if sim_chat.chat_type == "supergroup" {
            Some(sim_chat.is_forum)
        } else {
            None
        },
        is_direct_messages: None,
    };

    let old_chat_member = chat_member_from_status(&old_status, &bot_user)?;
    let new_chat_member = chat_member_from_status(target_status, &bot_user)?;
    let membership_update = Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: Some(ChatMemberUpdated {
            chat: chat.clone(),
            from: actor.clone(),
            date: now,
            old_chat_member,
            new_chat_member,
            invite_link: None,
            via_join_request: None,
            via_chat_folder_invite_link: None,
        }),
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    };
    persist_and_dispatch_update(
        state,
        &mut conn,
        token,
        bot.id,
        serde_json::to_value(membership_update).map_err(ApiError::internal)?,
    )?;

    let old_active = is_active_chat_member_status(&old_status);
    let new_active = is_active_chat_member_status(target_status);
    if old_active != new_active && sim_chat.chat_type != "channel" {
        let mut service_fields = Map::<String, Value>::new();
        let text = if new_active {
            service_fields.insert(
                "new_chat_members".to_string(),
                serde_json::to_value(vec![bot_user.clone()]).map_err(ApiError::internal)?,
            );
            service_text_new_chat_members(&actor, std::slice::from_ref(&bot_user))
        } else {
            service_fields.insert(
                "left_chat_member".to_string(),
                serde_json::to_value(bot_user.clone()).map_err(ApiError::internal)?,
            );
            service_text_left_chat_member(&actor, &bot_user)
        };

        emit_service_message_update(
            state,
            &mut conn,
            token,
            bot.id,
            &chat_key,
            &chat,
            &actor,
            now,
            text,
            service_fields,
        )?;
    }

    Ok(json!({
        "changed": true,
        "chat_id": body.chat_id,
        "status": target_status,
    }))
}

// --- Forum Topics Begin ---

pub fn forum_topic_default_icon_color() -> i64 {
    0x6FB9F0
}

pub fn is_allowed_forum_topic_icon_color(value: i64) -> bool {
    matches!(
        value,
        0x6FB9F0 | 0xFFD67E | 0xCB86DB | 0x8EEE98 | 0xFF93B2 | 0xFB6F5F
    )
}

pub fn resolve_forum_message_thread_for_chat_key(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    requested_message_thread_id: Option<i64>,
) -> Result<Option<i64>, ApiError> {
    if let Some(sim_chat) = chats::load_sim_chat_record(conn, bot_id, chat_key)? {
        return resolve_forum_message_thread_id(conn, bot_id, &sim_chat, requested_message_thread_id);
    }

    if requested_message_thread_id.is_some() {
        return Err(ApiError::bad_request(
            "message_thread_id is available only in forum supergroups",
        ));
    }

    Ok(None)
}

pub fn resolve_forum_message_thread_id(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    sim_chat: &SimChatRecord,
    requested_message_thread_id: Option<i64>,
) -> Result<Option<i64>, ApiError> {
    if channels::is_direct_messages_chat(sim_chat) {
        if requested_message_thread_id.is_some() {
            return Err(ApiError::bad_request(
                "message_thread_id is not available in channel direct messages chats",
            ));
        }
        return Ok(None);
    }

    if sim_chat.chat_type != "supergroup" || !sim_chat.is_forum {
        if requested_message_thread_id.is_some() {
            return Err(ApiError::bad_request(
                "message_thread_id is available only in forum supergroups",
            ));
        }
        return Ok(None);
    }

    let thread_id = requested_message_thread_id.unwrap_or(1);
    if thread_id <= 0 {
        return Err(ApiError::bad_request("message_thread_id is invalid"));
    }

    if thread_id == 1 {
        let (_, is_closed, is_hidden) = ensure_general_forum_topic_state(conn, bot_id, &sim_chat.chat_key)?;
        if is_closed {
            return Err(ApiError::bad_request("general forum topic is closed"));
        }
        if is_hidden {
            return Err(ApiError::bad_request("general forum topic is hidden"));
        }
        return Ok(Some(thread_id));
    }

    let topic_is_closed: Option<i64> = conn
        .query_row(
            "SELECT is_closed FROM forum_topics
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_thread_id = ?3",
            params![bot_id, &sim_chat.chat_key, thread_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if topic_is_closed.is_none() {
        return Err(ApiError::not_found("forum topic not found"));
    }

    if topic_is_closed.unwrap_or_default() == 1 {
        return Err(ApiError::bad_request("forum topic is closed"));
    }

    Ok(Some(thread_id))
}

pub fn collect_message_ids_for_thread(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    message_thread_id: i64,
) -> Result<Vec<i64>, ApiError> {
    let chat_id = chat_key
        .parse::<i64>()
        .unwrap_or_else(|_| fallback_chat_id(chat_key));

    let value_as_i64 = |value: Option<&Value>| -> Option<i64> {
        value.and_then(|raw| {
            raw.as_i64()
                .or_else(|| raw.as_str().and_then(|text| text.trim().parse::<i64>().ok()))
        })
    };

    let mut thread_message_ids = HashSet::<i64>::new();
    let mut update_stmt = conn
        .prepare(
            "SELECT update_json
             FROM updates
             WHERE bot_id = ?1
             ORDER BY update_id DESC",
        )
        .map_err(ApiError::internal)?;
    let update_rows = update_stmt
        .query_map(params![bot_id], |row| row.get::<_, String>(0))
        .map_err(ApiError::internal)?;

    for row in update_rows {
        let raw = row.map_err(ApiError::internal)?;
        let Ok(update_value) = serde_json::from_str::<Value>(&raw) else {
            continue;
        };

        for field in [
            "edited_business_message",
            "business_message",
            "edited_channel_post",
            "channel_post",
            "edited_message",
            "message",
        ] {
            let Some(message_obj) = update_value.get(field).and_then(Value::as_object) else {
                continue;
            };

            let belongs_to_chat = value_as_i64(
                message_obj
                    .get("chat")
                    .and_then(Value::as_object)
                    .and_then(|chat| chat.get("id")),
            ) == Some(chat_id);
            if !belongs_to_chat {
                continue;
            }

            let belongs_to_thread = value_as_i64(message_obj.get("message_thread_id"))
                == Some(message_thread_id)
                || value_as_i64(
                    message_obj
                        .get("direct_messages_topic")
                        .and_then(Value::as_object)
                        .and_then(|topic| topic.get("topic_id")),
                ) == Some(message_thread_id);
            if !belongs_to_thread {
                continue;
            }

            if let Some(message_id) = value_as_i64(message_obj.get("message_id")) {
                thread_message_ids.insert(message_id);
            }
        }
    }
    drop(update_stmt);

    if thread_message_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut stmt = conn
        .prepare(
            "SELECT message_id
             FROM messages
             WHERE bot_id = ?1 AND chat_key = ?2
             ORDER BY message_id ASC",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![bot_id, chat_key], |row| row.get::<_, i64>(0))
        .map_err(ApiError::internal)?;

    let message_ids: Vec<i64> = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    drop(stmt);

    let mut result = Vec::new();
    for message_id in message_ids {
        if thread_message_ids.contains(&message_id) {
            result.push(message_id);
        }
    }

    Ok(result)
}

pub fn ensure_general_forum_topic_state(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
) -> Result<(String, bool, bool), ApiError> {
    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO forum_topic_general_states (bot_id, chat_key, name, is_closed, is_hidden, updated_at)
         VALUES (?1, ?2, 'General', 0, 0, ?3)
         ON CONFLICT(bot_id, chat_key) DO NOTHING",
        params![bot_id, chat_key, now],
    )
    .map_err(ApiError::internal)?;

    conn.query_row(
        "SELECT name, is_closed, is_hidden FROM forum_topic_general_states
         WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot_id, chat_key],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)? == 1,
                row.get::<_, i64>(2)? == 1,
            ))
        },
    )
    .map_err(ApiError::internal)
}

pub fn load_forum_topic(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    message_thread_id: i64,
) -> Result<Option<ForumTopic>, ApiError> {
    conn.query_row(
        "SELECT name, icon_color, icon_custom_emoji_id
         FROM forum_topics
         WHERE bot_id = ?1 AND chat_key = ?2 AND message_thread_id = ?3",
        params![bot_id, chat_key, message_thread_id],
        |row| {
            Ok(ForumTopic {
                message_thread_id,
                name: row.get(0)?,
                icon_color: row.get(1)?,
                icon_custom_emoji_id: row.get(2)?,
                is_name_implicit: None,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

// --- Links ---

pub fn handle_sim_create_group_invite_link(
    state: &Data<AppState>,
    token: &str,
    body: SimCreateGroupInviteLinkRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let chat_key = body.chat_id.to_string();
    let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &chat_key)? else {
        return Err(ApiError::not_found("group not found"));
    };
    if sim_chat.chat_type == "private" {
        return Err(ApiError::bad_request("cannot create invite link for private chat"));
    }

    let actor_id = body.user_id.unwrap_or(bot.id);
    let actor_user = if actor_id == bot.id {
        build_bot_user(&bot)
    } else {
        let actor_record = get_or_create_user(
            &mut conn,
            Some(actor_id),
            body.actor_first_name,
            body.actor_username,
        )?;
        build_user_from_sim_record(&actor_record, false)
    };

    let actor_status = load_chat_member_status(&mut conn, bot.id, &chat_key, actor_user.id)?;
    if sim_chat.chat_type == "channel" {
        let can_manage = match actor_status.as_deref() {
            Some("owner") => true,
            Some("admin") => {
                let rights = load_chat_member_record(&mut conn, bot.id, &chat_key, actor_user.id)?
                    .map(|record| parse_channel_admin_rights_json(record.admin_rights_json.as_deref()))
                    .unwrap_or_else(channel_admin_rights_full_access);
                rights.can_manage_chat || rights.can_invite_users
            }
            _ => false,
        };

        if !can_manage {
            return Err(ApiError::bad_request(
                "only channel owner/admin with invite rights can create invite link",
            ));
        }
    } else {
        let can_invite_by_role = actor_status
            .as_deref()
            .map(is_group_admin_or_owner_status)
            .unwrap_or(false);
        let can_invite_by_permission = if actor_status.as_deref() == Some("member") {
            load_group_runtime_settings(&mut conn, bot.id, &chat_key)?
                .map(|settings| settings.permissions.can_invite_users.unwrap_or(true))
                .unwrap_or(false)
        } else {
            false
        };
        if !(can_invite_by_role || can_invite_by_permission) {
            return Err(ApiError::bad_request(
                "only owner/admin or members with can_invite_users can create invite link",
            ));
        }
    }

    let mut invite_link = generate_sim_invite_link();
    loop {
        let exists: Option<String> = conn
            .query_row(
                "SELECT invite_link FROM sim_chat_invite_links WHERE bot_id = ?1 AND invite_link = ?2",
                params![bot.id, &invite_link],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;
        if exists.is_none() {
            break;
        }
        invite_link = generate_sim_invite_link();
    }

    let now = Utc::now().timestamp();
    let creates_join_request = body.creates_join_request.unwrap_or(false);
    let name = body
        .name
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let expire_date = body.expire_date.filter(|v| *v > now);
    let member_limit = body.member_limit.filter(|v| *v > 0);

    conn.execute(
        "INSERT INTO sim_chat_invite_links
         (bot_id, chat_key, invite_link, creator_user_id, creates_join_request, is_primary, is_revoked, name, expire_date, member_limit, subscription_period, subscription_price, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, 0, 0, ?6, ?7, ?8, NULL, NULL, ?9, ?9)",
        params![
            bot.id,
            &chat_key,
            &invite_link,
            actor_user.id,
            if creates_join_request { 1 } else { 0 },
            name,
            expire_date,
            member_limit,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    let record = SimChatInviteLinkRecord {
        invite_link,
        creator_user_id: actor_user.id,
        creates_join_request,
        is_primary: false,
        is_revoked: false,
        name,
        expire_date,
        member_limit,
        subscription_period: None,
        subscription_price: None,
    };

    let pending_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sim_chat_join_requests
             WHERE bot_id = ?1 AND chat_key = ?2 AND invite_link = ?3 AND status = 'pending'",
            params![bot.id, &chat_key, &record.invite_link],
            |row| row.get(0),
        )
        .map_err(ApiError::internal)?;

    let invite = chat_invite_link_from_record(actor_user, &record, Some(pending_count));
    serde_json::to_value(invite).map_err(ApiError::internal)
}

pub fn handle_sim_join_group_by_invite_link(
    state: &Data<AppState>,
    token: &str,
    body: SimJoinGroupByInviteLinkRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let invite_link = body.invite_link.trim().to_string();
    if invite_link.is_empty() {
        return Err(ApiError::bad_request("invite_link is empty"));
    }

    let link_row: Option<(String, i64, i64, i64, Option<String>, Option<i64>, Option<i64>, Option<i64>, Option<i64>)> = conn
        .query_row(
            "SELECT chat_key, creator_user_id, creates_join_request, is_primary, name, expire_date, member_limit, subscription_period, subscription_price
             FROM sim_chat_invite_links
             WHERE bot_id = ?1 AND invite_link = ?2 AND is_revoked = 0",
            params![bot.id, &invite_link],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                ))
            },
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((chat_key, creator_user_id, creates_join_request_raw, is_primary_raw, name, expire_date, member_limit, subscription_period, subscription_price)) = link_row else {
        return Err(ApiError::not_found("invite link not found"));
    };

    let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &chat_key)? else {
        return Err(ApiError::not_found("chat not found"));
    };
    if sim_chat.chat_type == "private" {
        return Err(ApiError::bad_request("cannot join private chat by invite link"));
    }

    let now = Utc::now().timestamp();
    if let Some(expire_at) = expire_date {
        if expire_at <= now {
            return Err(ApiError::bad_request("invite link expired"));
        }
    }

    let user = get_or_create_user(&mut conn, body.user_id, body.first_name, body.username)?;
    let current_status = load_chat_member_status(&mut conn, bot.id, &chat_key, user.id)?;
    if current_status
        .as_deref()
        .map(is_active_chat_member_status)
        .unwrap_or(false)
    {
        return Ok(json!({
            "joined": false,
            "reason": "already_member",
            "chat_id": sim_chat.chat_id,
            "user_id": user.id,
        }));
    }
    if current_status.as_deref() == Some("banned") {
        return Err(ApiError::bad_request("user is banned in this chat"));
    }

    if let Some(limit) = member_limit {
        let active_members: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sim_chat_members
                 WHERE bot_id = ?1 AND chat_key = ?2 AND status IN ('owner','admin','member')",
                params![bot.id, &chat_key],
                |row| row.get(0),
            )
            .map_err(ApiError::internal)?;
        if active_members >= limit {
            return Err(ApiError::bad_request("invite link member limit reached"));
        }
    }

    let creator = if creator_user_id == bot.id {
        build_bot_user(&bot)
    } else if let Some(record) = load_sim_user_record(&mut conn, creator_user_id)? {
        build_user_from_sim_record(&record, false)
    } else {
        build_user_from_sim_record(
            &ensure_user(
                &mut conn,
                Some(creator_user_id),
                Some(format!("User {}", creator_user_id)),
                None,
            )?,
            false,
        )
    };

    let invite_record = SimChatInviteLinkRecord {
        invite_link: invite_link.clone(),
        creator_user_id,
        creates_join_request: creates_join_request_raw == 1,
        is_primary: is_primary_raw == 1,
        is_revoked: false,
        name,
        expire_date,
        member_limit,
        subscription_period,
        subscription_price,
    };
    let pending_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sim_chat_join_requests
             WHERE bot_id = ?1 AND chat_key = ?2 AND invite_link = ?3 AND status = 'pending'",
            params![bot.id, &chat_key, &invite_link],
            |row| row.get(0),
        )
        .map_err(ApiError::internal)?;
    let invite = chat_invite_link_from_record(creator, &invite_record, Some(pending_count));

    if invite.creates_join_request {
        conn.execute(
            "INSERT INTO sim_chat_join_requests
             (bot_id, chat_key, user_id, invite_link, status, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, 'pending', ?5, ?5)
             ON CONFLICT(bot_id, chat_key, user_id)
             DO UPDATE SET invite_link = excluded.invite_link, status = 'pending', updated_at = excluded.updated_at",
            params![bot.id, &chat_key, user.id, &invite_link, now],
        )
        .map_err(ApiError::internal)?;

        let from_user = build_user_from_sim_record(&user, false);
        let chat = chat_from_sim_record(&sim_chat, &user);

        let join_request_update = Update {
            update_id: 0,
            message: None,
            edited_message: None,
            channel_post: None,
            edited_channel_post: None,
            business_connection: None,
            business_message: None,
            edited_business_message: None,
            deleted_business_messages: None,
            message_reaction: None,
            message_reaction_count: None,
            inline_query: None,
            chosen_inline_result: None,
            callback_query: None,
            shipping_query: None,
            pre_checkout_query: None,
            purchased_paid_media: None,
            poll: None,
            poll_answer: None,
            my_chat_member: None,
            chat_member: None,
            chat_join_request: Some(ChatJoinRequest {
                chat,
                from: from_user,
                user_chat_id: build_sim_user_chat_id(sim_chat.chat_id, user.id),
                date: now,
                bio: None,
                invite_link: Some(invite),
            }),
            chat_boost: None,
            removed_chat_boost: None,
        managed_bot: None,
        };
        persist_and_dispatch_update(
            state,
            &mut conn,
            token,
            bot.id,
            serde_json::to_value(join_request_update).map_err(ApiError::internal)?,
        )?;

        return Ok(json!({
            "joined": false,
            "pending": true,
            "chat_id": sim_chat.chat_id,
            "chat_type": sim_chat.chat_type,
            "user_id": user.id,
        }));
    }

    join_user_to_group(
        state,
        &mut conn,
        token,
        bot.id,
        &sim_chat,
        &user,
        current_status.as_deref(),
        Some(invite),
        None,
    )?;

    Ok(json!({
        "joined": true,
        "pending": false,
        "chat_id": sim_chat.chat_id,
        "chat_type": sim_chat.chat_type,
        "user_id": user.id,
    }))
}