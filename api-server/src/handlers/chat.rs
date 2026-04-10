use super::*;
use crate::generated::methods::{
    ApproveChatJoinRequestRequest, BanChatMemberRequest, BanChatSenderChatRequest,
    DeleteChatPhotoRequest, DeleteChatStickerSetRequest, DeclineChatJoinRequestRequest,
    GetChatRequest, GetChatAdministratorsRequest, GetChatMemberCountRequest, GetChatMemberRequest,
    GetChatMenuButtonRequest, LeaveChatRequest,
    PinChatMessageRequest, PromoteChatMemberRequest, RemoveChatVerificationRequest,
    RestrictChatMemberRequest, SetChatAdministratorCustomTitleRequest,
    SetChatDescriptionRequest, SetChatMemberTagRequest, SetChatPhotoRequest, SetChatPermissionsRequest,
    SetChatStickerSetRequest, SetChatMenuButtonRequest, SetChatTitleRequest,
    UnbanChatMemberRequest, UnbanChatSenderChatRequest, UnpinAllChatMessagesRequest,
    UnpinChatMessageRequest, VerifyChatRequest,
};

pub fn handle_approve_chat_join_request(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: ApproveChatJoinRequestRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;
    if sim_chat.chat_type == "channel" {
        let actor_user_id = current_request_actor_user_id().unwrap_or(bot.id);
        ensure_channel_actor_can_manage_invite_links(&mut conn, bot.id, &chat_key, actor_user_id)?;
    }

    let request_row: Option<(Option<String>, String)> = conn
        .query_row(
            "SELECT invite_link, status
             FROM sim_chat_join_requests
             WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
            params![bot.id, &chat_key, request.user_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;
    let Some((invite_link, status)) = request_row else {
        return Err(ApiError::not_found("join request not found"));
    };
    if status != "pending" {
        return Err(ApiError::bad_request("join request is not pending"));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chat_join_requests
         SET status = 'approved', updated_at = ?4
         WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
        params![bot.id, &chat_key, request.user_id, now],
    )
    .map_err(ApiError::internal)?;

    let current_status = load_chat_member_status(&mut conn, bot.id, &chat_key, request.user_id)?;
    if current_status
        .as_deref()
        .map(is_active_chat_member_status)
        .unwrap_or(false)
    {
        return Ok(Value::Bool(true));
    }
    if current_status.as_deref() == Some("banned") {
        return Err(ApiError::bad_request("user is banned in this chat"));
    }

    let target_user = ensure_sim_user_record(&mut conn, request.user_id)?;
    let invite = if let Some(raw_link) = invite_link {
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

    join_user_to_group(
        state,
        &mut conn,
        token,
        bot.id,
        &sim_chat,
        &target_user,
        current_status.as_deref(),
        invite,
        Some(true),
    )?;

    Ok(Value::Bool(true))
}

pub fn handle_ban_chat_member(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: BanChatMemberRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    if sim_chat.chat_type == "channel" {
        return Err(ApiError::bad_request("channel members do not support tags"));
    }
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    if request.user_id == bot.id {
        return Err(ApiError::bad_request("bot can't ban itself"));
    }

    let old_record = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)?;
    let old_status = old_record
        .as_ref()
        .map(|record| record.status.clone())
        .unwrap_or_else(|| "left".to_string());

    if old_status == "owner" {
        return Err(ApiError::bad_request("can't ban chat owner"));
    }
    if old_status == "banned" {
        return Ok(json!(true));
    }

    let target_record = ensure_sim_user_record(&mut conn, request.user_id)?;
    let now = Utc::now().timestamp();
    upsert_chat_member_record(
        &mut conn,
        bot.id,
        &chat_key,
        request.user_id,
        "banned",
        "banned",
        None,
        None,
        request.until_date,
        None,
        None,
        now,
    )?;

    let new_record = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)?;

    let chat = build_chat_from_group_record(&sim_chat);
    let target = build_user_from_sim_record(&target_record, false);

    emit_chat_member_transition_update_with_records(
        state,
        &mut conn,
        token,
        bot.id,
        &chat,
        &actor,
        &target,
        &old_status,
        "banned",
        old_record.as_ref(),
        new_record.as_ref(),
        now,
    )?;

    if is_active_chat_member_status(old_status.as_str()) {
        let mut service_fields = Map::<String, Value>::new();
        service_fields.insert(
            "left_chat_member".to_string(),
            serde_json::to_value(target.clone()).map_err(ApiError::internal)?,
        );
        emit_service_message_update(
            state,
            &mut conn,
            token,
            bot.id,
            &chat_key,
            &chat,
            &actor,
            now,
            service_text_left_chat_member(&actor, &target),
            service_fields,
        )?;
    }

    Ok(json!(true))
}

pub fn handle_ban_chat_sender_chat(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: BanChatSenderChatRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_can_manage_sender_chat_in_linked_context(
        &mut conn,
        &bot,
        &chat_key,
        &sim_chat,
    )?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_banned_sender_chats (bot_id, chat_key, sender_chat_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?4)
         ON CONFLICT(bot_id, chat_key, sender_chat_id)
         DO UPDATE SET updated_at = excluded.updated_at",
        params![bot.id, &chat_key, request.sender_chat_id, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_delete_chat_photo(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: DeleteChatPhotoRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    let had_photo: Option<String> = conn
        .query_row(
            "SELECT photo_file_id FROM sim_chats WHERE bot_id = ?1 AND chat_key = ?2",
            params![bot.id, &chat_key],
            |row| row.get(0),
        )
        .map_err(ApiError::internal)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chats
         SET photo_file_id = NULL, updated_at = ?1
         WHERE bot_id = ?2 AND chat_key = ?3",
        params![now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    if had_photo.is_some() {
        let chat = build_chat_from_group_record(&sim_chat);
        let mut service_fields = Map::<String, Value>::new();
        service_fields.insert("delete_chat_photo".to_string(), Value::Bool(true));
        emit_service_message_update(
            state,
            &mut conn,
            token,
            bot.id,
            &chat_key,
            &chat,
            &actor,
            now,
            format!("{} deleted the group photo", display_name_for_service_user(&actor)),
            service_fields,
        )?;
    }

    Ok(json!(true))
}

pub fn handle_delete_chat_sticker_set(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: DeleteChatStickerSetRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;

    if sim_chat.chat_type != "supergroup" {
        return Err(ApiError::bad_request("sticker set can only be removed for supergroups"));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chats
         SET sticker_set_name = NULL, updated_at = ?1
         WHERE bot_id = ?2 AND chat_key = ?3",
        params![now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_decline_chat_join_request(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: DeclineChatJoinRequestRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;
    if sim_chat.chat_type == "channel" {
        let actor_user_id = current_request_actor_user_id().unwrap_or(bot.id);
        ensure_channel_actor_can_manage_invite_links(&mut conn, bot.id, &chat_key, actor_user_id)?;
    }

    let status: Option<String> = conn
        .query_row(
            "SELECT status
             FROM sim_chat_join_requests
             WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
            params![bot.id, &chat_key, request.user_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some(current_status) = status else {
        return Err(ApiError::not_found("join request not found"));
    };
    if current_status != "pending" {
        return Err(ApiError::bad_request("join request is not pending"));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chat_join_requests
         SET status = 'declined', updated_at = ?4
         WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
        params![bot.id, &chat_key, request.user_id, now],
    )
    .map_err(ApiError::internal)?;

    Ok(Value::Bool(true))
}

pub fn handle_get_chat(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetChatRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let requested_chat_key = value_to_chat_key(&request.chat_id)?;
    let requested_chat_id = chat_id_as_i64(&request.chat_id, &requested_chat_key);
    let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &requested_chat_key)?
        .or(load_sim_chat_record_by_chat_id(&mut conn, bot.id, requested_chat_id)?) else {
        return Err(ApiError::not_found("chat not found"));
    };
    let chat_key = sim_chat.chat_key.clone();
    let is_direct_messages = is_direct_messages_chat(&sim_chat);

    if sim_chat.chat_type != "private" {
        if is_direct_messages {
            let actor_user_id = current_request_actor_user_id().unwrap_or(bot.id);
            let parent_channel_chat_id = sim_chat
                .parent_channel_chat_id
                .ok_or_else(|| ApiError::bad_request("direct messages chat parent channel is missing"))?;
            ensure_channel_member_can_manage_direct_messages(
                &mut conn,
                bot.id,
                &parent_channel_chat_id.to_string(),
                actor_user_id,
            )?;
        } else {
            ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;
        }
    }

    let row: Option<(Option<String>, Option<String>, i64, i64, Option<String>, Option<String>, Option<i64>)> = conn
        .query_row(
            "SELECT description, permissions_json, message_history_visible, slow_mode_delay, photo_file_id, sticker_set_name, pinned_message_id
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
                ))
            },
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((description, permissions_json, message_history_visible, slow_mode_delay, photo_file_id, sticker_set_name, pinned_message_id)) = row else {
        return Err(ApiError::not_found("chat not found"));
    };

    let photo = if let Some(file_id) = photo_file_id {
        let file_unique_id: Option<String> = conn
            .query_row(
                "SELECT file_unique_id FROM files WHERE bot_id = ?1 AND file_id = ?2",
                params![bot.id, &file_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;
        let unique = file_unique_id.unwrap_or_else(|| file_id.clone());
        Some(ChatPhoto {
            small_file_id: file_id.clone(),
            small_file_unique_id: unique.clone(),
            big_file_id: file_id,
            big_file_unique_id: unique,
        })
    } else {
        None
    };

    let latest_pinned_message_id = load_latest_pinned_message_id(&mut conn, bot.id, &chat_key)?
        .or(pinned_message_id);

    let pinned_message = if let Some(message_id) = latest_pinned_message_id {
        load_message_value(&mut conn, &bot, message_id)
            .ok()
            .and_then(|value| serde_json::from_value::<Message>(value).ok())
    } else {
        None
    };

    let permissions = if sim_chat.chat_type == "private" || is_direct_messages {
        None
    } else {
        Some(
            permissions_json
                .as_deref()
                .and_then(|raw| serde_json::from_str::<ChatPermissions>(raw).ok())
                .unwrap_or_else(default_group_permissions),
        )
    };

    let primary_invite_row: Option<(String, i64)> = conn
        .query_row(
            "SELECT invite_link, creates_join_request
             FROM sim_chat_invite_links
             WHERE bot_id = ?1 AND chat_key = ?2 AND is_primary = 1 AND is_revoked = 0
             ORDER BY updated_at DESC
             LIMIT 1",
            params![bot.id, &chat_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let linked_chat_id = if is_direct_messages {
        None
    } else {
        resolve_linked_chat_id_for_chat(&mut conn, bot.id, &sim_chat)?
    };
    let parent_chat = if is_direct_messages {
        let parent_channel_chat_id = sim_chat.parent_channel_chat_id.unwrap_or_default();
        let parent_channel_chat = load_sim_chat_record(&mut conn, bot.id, &parent_channel_chat_id.to_string())?
            .ok_or_else(|| ApiError::not_found("parent channel not found"))?;
        Some(build_chat_from_group_record(&parent_channel_chat))
    } else {
        None
    };

    let chat_full = ChatFullInfo {
        id: sim_chat.chat_id,
        r#type: sim_chat.chat_type.clone(),
        title: sim_chat.title.clone(),
        username: sim_chat.username.clone(),
        first_name: None,
        last_name: None,
        is_forum: if sim_chat.chat_type == "supergroup" && !is_direct_messages {
            Some(sim_chat.is_forum)
        } else {
            None
        },
        is_direct_messages: if is_direct_messages { Some(true) } else { None },
        accent_color_id: 0,
        max_reaction_count: 11,
        photo,
        active_usernames: None,
        birthdate: None,
        business_intro: None,
        business_location: None,
        business_opening_hours: None,
        personal_chat: None,
        parent_chat,
        available_reactions: None,
        background_custom_emoji_id: None,
        profile_accent_color_id: None,
        profile_background_custom_emoji_id: None,
        emoji_status_custom_emoji_id: None,
        emoji_status_expiration_date: None,
        bio: None,
        has_private_forwards: None,
        has_restricted_voice_and_video_messages: None,
        join_to_send_messages: None,
        join_by_request: primary_invite_row.as_ref().map(|(_, creates_join_request)| *creates_join_request == 1),
        description,
        invite_link: primary_invite_row.as_ref().map(|(invite_link, _)| invite_link.clone()),
        pinned_message,
        permissions,
        accepted_gift_types: AcceptedGiftTypes {
            unlimited_gifts: true,
            limited_gifts: true,
            unique_gifts: true,
            premium_subscription: true,
            gifts_from_channels: true,
        },
        can_send_paid_media: None,
        slow_mode_delay: if sim_chat.chat_type == "private" || is_direct_messages {
            None
        } else {
            Some(slow_mode_delay.max(0))
        },
        unrestrict_boost_count: None,
        message_auto_delete_time: None,
        has_aggressive_anti_spam_enabled: None,
        has_hidden_members: None,
        has_protected_content: None,
        has_visible_history: if sim_chat.chat_type == "private" || is_direct_messages {
            None
        } else {
            Some(message_history_visible != 0)
        },
        sticker_set_name,
        can_set_sticker_set: if sim_chat.chat_type == "supergroup" {
            Some(true)
        } else {
            None
        },
        custom_emoji_sticker_set_name: None,
        linked_chat_id,
        location: None,
        rating: None,
        first_profile_audio: None,
        unique_gift_colors: None,
        paid_message_star_count: None,
    };

    let mut chat_value = serde_json::to_value(chat_full).map_err(ApiError::internal)?;
    let verification_description =
        load_chat_verification_description(&mut conn, bot.id, &chat_key)?;
    if let Some(object) = chat_value.as_object_mut() {
        object.insert(
            "is_verified".to_string(),
            Value::Bool(verification_description.is_some()),
        );
        if let Some(description) = verification_description {
            object.insert(
                "verification_description".to_string(),
                Value::String(description),
            );
        }
    }

    Ok(chat_value)
}

pub fn handle_get_chat_administrators(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetChatAdministratorsRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;

    if sim_chat.chat_type == "private" {
        return Err(ApiError::bad_request("private chats do not have administrators list"));
    }

    let rows: Vec<(i64, SimChatMemberRecord)> = {
        let mut stmt = conn
            .prepare(
                "SELECT user_id, status, role, permissions_json, admin_rights_json, until_date, custom_title, tag, joined_at
                 FROM sim_chat_members
                 WHERE bot_id = ?1 AND chat_key = ?2 AND status IN ('owner','admin')
                 ORDER BY CASE status WHEN 'owner' THEN 0 WHEN 'admin' THEN 1 ELSE 2 END, user_id ASC",
            )
            .map_err(ApiError::internal)?;

        let mapped = stmt
            .query_map(params![bot.id, &chat_key], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    SimChatMemberRecord {
                        status: row.get(1)?,
                        role: row.get(2)?,
                        permissions_json: row.get(3)?,
                        admin_rights_json: row.get(4)?,
                        until_date: row.get(5)?,
                        custom_title: row.get(6)?,
                        tag: row.get(7)?,
                        joined_at: row.get(8)?,
                    },
                ))
            })
            .map_err(ApiError::internal)?;

        let mut collected: Vec<(i64, SimChatMemberRecord)> = Vec::new();
        for row in mapped {
            collected.push(row.map_err(ApiError::internal)?);
        }
        collected
    };

    let mut admins: Vec<ChatMember> = Vec::new();
    for (user_id, record) in rows {
        if user_id == bot.id {
            continue;
        }
        let user = build_user_from_sim_record(&ensure_sim_user_record(&mut conn, user_id)?, false);
        admins.push(chat_member_from_record(&record, &user, sim_chat.chat_type.as_str())?);
    }

    serde_json::to_value(admins).map_err(ApiError::internal)
}

pub fn handle_get_chat_member_count(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetChatMemberCountRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;

    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*)
             FROM sim_chat_members
             WHERE bot_id = ?1 AND chat_key = ?2 AND status IN ('owner','admin','member','restricted')",
            params![bot.id, &chat_key],
            |row| row.get(0),
        )
        .map_err(ApiError::internal)?;

    serde_json::to_value(count).map_err(ApiError::internal)
}

pub fn handle_get_chat_member(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetChatMemberRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;

    let member_record = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)?;
    let user = build_user_from_sim_record(&ensure_sim_user_record(&mut conn, request.user_id)?, false);

    let member = if let Some(record) = member_record {
        chat_member_from_record(&record, &user, sim_chat.chat_type.as_str())?
    } else {
        chat_member_from_status("left", &user)?
    };

    serde_json::to_value(member).map_err(ApiError::internal)
}

pub fn handle_get_chat_menu_button(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetChatMenuButtonRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    if let Some(chat_id) = request.chat_id {
        let chat_key = chat_id_to_chat_key(chat_id);
        if let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &chat_key)? {
            if sim_chat.chat_type != "private" {
                return Err(ApiError::bad_request("chat menu button is only available in private chats"));
            }
        } else if chat_id <= 0 {
            return Err(ApiError::bad_request("chat menu button is only available in private chats"));
        }
    }

    let scoped_key = menu_button_scope_key(request.chat_id);
    let mut stored: Option<String> = conn
        .query_row(
            "SELECT menu_button_json
             FROM bot_chat_menu_buttons
             WHERE bot_id = ?1 AND scope_key = ?2",
            params![bot.id, &scoped_key],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if stored.is_none() && request.chat_id.is_some() {
        stored = conn
            .query_row(
                "SELECT menu_button_json
                 FROM bot_chat_menu_buttons
                 WHERE bot_id = ?1 AND scope_key = 'default'",
                params![bot.id],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;
    }

    let menu_button = if let Some(raw) = stored {
        serde_json::from_str::<MenuButton>(&raw).unwrap_or_else(|_| default_menu_button())
    } else {
        default_menu_button()
    };

    serde_json::to_value(menu_button).map_err(ApiError::internal)
}

pub fn handle_leave_chat(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: LeaveChatRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;

    let old_record = load_chat_member_record(&mut conn, bot.id, &chat_key, bot.id)?;
    let Some(old_record) = old_record else {
        return Ok(json!(true));
    };
    let old_status = old_record.status.clone();

    if old_status == "left" || old_status == "banned" {
        return Ok(json!(true));
    }
    if old_status == "owner" {
        return Err(ApiError::bad_request("chat owner can't leave the chat"));
    }
    if !is_active_chat_member_status(old_status.as_str()) {
        return Ok(json!(true));
    }

    let now = Utc::now().timestamp();
    upsert_chat_member_record(
        &mut conn,
        bot.id,
        &chat_key,
        bot.id,
        "left",
        "left",
        None,
        None,
        None,
        None,
        None,
        now,
    )?;

    let new_record = load_chat_member_record(&mut conn, bot.id, &chat_key, bot.id)?;

    let chat = build_chat_from_group_record(&sim_chat);
    let bot_user = build_bot_user(&bot);

    emit_my_chat_member_transition_update_with_records(
        state,
        &mut conn,
        token,
        bot.id,
        &chat,
        &bot_user,
        &old_status,
        "left",
        Some(&old_record),
        new_record.as_ref(),
        now,
    )?;

    if sim_chat.chat_type != "channel" {
        let mut service_fields = Map::<String, Value>::new();
        service_fields.insert(
            "left_chat_member".to_string(),
            serde_json::to_value(bot_user.clone()).map_err(ApiError::internal)?,
        );
        emit_service_message_update(
            state,
            &mut conn,
            token,
            bot.id,
            &chat_key,
            &chat,
            &bot_user,
            now,
            service_text_left_chat_member(&bot_user, &bot_user),
            service_fields,
        )?;
    }

    Ok(json!(true))
}

pub fn handle_pin_chat_message(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: PinChatMessageRequest = parse_request(params)?;
    if request.business_connection_id.is_some() {
        return Err(ApiError::bad_request("business_connection_id is not supported in simulator"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    let exists: Option<i64> = conn
        .query_row(
            "SELECT message_id FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, &chat_key, request.message_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;
    if exists.is_none() {
        return Err(ApiError::bad_request("message to pin was not found"));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "DELETE FROM sim_chat_pinned_messages
         WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
        params![bot.id, &chat_key, request.message_id],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "INSERT INTO sim_chat_pinned_messages (bot_id, chat_key, message_id, pinned_at)
         VALUES (?1, ?2, ?3, ?4)",
        params![bot.id, &chat_key, request.message_id, now],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "UPDATE sim_chats
         SET pinned_message_id = ?1, updated_at = ?2
         WHERE bot_id = ?3 AND chat_key = ?4",
        params![request.message_id, now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    let chat = build_chat_from_group_record(&sim_chat);
    let pinned_message = load_message_value(&mut conn, &bot, request.message_id)?;
    let mut service_fields = Map::<String, Value>::new();
    service_fields.insert("pinned_message".to_string(), pinned_message);
    emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &actor,
        now,
        format!("{} pinned a message", display_name_for_service_user(&actor)),
        service_fields,
    )?;

    Ok(json!(true))
}

pub fn handle_promote_chat_member(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: PromoteChatMemberRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    let is_channel_chat = sim_chat.chat_type == "channel";
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    if request.user_id == bot.id {
        return Err(ApiError::bad_request("bot can't change its own admin role with promoteChatMember"));
    }

    let old_record = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)?;
    let old_status = old_record
        .as_ref()
        .map(|record| record.status.clone())
        .unwrap_or_else(|| "left".to_string());
    if old_status == "owner" {
        return Err(ApiError::bad_request("can't change chat owner role"));
    }

    let should_promote = request.is_anonymous.unwrap_or(false)
        || request.can_manage_chat.unwrap_or(false)
        || request.can_delete_messages.unwrap_or(false)
        || request.can_manage_video_chats.unwrap_or(false)
        || request.can_restrict_members.unwrap_or(false)
        || request.can_promote_members.unwrap_or(false)
        || request.can_change_info.unwrap_or(false)
        || request.can_invite_users.unwrap_or(false)
        || request.can_post_stories.unwrap_or(false)
        || request.can_edit_stories.unwrap_or(false)
        || request.can_delete_stories.unwrap_or(false)
        || request.can_post_messages.unwrap_or(false)
        || request.can_edit_messages.unwrap_or(false)
        || request.can_pin_messages.unwrap_or(false)
        || request.can_manage_topics.unwrap_or(false)
        || request.can_manage_direct_messages.unwrap_or(false)
        || request.can_manage_tags.unwrap_or(false);

    let new_status = if should_promote { "admin" } else { "member" };
    let desired_channel_admin_rights = if is_channel_chat && new_status == "admin" {
        Some(channel_admin_rights_from_promote_request(&request))
    } else {
        None
    };
    let channel_rights_changed = if is_channel_chat && old_status == "admin" && new_status == "admin" {
        let existing_rights = old_record
            .as_ref()
            .map(|record| parse_channel_admin_rights_json(record.admin_rights_json.as_deref()))
            .unwrap_or_else(channel_admin_rights_full_access);
        let desired_rights = desired_channel_admin_rights
            .clone()
            .unwrap_or_else(|| existing_rights.clone());
        existing_rights != desired_rights
    } else {
        false
    };

    if old_status == new_status && !channel_rights_changed {
        return Ok(json!(true));
    }

    let target_record = ensure_sim_user_record(&mut conn, request.user_id)?;
    let now = Utc::now().timestamp();
    let joined_at = old_record
        .as_ref()
        .and_then(|record| record.joined_at)
        .or(Some(now));

    let custom_title = if is_channel_chat {
        None
    } else if new_status == "admin" {
        old_record
            .as_ref()
            .and_then(|record| record.custom_title.as_deref())
    } else {
        None
    };
    let tag = if is_channel_chat {
        None
    } else {
        old_record.as_ref().and_then(|record| record.tag.as_deref())
    };

    upsert_chat_member_record(
        &mut conn,
        bot.id,
        &chat_key,
        request.user_id,
        new_status,
        new_status,
        joined_at,
        None,
        None,
        custom_title,
        tag,
        now,
    )?;

    if is_channel_chat {
        let admin_rights_json = if new_status == "admin" {
            let rights = desired_channel_admin_rights
                .clone()
                .or_else(|| {
                    old_record
                        .as_ref()
                        .map(|record| parse_channel_admin_rights_json(record.admin_rights_json.as_deref()))
                })
                .unwrap_or_else(channel_admin_rights_full_access);
            Some(serde_json::to_string(&rights).map_err(ApiError::internal)?)
        } else {
            None
        };
        conn.execute(
            "UPDATE sim_chat_members
             SET admin_rights_json = ?1
             WHERE bot_id = ?2 AND chat_key = ?3 AND user_id = ?4",
            params![admin_rights_json, bot.id, &chat_key, request.user_id],
        )
        .map_err(ApiError::internal)?;
    }

    let new_record = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)?;

    let chat = build_chat_from_group_record(&sim_chat);
    let target = build_user_from_sim_record(&target_record, false);

    emit_chat_member_transition_update_with_records(
        state,
        &mut conn,
        token,
        bot.id,
        &chat,
        &actor,
        &target,
        &old_status,
        new_status,
        old_record.as_ref(),
        new_record.as_ref(),
        now,
    )?;

    Ok(json!(true))
}

pub fn handle_remove_chat_verification(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: RemoveChatVerificationRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_verifications_storage(&mut conn)?;

    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    if is_direct_messages_chat(&sim_chat) {
        return Err(ApiError::bad_request(
            "verification is not supported for channel direct messages chats",
        ));
    }

    conn.execute(
        "DELETE FROM sim_chat_verifications WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, chat_key],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_restrict_chat_member(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: RestrictChatMemberRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    if sim_chat.chat_type == "channel" {
        return Err(ApiError::bad_request("channel members do not support restrictions"));
    }
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    if request.user_id == bot.id {
        return Err(ApiError::bad_request("bot can't restrict itself"));
    }

    let old_record = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)?;
    let old_status = old_record
        .as_ref()
        .map(|record| record.status.clone())
        .unwrap_or_else(|| "left".to_string());

    if old_status == "owner" {
        return Err(ApiError::bad_request("can't restrict chat owner"));
    }
    if old_status == "banned" {
        return Err(ApiError::bad_request("can't restrict banned user"));
    }

    let now = Utc::now().timestamp();
    let target_record = ensure_sim_user_record(&mut conn, request.user_id)?;

    let permissions = request.permissions;
    let full_permissions = permission_enabled(permissions.can_send_messages, false)
        && permission_enabled(permissions.can_send_audios, false)
        && permission_enabled(permissions.can_send_documents, false)
        && permission_enabled(permissions.can_send_photos, false)
        && permission_enabled(permissions.can_send_videos, false)
        && permission_enabled(permissions.can_send_video_notes, false)
        && permission_enabled(permissions.can_send_voice_notes, false)
        && permission_enabled(permissions.can_send_polls, false)
        && permission_enabled(permissions.can_send_other_messages, false)
        && permission_enabled(permissions.can_add_web_page_previews, false)
        && permission_enabled(permissions.can_invite_users, false)
        && permission_enabled(permissions.can_change_info, false)
        && permission_enabled(permissions.can_pin_messages, false)
        && permission_enabled(permissions.can_manage_topics, false);

    let restriction_expired = request.until_date.map(|until| until > 0 && until <= now).unwrap_or(false);
    let next_status = if full_permissions && restriction_expired {
        "member"
    } else if full_permissions && request.until_date.is_none() {
        "member"
    } else {
        "restricted"
    };

    let tag = old_record.as_ref().and_then(|record| record.tag.clone());
    let permissions_json = if next_status == "restricted" {
        Some(serde_json::to_string(&permissions).map_err(ApiError::internal)?)
    } else {
        None
    };

    upsert_chat_member_record(
        &mut conn,
        bot.id,
        &chat_key,
        request.user_id,
        next_status,
        next_status,
        old_record
            .as_ref()
            .and_then(|record| record.joined_at)
            .or(Some(now)),
        permissions_json.as_deref(),
        if next_status == "restricted" {
            request.until_date
        } else {
            None
        },
        None,
        tag.as_deref(),
        now,
    )?;

    let new_record = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)?;
    let chat = build_chat_from_group_record(&sim_chat);
    let target = build_user_from_sim_record(&target_record, false);

    emit_chat_member_transition_update_with_records(
        state,
        &mut conn,
        token,
        bot.id,
        &chat,
        &actor,
        &target,
        &old_status,
        next_status,
        old_record.as_ref(),
        new_record.as_ref(),
        now,
    )?;

    Ok(json!(true))
}

pub fn handle_set_chat_administrator_custom_title(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetChatAdministratorCustomTitleRequest = parse_request(params)?;
    let custom_title = request.custom_title.trim().to_string();
    if custom_title.is_empty() {
        return Err(ApiError::bad_request("custom_title is empty"));
    }
    if custom_title.chars().count() > 16 {
        return Err(ApiError::bad_request("custom_title is too long"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    if sim_chat.chat_type == "channel" {
        return Err(ApiError::bad_request("channel members do not support custom titles"));
    }
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    let Some(old_record) = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)? else {
        return Err(ApiError::not_found("chat member not found"));
    };
    if old_record.status != "admin" {
        return Err(ApiError::bad_request("user is not an administrator"));
    }
    if old_record.custom_title.as_deref() == Some(custom_title.as_str()) {
        return Ok(json!(true));
    }

    let now = Utc::now().timestamp();
    upsert_chat_member_record(
        &mut conn,
        bot.id,
        &chat_key,
        request.user_id,
        "admin",
        "admin",
        old_record.joined_at,
        old_record.permissions_json.as_deref(),
        old_record.until_date,
        Some(custom_title.as_str()),
        old_record.tag.as_deref(),
        now,
    )?;

    let Some(new_record) = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)? else {
        return Err(ApiError::internal("failed to load updated chat member"));
    };

    let target_user = ensure_sim_user_record(&mut conn, request.user_id)?;
    let chat = build_chat_from_group_record(&sim_chat);
    let target = build_user_from_sim_record(&target_user, false);

    emit_chat_member_transition_update_with_records(
        state,
        &mut conn,
        token,
        bot.id,
        &chat,
        &actor,
        &target,
        "admin",
        "admin",
        Some(&old_record),
        Some(&new_record),
        now,
    )?;

    Ok(json!(true))
}

pub fn handle_set_chat_description(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetChatDescriptionRequest = parse_request(params)?;
    let description = request
        .description
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    if let Some(value) = description.as_ref() {
        if value.chars().count() > 255 {
            return Err(ApiError::bad_request("description is too long"));
        }
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chats
         SET description = ?1, updated_at = ?2
         WHERE bot_id = ?3 AND chat_key = ?4",
        params![description, now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_set_chat_member_tag(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetChatMemberTagRequest = parse_request(params)?;
    let normalized_tag = request
        .tag
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    if let Some(tag) = normalized_tag.as_ref() {
        if tag.chars().count() > 32 {
            return Err(ApiError::bad_request("tag is too long"));
        }
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    if sim_chat.chat_type == "channel" {
        return Err(ApiError::bad_request("channel members do not support tags"));
    }
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    let Some(old_record) = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)? else {
        return Err(ApiError::not_found("chat member not found"));
    };
    if !matches!(old_record.status.as_str(), "member" | "restricted") {
        return Err(ApiError::bad_request("tag can only be set for regular or restricted members"));
    }
    if old_record.tag == normalized_tag {
        return Ok(json!(true));
    }

    let now = Utc::now().timestamp();
    upsert_chat_member_record(
        &mut conn,
        bot.id,
        &chat_key,
        request.user_id,
        old_record.status.as_str(),
        old_record.role.as_str(),
        old_record.joined_at,
        old_record.permissions_json.as_deref(),
        old_record.until_date,
        old_record.custom_title.as_deref(),
        normalized_tag.as_deref(),
        now,
    )?;

    let Some(new_record) = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)? else {
        return Err(ApiError::internal("failed to load updated chat member"));
    };

    let target_user = ensure_sim_user_record(&mut conn, request.user_id)?;
    let chat = build_chat_from_group_record(&sim_chat);
    let target = build_user_from_sim_record(&target_user, false);

    emit_chat_member_transition_update_with_records(
        state,
        &mut conn,
        token,
        bot.id,
        &chat,
        &actor,
        &target,
        old_record.status.as_str(),
        new_record.status.as_str(),
        Some(&old_record),
        Some(&new_record),
        now,
    )?;

    Ok(json!(true))
}

pub fn handle_set_chat_permissions(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetChatPermissionsRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;

    let permissions_json = serde_json::to_string(&request.permissions).map_err(ApiError::internal)?;
    let now = Utc::now().timestamp();

    conn.execute(
        "UPDATE sim_chats
         SET permissions_json = ?1, updated_at = ?2
         WHERE bot_id = ?3 AND chat_key = ?4",
        params![permissions_json, now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_set_chat_photo(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetChatPhotoRequest = parse_request(params)?;
    let photo_value = serde_json::to_value(&request.photo).map_err(ApiError::internal)?;
    let normalized_photo = parse_input_file_value(&photo_value, "photo")?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    let stored_file = resolve_media_file_with_conn(&mut conn, bot.id, &normalized_photo, "photo")?;
    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chats
         SET photo_file_id = ?1, updated_at = ?2
         WHERE bot_id = ?3 AND chat_key = ?4",
        params![stored_file.file_id, now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    let chat = build_chat_from_group_record(&sim_chat);
    let mut service_fields = Map::<String, Value>::new();
    service_fields.insert("new_chat_photo".to_string(), Value::Bool(true));
    emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &actor,
        now,
        format!("{} changed the group photo", display_name_for_service_user(&actor)),
        service_fields,
    )?;

    Ok(json!(true))
}

pub fn handle_set_chat_sticker_set(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetChatStickerSetRequest = parse_request(params)?;
    let set_name = request.sticker_set_name.trim().to_string();
    if set_name.is_empty() {
        return Err(ApiError::bad_request("sticker_set_name is empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;

    if sim_chat.chat_type != "supergroup" {
        return Err(ApiError::bad_request("sticker set can only be set for supergroups"));
    }

    let exists: Option<String> = conn
        .query_row(
            "SELECT name FROM sticker_sets WHERE bot_id = ?1 AND name = ?2",
            params![bot.id, set_name],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if exists.is_none() {
        return Err(ApiError::bad_request("sticker set was not found"));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chats
         SET sticker_set_name = ?1, updated_at = ?2
         WHERE bot_id = ?3 AND chat_key = ?4",
        params![set_name, now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_set_chat_menu_button(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetChatMenuButtonRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    if let Some(chat_id) = request.chat_id {
        let chat_key = chat_id_to_chat_key(chat_id);
        if let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &chat_key)? {
            if sim_chat.chat_type != "private" {
                return Err(ApiError::bad_request("chat menu button can only be set for private chats"));
            }
        } else if chat_id <= 0 {
            return Err(ApiError::bad_request("chat menu button can only be set for private chats"));
        }
    }

    let menu_button_value = if let Some(menu_button) = request.menu_button {
        serde_json::to_value(menu_button).map_err(ApiError::internal)?
    } else {
        serde_json::to_value(default_menu_button()).map_err(ApiError::internal)?
    };

    if !menu_button_value.is_object() {
        return Err(ApiError::bad_request("menu_button is invalid"));
    }

    let now = Utc::now().timestamp();
    let scope_key = menu_button_scope_key(request.chat_id);
    conn.execute(
        "INSERT INTO bot_chat_menu_buttons (bot_id, scope_key, menu_button_json, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(bot_id, scope_key)
         DO UPDATE SET
            menu_button_json = excluded.menu_button_json,
            updated_at = excluded.updated_at",
        params![bot.id, scope_key, menu_button_value.to_string(), now],
    )
    .map_err(ApiError::internal)?;

    Ok(Value::Bool(true))
}

pub fn handle_set_chat_title(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetChatTitleRequest = parse_request(params)?;
    let title = request.title.trim().to_string();
    if title.is_empty() {
        return Err(ApiError::bad_request("title is empty"));
    }
    if title.chars().count() > 128 {
        return Err(ApiError::bad_request("title is too long"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    let now = Utc::now().timestamp();
    let title_changed = sim_chat.title.as_deref() != Some(title.as_str());

    conn.execute(
        "UPDATE chats SET title = ?1 WHERE chat_key = ?2",
        params![&title, &chat_key],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "UPDATE sim_chats
         SET title = ?1, updated_at = ?2
         WHERE bot_id = ?3 AND chat_key = ?4",
        params![&title, now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    if title_changed {
        let mut chat = build_chat_from_group_record(&sim_chat);
        chat.title = Some(title.clone());
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

    Ok(json!(true))
}

pub fn handle_unban_chat_member(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: UnbanChatMemberRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    if sim_chat.chat_type == "channel" {
        return Err(ApiError::bad_request("channel members do not support tags"));
    }
    let actor = resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    if request.user_id == bot.id {
        return Err(ApiError::bad_request("bot can't unban itself"));
    }

    let old_record = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)?;
    let Some(old_record) = old_record else {
        return Ok(json!(true));
    };
    let old_status = old_record.status.clone();

    if old_status != "banned" {
        if request.only_if_banned.unwrap_or(false) {
            return Ok(json!(true));
        }
        return Ok(json!(true));
    }

    let target_record = ensure_sim_user_record(&mut conn, request.user_id)?;
    let now = Utc::now().timestamp();
    upsert_chat_member_record(
        &mut conn,
        bot.id,
        &chat_key,
        request.user_id,
        "left",
        "left",
        None,
        None,
        None,
        None,
        None,
        now,
    )?;

    let new_record = load_chat_member_record(&mut conn, bot.id, &chat_key, request.user_id)?;

    let chat = build_chat_from_group_record(&sim_chat);
    let target = build_user_from_sim_record(&target_record, false);

    emit_chat_member_transition_update_with_records(
        state,
        &mut conn,
        token,
        bot.id,
        &chat,
        &actor,
        &target,
        "banned",
        "left",
        Some(&old_record),
        new_record.as_ref(),
        now,
    )?;

    Ok(json!(true))
}

pub fn handle_unban_chat_sender_chat(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: UnbanChatSenderChatRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_can_manage_sender_chat_in_linked_context(
        &mut conn,
        &bot,
        &chat_key,
        &sim_chat,
    )?;

    conn.execute(
        "DELETE FROM sim_banned_sender_chats
         WHERE bot_id = ?1 AND chat_key = ?2 AND sender_chat_id = ?3",
        params![bot.id, &chat_key, request.sender_chat_id],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_unpin_all_chat_messages(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: UnpinAllChatMessagesRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "DELETE FROM sim_chat_pinned_messages WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "UPDATE sim_chats
         SET pinned_message_id = NULL, updated_at = ?1
         WHERE bot_id = ?2 AND chat_key = ?3",
        params![now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_unpin_chat_message(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: UnpinChatMessageRequest = parse_request(params)?;
    if request.business_connection_id.is_some() {
        return Err(ApiError::bad_request("business_connection_id is not supported in simulator"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    ensure_request_actor_is_chat_admin_or_owner(&mut conn, &bot, &chat_key)?;

    let target_message_id = if let Some(message_id) = request.message_id {
        Some(message_id)
    } else {
        load_latest_pinned_message_id(&mut conn, bot.id, &chat_key)?
    };

    if let Some(message_id) = target_message_id {
        conn.execute(
            "DELETE FROM sim_chat_pinned_messages
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, &chat_key, message_id],
        )
        .map_err(ApiError::internal)?;
    }

    let now = Utc::now().timestamp();
    let _ = sync_latest_pinned_message_id(&mut conn, bot.id, &chat_key, now)?;

    Ok(json!(true))
}

pub fn handle_verify_chat(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: VerifyChatRequest = parse_request(params)?;
    let custom_description =
        normalize_verification_custom_description(request.custom_description.as_deref())?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_verifications_storage(&mut conn)?;

    let (chat_key, sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    if is_direct_messages_chat(&sim_chat) {
        return Err(ApiError::bad_request(
            "verification is not supported for channel direct messages chats",
        ));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_chat_verifications
         (bot_id, chat_key, custom_description, verified_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?4)
         ON CONFLICT(bot_id, chat_key)
         DO UPDATE SET
             custom_description = excluded.custom_description,
             verified_at = excluded.verified_at,
             updated_at = excluded.updated_at",
        params![bot.id, chat_key, custom_description, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}
