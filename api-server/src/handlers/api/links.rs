use actix_web::web::Data;
use chrono::Utc;
use rusqlite::params;
use serde_json::Value;
use std::collections::HashMap;

use crate::database::{
    ensure_bot, lock_db, AppState
};

use crate::types::{ApiError, ApiResult};

use crate::generated::methods::{
    CreateChatInviteLinkRequest, CreateChatSubscriptionInviteLinkRequest,
    EditChatInviteLinkRequest, EditChatSubscriptionInviteLinkRequest,
    ExportChatInviteLinkRequest, RevokeChatInviteLinkRequest,
};

use crate::handlers::client::types::groups::SimChatInviteLinkRecord;

use crate::handlers::client::{chats, channels};

use crate::handlers::parse_request;

pub fn handle_create_chat_invite_link(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: CreateChatInviteLinkRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = chats::resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    let actor = chats::resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;
    if sim_chat.chat_type == "channel" {
        channels::ensure_channel_actor_can_manage_invite_links(&mut conn, bot.id, &chat_key, actor.id)?;
    }

    let now = Utc::now().timestamp();
    let creates_join_request = request.creates_join_request.unwrap_or(false);
    let name = request
        .name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let expire_date = request.expire_date.filter(|value| *value > now);
    let member_limit = request.member_limit.filter(|value| *value > 0);

    if creates_join_request && member_limit.is_some() {
        return Err(ApiError::bad_request("member_limit can't be used when creates_join_request is true"));
    }

    let invite_link = chats::generate_unique_invite_link_for_bot(&mut conn, bot.id)?;
    conn.execute(
        "INSERT INTO sim_chat_invite_links
         (bot_id, chat_key, invite_link, creator_user_id, creates_join_request, is_primary, is_revoked, name, expire_date, member_limit, subscription_period, subscription_price, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, 0, 0, ?6, ?7, ?8, NULL, NULL, ?9, ?9)",
        params![
            bot.id,
            &chat_key,
            &invite_link,
            actor.id,
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
        creator_user_id: actor.id,
        creates_join_request,
        is_primary: false,
        is_revoked: false,
        name,
        expire_date,
        member_limit,
        subscription_period: None,
        subscription_price: None,
    };

    let pending_count = chats::pending_join_request_count_for_link(&mut conn, bot.id, &chat_key, &record.invite_link)?;
    let invite = chats::chat_invite_link_from_record(actor, &record, Some(pending_count));
    serde_json::to_value(invite).map_err(ApiError::internal)
}

pub fn handle_create_chat_subscription_invite_link(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: CreateChatSubscriptionInviteLinkRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = chats::resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    let actor = chats::resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    if sim_chat.chat_type != "channel" {
        return Err(ApiError::bad_request("subscription invite links are only available for channels"));
    }
    channels::ensure_channel_actor_can_manage_invite_links(&mut conn, bot.id, &chat_key, actor.id)?;
    if request.subscription_period <= 0 {
        return Err(ApiError::bad_request("subscription_period must be greater than zero"));
    }
    if request.subscription_price <= 0 {
        return Err(ApiError::bad_request("subscription_price must be greater than zero"));
    }

    let name = request
        .name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    let now = Utc::now().timestamp();
    let invite_link = chats::generate_unique_invite_link_for_bot(&mut conn, bot.id)?;
    conn.execute(
        "INSERT INTO sim_chat_invite_links
         (bot_id, chat_key, invite_link, creator_user_id, creates_join_request, is_primary, is_revoked, name, expire_date, member_limit, subscription_period, subscription_price, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, 0, 0, 0, ?5, NULL, NULL, ?6, ?7, ?8, ?8)",
        params![
            bot.id,
            &chat_key,
            &invite_link,
            actor.id,
            name,
            request.subscription_period,
            request.subscription_price,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    let record = SimChatInviteLinkRecord {
        invite_link,
        creator_user_id: actor.id,
        creates_join_request: false,
        is_primary: false,
        is_revoked: false,
        name,
        expire_date: None,
        member_limit: None,
        subscription_period: Some(request.subscription_period),
        subscription_price: Some(request.subscription_price),
    };
    let invite = chats::chat_invite_link_from_record(actor, &record, Some(0));
    serde_json::to_value(invite).map_err(ApiError::internal)
}

pub fn handle_edit_chat_invite_link(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditChatInviteLinkRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = chats::resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    let actor = chats::resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;
    if sim_chat.chat_type == "channel" {
        channels::ensure_channel_actor_can_manage_invite_links(&mut conn, bot.id, &chat_key, actor.id)?;
    }

    let Some(existing) = chats::load_invite_link_record(&mut conn, bot.id, &chat_key, &request.invite_link)? else {
        return Err(ApiError::not_found("invite link not found"));
    };
    if existing.is_primary {
        return Err(ApiError::bad_request("primary invite link can't be edited with editChatInviteLink"));
    }
    if existing.creator_user_id != actor.id {
        return Err(ApiError::bad_request("invite link wasn't created by this actor"));
    }
    if existing.is_revoked {
        return Err(ApiError::bad_request("invite link is revoked"));
    }

    let now = Utc::now().timestamp();
    let name = request
        .name
        .map(|value| value.trim().to_string())
        .or(existing.name.clone())
        .filter(|value| !value.is_empty());
    let expire_date = match request.expire_date {
        Some(value) if value > now => Some(value),
        Some(_) => None,
        None => existing.expire_date,
    };
    let member_limit = match request.member_limit {
        Some(value) if value > 0 => Some(value),
        Some(_) => None,
        None => existing.member_limit,
    };
    let creates_join_request = request
        .creates_join_request
        .unwrap_or(existing.creates_join_request);

    if creates_join_request && member_limit.is_some() {
        return Err(ApiError::bad_request("member_limit can't be used when creates_join_request is true"));
    }

    conn.execute(
        "UPDATE sim_chat_invite_links
         SET creates_join_request = ?1,
             name = ?2,
             expire_date = ?3,
             member_limit = ?4,
             updated_at = ?5
         WHERE bot_id = ?6 AND chat_key = ?7 AND invite_link = ?8",
        params![
            if creates_join_request { 1 } else { 0 },
            name,
            expire_date,
            member_limit,
            now,
            bot.id,
            &chat_key,
            &request.invite_link,
        ],
    )
    .map_err(ApiError::internal)?;

    let updated = SimChatInviteLinkRecord {
        invite_link: existing.invite_link,
        creator_user_id: existing.creator_user_id,
        creates_join_request,
        is_primary: existing.is_primary,
        is_revoked: existing.is_revoked,
        name,
        expire_date,
        member_limit,
        subscription_period: existing.subscription_period,
        subscription_price: existing.subscription_price,
    };

    let pending_count = chats::pending_join_request_count_for_link(&mut conn, bot.id, &chat_key, &updated.invite_link)?;
    let invite = chats::chat_invite_link_from_record(
        chats::resolve_invite_creator_user(&mut conn, &bot, updated.creator_user_id)?,
        &updated,
        Some(pending_count),
    );
    serde_json::to_value(invite).map_err(ApiError::internal)
}

pub fn handle_edit_chat_subscription_invite_link(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditChatSubscriptionInviteLinkRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = chats::resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    let actor = chats::resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    if sim_chat.chat_type != "channel" {
        return Err(ApiError::bad_request("subscription invite links are only available for channels"));
    }
    channels::ensure_channel_actor_can_manage_invite_links(&mut conn, bot.id, &chat_key, actor.id)?;

    let Some(existing) = chats::load_invite_link_record(&mut conn, bot.id, &chat_key, &request.invite_link)? else {
        return Err(ApiError::not_found("invite link not found"));
    };
    if existing.creator_user_id != actor.id {
        return Err(ApiError::bad_request("invite link wasn't created by this actor"));
    }
    if existing.subscription_period.is_none() || existing.subscription_price.is_none() {
        return Err(ApiError::bad_request("invite link is not a subscription link"));
    }

    let now = Utc::now().timestamp();
    let name = request
        .name
        .map(|value| value.trim().to_string())
        .or(existing.name.clone())
        .filter(|value| !value.is_empty());

    conn.execute(
        "UPDATE sim_chat_invite_links
         SET name = ?1, updated_at = ?2
         WHERE bot_id = ?3 AND chat_key = ?4 AND invite_link = ?5",
        params![name, now, bot.id, &chat_key, &request.invite_link],
    )
    .map_err(ApiError::internal)?;

    let updated = SimChatInviteLinkRecord {
        invite_link: existing.invite_link,
        creator_user_id: existing.creator_user_id,
        creates_join_request: existing.creates_join_request,
        is_primary: existing.is_primary,
        is_revoked: existing.is_revoked,
        name,
        expire_date: existing.expire_date,
        member_limit: existing.member_limit,
        subscription_period: existing.subscription_period,
        subscription_price: existing.subscription_price,
    };
    let invite = chats::chat_invite_link_from_record(
        chats::resolve_invite_creator_user(&mut conn, &bot, updated.creator_user_id)?,
        &updated,
        Some(0),
    );
    serde_json::to_value(invite).map_err(ApiError::internal)
}

pub fn handle_export_chat_invite_link(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: ExportChatInviteLinkRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = chats::resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    let actor = chats::resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;
    if sim_chat.chat_type == "channel" {
        channels::ensure_channel_actor_can_manage_invite_links(&mut conn, bot.id, &chat_key, actor.id)?;
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chat_invite_links
         SET is_revoked = 1, updated_at = ?3
         WHERE bot_id = ?1 AND chat_key = ?2 AND is_primary = 1 AND is_revoked = 0",
        params![bot.id, &chat_key, now],
    )
    .map_err(ApiError::internal)?;

    let invite_link = chats::generate_unique_invite_link_for_bot(&mut conn, bot.id)?;
    conn.execute(
        "INSERT INTO sim_chat_invite_links
         (bot_id, chat_key, invite_link, creator_user_id, creates_join_request, is_primary, is_revoked, name, expire_date, member_limit, subscription_period, subscription_price, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, 0, 1, 0, NULL, NULL, NULL, NULL, NULL, ?5, ?5)",
        params![bot.id, &chat_key, &invite_link, actor.id, now],
    )
    .map_err(ApiError::internal)?;

    Ok(Value::String(invite_link))
}

pub fn handle_revoke_chat_invite_link(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: RevokeChatInviteLinkRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = chats::resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    let actor = chats::resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;
    if sim_chat.chat_type == "channel" {
        channels::ensure_channel_actor_can_manage_invite_links(&mut conn, bot.id, &chat_key, actor.id)?;
    }

    let Some(existing) = chats::load_invite_link_record(&mut conn, bot.id, &chat_key, &request.invite_link)? else {
        return Err(ApiError::not_found("invite link not found"));
    };

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chat_invite_links
         SET is_revoked = 1, updated_at = ?1
         WHERE bot_id = ?2 AND chat_key = ?3 AND invite_link = ?4",
        params![now, bot.id, &chat_key, &request.invite_link],
    )
    .map_err(ApiError::internal)?;

    if existing.is_primary {
        let new_primary_link = chats::generate_unique_invite_link_for_bot(&mut conn, bot.id)?;
        conn.execute(
            "INSERT INTO sim_chat_invite_links
             (bot_id, chat_key, invite_link, creator_user_id, creates_join_request, is_primary, is_revoked, name, expire_date, member_limit, subscription_period, subscription_price, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, 0, 1, 0, NULL, NULL, NULL, NULL, NULL, ?5, ?5)",
            params![bot.id, &chat_key, &new_primary_link, actor.id, now],
        )
        .map_err(ApiError::internal)?;
    }

    let revoked = SimChatInviteLinkRecord {
        invite_link: existing.invite_link,
        creator_user_id: existing.creator_user_id,
        creates_join_request: existing.creates_join_request,
        is_primary: existing.is_primary,
        is_revoked: true,
        name: existing.name,
        expire_date: existing.expire_date,
        member_limit: existing.member_limit,
        subscription_period: existing.subscription_period,
        subscription_price: existing.subscription_price,
    };

    let pending_count = chats::pending_join_request_count_for_link(&mut conn, bot.id, &chat_key, &revoked.invite_link)?;
    let invite = chats::chat_invite_link_from_record(
        chats::resolve_invite_creator_user(&mut conn, &bot, revoked.creator_user_id)?,
        &revoked,
        Some(pending_count),
    );
    serde_json::to_value(invite).map_err(ApiError::internal)
}