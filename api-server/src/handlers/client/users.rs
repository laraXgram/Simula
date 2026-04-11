use chrono::Utc;
use rusqlite::{params, OptionalExtension};

use crate::types::ApiError;
use crate::handlers::client::types::users::SimUserRecord;

use crate::generated::types::{Chat, User};

use crate::handlers::utils::updates::current_request_actor_user_id;

use super::chats::ChatSendKind;
use super::{bot, chats};

pub enum SimUserPayload {
    Dice(Dice),
    Game(Game),
    Contact(Contact),
    Location(Location),
    Venue(Venue),
}

pub fn build_user_from_sim_record(record: &SimUserRecord, is_bot: bool) -> User {
    User {
        id: record.id,
        is_bot,
        first_name: record.first_name.clone(),
        last_name: record.last_name.clone(),
        username: record.username.clone(),
        language_code: None,
        is_premium: Some(record.is_premium),
        added_to_attachment_menu: None,
        can_join_groups: None,
        can_read_all_group_messages: None,
        supports_inline_queries: None,
        can_connect_to_business: None,
        has_main_web_app: None,
        has_topics_enabled: None,
        allows_users_to_create_topics: None,
        can_manage_bots: None,
    }
}

pub fn ensure_sim_user_record(
    conn: &mut rusqlite::Connection,
    user_id: i64,
) -> Result<SimUserRecord, ApiError> {
    if let Some(existing) = load_sim_user_record(conn, user_id)? {
        return Ok(existing);
    }

    ensure_user(
        conn,
        Some(user_id),
        Some(format!("User {}", user_id)),
        None,
    )
}

pub fn ensure_user(
    conn: &mut rusqlite::Connection,
    user_id: Option<i64>,
    first_name: Option<String>,
    username: Option<String>,
) -> Result<SimUserRecord, ApiError> {
    let id = user_id.unwrap_or(10001);
    let effective_first_name = first_name.unwrap_or_else(|| "Test User".to_string());
    let now = Utc::now().timestamp();

    conn.execute(
        "INSERT INTO users (id, username, first_name, created_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(id) DO UPDATE SET
            username = COALESCE(excluded.username, users.username),
            first_name = excluded.first_name",
        params![id, username, effective_first_name, now],
    )
    .map_err(ApiError::internal)?;

    conn.query_row(
        "SELECT id, first_name, username, last_name, is_premium FROM users WHERE id = ?1",
        params![id],
        |row| {
            Ok(SimUserRecord {
                id: row.get(0)?,
                first_name: row.get(1)?,
                username: row.get(2)?,
                last_name: row.get(3)?,
                is_premium: row.get::<_, i64>(4)? == 1,
            })
        },
    )
    .map_err(ApiError::internal)
}

pub fn load_sim_user_record(
    conn: &mut rusqlite::Connection,
    user_id: i64,
) -> Result<Option<SimUserRecord>, ApiError> {
    conn.query_row(
        "SELECT id, first_name, username, last_name, is_premium FROM users WHERE id = ?1",
        params![user_id],
        |row| {
            Ok(SimUserRecord {
                id: row.get(0)?,
                first_name: row.get(1)?,
                username: row.get(2)?,
                last_name: row.get(3)?,
                is_premium: row.get::<_, i64>(4)? == 1,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

pub fn resolve_transport_sender_user(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    destination_chat_key: &str,
    destination_chat: &Chat,
    send_kind: ChatSendKind,
) -> Result<User, ApiError> {
    let actor_user_id = current_request_actor_user_id().unwrap_or(bot.id);
    if actor_user_id == bot.id {
        return Ok(bot::build_bot_user(bot));
    }

    let actor_record = ensure_sim_user_record(conn, actor_user_id)?;
    if destination_chat.r#type != "private" {
        chats::ensure_sender_can_send_in_chat(conn, bot.id, destination_chat_key, actor_user_id, send_kind)?;
    }

    Ok(build_user_from_sim_record(&actor_record, false))
}

pub fn build_sim_user_chat_id(chat_id: i64, user_id: i64) -> i64 {
    let chat_part = chat_id.abs() % 10_000_000_000;
    let user_part = user_id.abs() % 100_000;
    ((chat_part * 100_000) + user_part).max(1)
}

pub fn get_or_create_user(
    conn: &mut rusqlite::Connection,
    user_id: Option<i64>,
    first_name: Option<String>,
    username: Option<String>,
) -> Result<SimUserRecord, ApiError> {
    if let Some(id) = user_id {
        if first_name.is_none() && username.is_none() {
            if let Some(existing) = load_sim_user_record(conn, id)? {
                return Ok(existing);
            }
        }
    }
    ensure_user(conn, user_id, first_name, username)
}

pub fn handle_sim_upsert_user(state: &Data<AppState>, body: SimUpsertUserRequest) -> ApiResult {
    let conn = lock_db(state)?;

    struct ExistingSimUserProfile {
        first_name: String,
        username: Option<String>,
        last_name: Option<String>,
        phone_number: Option<String>,
        photo_url: Option<String>,
        bio: Option<String>,
        is_premium: bool,
        business_name: Option<String>,
        business_intro: Option<String>,
        business_location: Option<String>,
        gift_count: i64,
    }

    let normalize_optional_text = |input: Option<String>| {
        input
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    };

    let id = body
        .id
        .unwrap_or_else(|| (Utc::now().timestamp_millis() % 9_000_000) + 10_000);

    let existing = conn
        .query_row(
            "SELECT first_name, username, last_name, phone_number, photo_url, bio, is_premium,
                    business_name, business_intro, business_location, gift_count
             FROM users
             WHERE id = ?1",
            params![id],
            |row| {
                Ok(ExistingSimUserProfile {
                    first_name: row.get(0)?,
                    username: row.get(1)?,
                    last_name: row.get(2)?,
                    phone_number: row.get(3)?,
                    photo_url: row.get(4)?,
                    bio: row.get(5)?,
                    is_premium: row.get::<_, i64>(6)? == 1,
                    business_name: row.get(7)?,
                    business_intro: row.get(8)?,
                    business_location: row.get(9)?,
                    gift_count: row.get(10)?,
                })
            },
        )
        .optional()
        .map_err(ApiError::internal)?;

    let first_name = normalize_optional_text(body.first_name)
        .or_else(|| existing.as_ref().map(|profile| profile.first_name.clone()))
        .unwrap_or_else(|| format!("User {}", id));
    let username = body
        .username
        .as_deref()
        .map(sanitize_username)
        .filter(|value| !value.is_empty())
        .or_else(|| existing.as_ref().and_then(|profile| profile.username.clone()))
        .unwrap_or_else(|| format!("user_{}", id));
    let last_name = normalize_optional_text(body.last_name)
        .or_else(|| existing.as_ref().and_then(|profile| profile.last_name.clone()));
    let phone_number = normalize_optional_text(body.phone_number)
        .or_else(|| existing.as_ref().and_then(|profile| profile.phone_number.clone()));
    let photo_url = normalize_optional_text(body.photo_url)
        .or_else(|| existing.as_ref().and_then(|profile| profile.photo_url.clone()));
    let bio = normalize_optional_text(body.bio)
        .or_else(|| existing.as_ref().and_then(|profile| profile.bio.clone()));
    let business_name = normalize_optional_text(body.business_name)
        .or_else(|| existing.as_ref().and_then(|profile| profile.business_name.clone()));
    let business_intro = normalize_optional_text(body.business_intro)
        .or_else(|| existing.as_ref().and_then(|profile| profile.business_intro.clone()));
    let business_location = normalize_optional_text(body.business_location)
        .or_else(|| existing.as_ref().and_then(|profile| profile.business_location.clone()));
    let is_premium = body
        .is_premium
        .or_else(|| existing.as_ref().map(|profile| profile.is_premium))
        .unwrap_or(false);
    let gift_count = body
        .gift_count
        .or_else(|| existing.as_ref().map(|profile| profile.gift_count))
        .unwrap_or(0)
        .max(0);

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO users
         (id, username, first_name, last_name, phone_number, photo_url, bio, is_premium,
          business_name, business_intro, business_location, gift_count, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
         ON CONFLICT(id) DO UPDATE SET
            username = excluded.username,
            first_name = excluded.first_name,
            last_name = excluded.last_name,
            phone_number = excluded.phone_number,
            photo_url = excluded.photo_url,
            bio = excluded.bio,
            is_premium = excluded.is_premium,
            business_name = excluded.business_name,
            business_intro = excluded.business_intro,
            business_location = excluded.business_location,
            gift_count = excluded.gift_count",
        params![
            id,
            username,
            first_name,
            last_name,
            phone_number,
            photo_url,
            bio,
            if is_premium { 1 } else { 0 },
            business_name,
            business_intro,
            business_location,
            gift_count,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!({
        "id": id,
        "username": username,
        "first_name": first_name,
        "last_name": last_name,
        "phone_number": phone_number,
        "photo_url": photo_url,
        "bio": bio,
        "is_premium": is_premium,
        "business_name": business_name,
        "business_intro": business_intro,
        "business_location": business_location,
        "gift_count": gift_count
    }))
}

pub fn handle_sim_delete_user(state: &Data<AppState>, body: SimDeleteUserRequest) -> ApiResult {
    if body.id <= 0 {
        return Err(ApiError::bad_request("user id is invalid"));
    }

    let conn = lock_db(state)?;
    let user_exists: Option<i64> = conn
        .query_row(
            "SELECT id FROM users WHERE id = ?1",
            params![body.id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if user_exists.is_none() {
        return Err(ApiError::not_found("user not found"));
    }

    let user_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM users", [], |row| row.get(0))
        .map_err(ApiError::internal)?;
    if user_count <= 1 {
        return Err(ApiError::bad_request("at least one user must remain in simulator"));
    }

    conn.execute(
        "DELETE FROM sim_business_read_messages
         WHERE connection_id IN (
             SELECT connection_id
             FROM sim_business_connections
             WHERE user_id = ?1
         )",
        params![body.id],
    )
    .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM sim_business_connections WHERE user_id = ?1", params![body.id])
        .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM sim_business_account_profiles WHERE user_id = ?1",
        params![body.id],
    )
    .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM sim_chat_members WHERE user_id = ?1", params![body.id])
        .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM sim_chat_join_requests WHERE user_id = ?1",
        params![body.id],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM sim_direct_message_topics WHERE user_id = ?1",
        params![body.id],
    )
    .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM poll_votes WHERE voter_user_id = ?1", params![body.id])
        .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM game_scores WHERE user_id = ?1", params![body.id])
        .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM star_transactions_ledger WHERE user_id = ?1", params![body.id])
        .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM star_subscriptions WHERE user_id = ?1", params![body.id])
        .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM callback_queries WHERE from_user_id = ?1", params![body.id])
        .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM inline_queries WHERE from_user_id = ?1", params![body.id])
        .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM shipping_queries WHERE from_user_id = ?1", params![body.id])
        .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM pre_checkout_queries WHERE from_user_id = ?1",
        params![body.id],
    )
    .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM users WHERE id = ?1", params![body.id])
        .map_err(ApiError::internal)?;

    Ok(json!({ "deleted": true, "id": body.id }))
}

pub fn handle_sim_set_user_profile_audio(
    state: &Data<AppState>,
    token: &str,
    body: SimSetUserProfileAudioRequest,
) -> ApiResult {
    if body.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let normalize_optional_text = |value: Option<String>| {
        value
            .map(|item| item.trim().to_string())
            .filter(|item| !item.is_empty())
    };

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_sim_user_record(&mut conn, body.user_id)?;
    ensure_sim_user_profile_audios_storage(&mut conn)?;

    let title = normalize_optional_text(body.title).unwrap_or_else(|| "Profile audio".to_string());
    let performer = normalize_optional_text(body.performer).or_else(|| Some(user.first_name.clone()));
    let file_name = normalize_optional_text(body.file_name).unwrap_or_else(|| "profile-audio.ogg".to_string());
    let mime_type = normalize_optional_text(body.mime_type).unwrap_or_else(|| "audio/ogg".to_string());
    let file_size = body.file_size.filter(|value| *value > 0);
    let duration = body.duration.unwrap_or(30).clamp(1, 3600);

    conn.execute(
        "UPDATE sim_user_profile_audios
         SET position = position + 1
         WHERE bot_id = ?1 AND user_id = ?2",
        params![bot.id, body.user_id],
    )
    .map_err(ApiError::internal)?;

    let file_id = generate_telegram_file_id("profile_audio");
    let file_unique_id = generate_telegram_file_unique_id();
    let now = Utc::now().timestamp();

    conn.execute(
        "INSERT INTO sim_user_profile_audios
         (bot_id, user_id, file_id, file_unique_id, duration, performer, title, file_name, mime_type, file_size, position, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 0, ?11)",
        params![
            bot.id,
            body.user_id,
            &file_id,
            &file_unique_id,
            duration,
            performer.clone(),
            title.clone(),
            file_name.clone(),
            mime_type.clone(),
            file_size,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!({
        "user_id": body.user_id,
        "file_id": file_id,
        "file_unique_id": file_unique_id,
        "title": title,
        "file_name": file_name,
        "mime_type": mime_type,
        "file_size": file_size,
        "duration": duration,
    }))
}

pub fn handle_sim_upload_user_profile_audio(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SimUploadUserProfileAudioRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let stored = resolve_media_file(state, token, &request.audio, "audio")?;

    let normalize_optional_text = |value: Option<String>| {
        value
            .map(|item| item.trim().to_string())
            .filter(|item| !item.is_empty())
    };

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_sim_user_record(&mut conn, request.user_id)?;
    ensure_sim_user_profile_audios_storage(&mut conn)?;

    let title = normalize_optional_text(request.title).unwrap_or_else(|| {
        request
            .file_name
            .as_deref()
            .map(|name| name.trim())
            .filter(|name| !name.is_empty())
            .map(|name| {
                if let Some((base, _)) = name.rsplit_once('.') {
                    base.trim().to_string()
                } else {
                    name.to_string()
                }
            })
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| "Profile audio".to_string())
    });

    let performer = normalize_optional_text(request.performer).or_else(|| Some(user.first_name.clone()));
    let file_name = normalize_optional_text(request.file_name)
        .or_else(|| {
            stored
                .file_path
                .rsplit('/')
                .next()
                .map(str::to_string)
        })
        .unwrap_or_else(|| "profile-audio.ogg".to_string());
    let mime_type = normalize_optional_text(request.mime_type)
        .or_else(|| stored.mime_type.clone())
        .unwrap_or_else(|| "audio/ogg".to_string());
    let duration = request.duration.unwrap_or(30).clamp(1, 3600);
    let now = Utc::now().timestamp();

    conn.execute(
        "UPDATE sim_user_profile_audios
         SET position = position + 1
         WHERE bot_id = ?1 AND user_id = ?2",
        params![bot.id, request.user_id],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "INSERT INTO sim_user_profile_audios
         (bot_id, user_id, file_id, file_unique_id, duration, performer, title, file_name, mime_type, file_size, position, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 0, ?11)",
        params![
            bot.id,
            request.user_id,
            &stored.file_id,
            &stored.file_unique_id,
            duration,
            performer.clone(),
            &title,
            &file_name,
            &mime_type,
            stored.file_size,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!({
        "user_id": request.user_id,
        "file_id": stored.file_id,
        "file_unique_id": stored.file_unique_id,
        "file_path": stored.file_path,
        "title": title,
        "performer": performer,
        "file_name": file_name,
        "mime_type": mime_type,
        "file_size": stored.file_size,
        "duration": duration,
    }))
}

pub fn handle_sim_delete_user_profile_audio(
    state: &Data<AppState>,
    token: &str,
    body: SimDeleteUserProfileAudioRequest,
) -> ApiResult {
    if body.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let file_id = body.file_id.trim();
    if file_id.is_empty() {
        return Err(ApiError::bad_request("file_id is required"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_user_record(&mut conn, body.user_id)?;
    ensure_sim_user_profile_audios_storage(&mut conn)?;

    let deleted = conn
        .execute(
            "DELETE FROM sim_user_profile_audios WHERE bot_id = ?1 AND user_id = ?2 AND file_id = ?3",
            params![bot.id, body.user_id, file_id],
        )
        .map_err(ApiError::internal)?;

    if deleted == 0 {
        return Err(ApiError::not_found("profile audio not found"));
    }

    Ok(json!({
        "deleted": true,
        "user_id": body.user_id,
        "file_id": file_id,
    }))
}

pub fn ensure_sim_user_emoji_statuses_storage(
    conn: &mut rusqlite::Connection,
) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_user_emoji_statuses (
            bot_id                           INTEGER NOT NULL,
            user_id                          INTEGER NOT NULL,
            emoji_status_custom_emoji_id     TEXT,
            emoji_status_expiration_date     INTEGER,
            updated_at                       INTEGER NOT NULL,
            PRIMARY KEY (bot_id, user_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );",
    )
    .map_err(ApiError::internal)
}

pub fn ensure_sim_user_profile_photos_storage(
    conn: &mut rusqlite::Connection,
) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_user_profile_photos (
            bot_id        INTEGER NOT NULL,
            user_id       INTEGER NOT NULL,
            file_id       TEXT NOT NULL,
            file_unique_id TEXT NOT NULL,
            width         INTEGER NOT NULL,
            height        INTEGER NOT NULL,
            file_size     INTEGER,
            position      INTEGER NOT NULL DEFAULT 0,
            created_at    INTEGER NOT NULL,
            PRIMARY KEY (bot_id, user_id, file_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );
        CREATE INDEX IF NOT EXISTS idx_sim_user_profile_photos_order
            ON sim_user_profile_photos (bot_id, user_id, position ASC, created_at DESC, file_id ASC);",
    )
    .map_err(ApiError::internal)
}

pub fn ensure_sim_user_profile_audios_storage(
    conn: &mut rusqlite::Connection,
) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_user_profile_audios (
            bot_id         INTEGER NOT NULL,
            user_id        INTEGER NOT NULL,
            file_id        TEXT NOT NULL,
            file_unique_id TEXT NOT NULL,
            duration       INTEGER NOT NULL,
            performer      TEXT,
            title          TEXT,
            file_name      TEXT,
            mime_type      TEXT,
            file_size      INTEGER,
            position       INTEGER NOT NULL DEFAULT 0,
            created_at     INTEGER NOT NULL,
            PRIMARY KEY (bot_id, user_id, file_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );
        CREATE INDEX IF NOT EXISTS idx_sim_user_profile_audios_order
            ON sim_user_profile_audios (bot_id, user_id, position ASC, created_at DESC, file_id ASC);",
    )
    .map_err(ApiError::internal)
}

pub fn ensure_sim_user_chat_boosts_storage(
    conn: &mut rusqlite::Connection,
) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_user_chat_boosts (
            bot_id           INTEGER NOT NULL,
            chat_key         TEXT NOT NULL,
            user_id          INTEGER NOT NULL,
            boost_id         TEXT NOT NULL,
            add_date         INTEGER NOT NULL,
            expiration_date  INTEGER NOT NULL,
            source_json      TEXT NOT NULL,
            created_at       INTEGER NOT NULL,
            updated_at       INTEGER NOT NULL,
            PRIMARY KEY (bot_id, chat_key, user_id, boost_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );
        CREATE INDEX IF NOT EXISTS idx_sim_user_chat_boosts_lookup
            ON sim_user_chat_boosts (bot_id, chat_key, user_id, expiration_date DESC, add_date DESC);",
    )
    .map_err(ApiError::internal)
}