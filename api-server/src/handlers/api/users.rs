use super::*;
use crate::generated::methods::{
    EditUserStarSubscriptionRequest,
    GetUserProfileAudiosRequest, GetUserProfilePhotosRequest, GetUserChatBoostsRequest,
    RemoveUserVerificationRequest, SetUserEmojiStatusRequest, VerifyUserRequest,
};

use crate::handlers::client::{chats, users};

pub fn handle_edit_user_star_subscription(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditUserStarSubscriptionRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let now = Utc::now().timestamp();

    conn.execute(
        "INSERT INTO star_subscriptions
         (bot_id, user_id, telegram_payment_charge_id, is_canceled, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(bot_id, user_id, telegram_payment_charge_id)
         DO UPDATE SET is_canceled = excluded.is_canceled, updated_at = excluded.updated_at",
        params![
            bot.id,
            request.user_id,
            request.telegram_payment_charge_id,
            if request.is_canceled { 1 } else { 0 },
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_get_user_profile_audios(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetUserProfileAudiosRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }
    let (offset, limit) = normalize_profile_pagination(request.offset, request.limit)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    users::ensure_sim_user_record(&mut conn, request.user_id)?;
    ensure_sim_user_profile_audios_storage(&mut conn)?;

    let mut stmt = conn
        .prepare(
            "SELECT file_id, file_unique_id, duration, performer, title, file_name, mime_type, file_size
             FROM sim_user_profile_audios
             WHERE bot_id = ?1 AND user_id = ?2
             ORDER BY position ASC, created_at DESC, file_id ASC",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![bot.id, request.user_id], |row| {
            Ok(Audio {
                file_id: row.get(0)?,
                file_unique_id: row.get(1)?,
                duration: row.get(2)?,
                performer: row.get(3)?,
                title: row.get(4)?,
                file_name: row.get(5)?,
                mime_type: row.get(6)?,
                file_size: row.get(7)?,
                thumbnail: None,
            })
        })
        .map_err(ApiError::internal)?;

    let all_audios = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    let total_count = all_audios.len() as i64;

    let audios = all_audios
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<Audio>>();

    serde_json::to_value(UserProfileAudios { total_count, audios }).map_err(ApiError::internal)
}

pub fn handle_get_user_profile_photos(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetUserProfilePhotosRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }
    let (offset, limit) = normalize_profile_pagination(request.offset, request.limit)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let _ = users::ensure_sim_user_record(&mut conn, request.user_id)?;
    ensure_sim_user_profile_photos_storage(&mut conn)?;

    let existing_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sim_user_profile_photos WHERE bot_id = ?1 AND user_id = ?2",
            params![bot.id, request.user_id],
            |row| row.get(0),
        )
        .map_err(ApiError::internal)?;

    if existing_count == 0 {
        let user_photo_url: Option<String> = conn
            .query_row(
                "SELECT photo_url FROM users WHERE id = ?1",
                params![request.user_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?
            .flatten();

        let business_profile_photo_id: Option<String> = conn
            .query_row(
                "SELECT COALESCE(public_profile_photo_file_id, profile_photo_file_id)
                 FROM sim_business_account_profiles
                 WHERE bot_id = ?1 AND user_id = ?2",
                params![bot.id, request.user_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?
            .flatten();

        if user_photo_url
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_some()
            || business_profile_photo_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_some()
        {
            let now = Utc::now().timestamp();
            let file_id = business_profile_photo_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| generate_telegram_file_id("profile_photo"));

            let file_unique_id = generate_telegram_file_unique_id();
            conn.execute(
                "INSERT OR IGNORE INTO sim_user_profile_photos
                 (bot_id, user_id, file_id, file_unique_id, width, height, file_size, position, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, 0, ?7)",
                params![bot.id, request.user_id, file_id, file_unique_id, 640, 640, now],
            )
            .map_err(ApiError::internal)?;
        }
    }

    let mut stmt = conn
        .prepare(
            "SELECT file_id, file_unique_id, width, height, file_size
             FROM sim_user_profile_photos
             WHERE bot_id = ?1 AND user_id = ?2
             ORDER BY position ASC, created_at DESC, file_id ASC",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![bot.id, request.user_id], |row| {
            Ok(PhotoSize {
                file_id: row.get(0)?,
                file_unique_id: row.get(1)?,
                width: row.get(2)?,
                height: row.get(3)?,
                file_size: row.get(4)?,
            })
        })
        .map_err(ApiError::internal)?;

    let all_photos = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    let total_count = all_photos.len() as i64;

    let photos = all_photos
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|photo| vec![photo])
        .collect::<Vec<Vec<PhotoSize>>>();

    serde_json::to_value(UserProfilePhotos { total_count, photos }).map_err(ApiError::internal)
}

pub fn handle_get_user_chat_boosts(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetUserChatBoostsRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_user_chat_boosts_storage(&mut conn)?;

    let (chat_key, _sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;
    chats::ensure_sender_is_chat_member(&mut conn, bot.id, &chat_key, request.user_id)?;

    users::ensure_sim_user_record(&mut conn, request.user_id)?;

    let mut stmt = conn
        .prepare(
            "SELECT boost_id, add_date, expiration_date, source_json
             FROM sim_user_chat_boosts
             WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3
             ORDER BY expiration_date DESC, add_date DESC, boost_id ASC",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![bot.id, &chat_key, request.user_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(ApiError::internal)?;

    let mut boosts = Vec::<ChatBoost>::new();
    for row in rows {
        let (boost_id, add_date, expiration_date, source_json) = row.map_err(ApiError::internal)?;
        let source = serde_json::from_str::<ChatBoostSource>(&source_json)
            .map_err(ApiError::internal)?;
        boosts.push(ChatBoost {
            boost_id,
            add_date,
            expiration_date,
            source,
        });
    }

    serde_json::to_value(UserChatBoosts { boosts }).map_err(ApiError::internal)
}

pub fn handle_remove_user_verification(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: RemoveUserVerificationRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_verifications_storage(&mut conn)?;

    conn.execute(
        "DELETE FROM sim_user_verifications WHERE bot_id = ?1 AND user_id = ?2",
        params![bot.id, request.user_id],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_set_user_emoji_status(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetUserEmojiStatusRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let custom_emoji_id = request
        .emoji_status_custom_emoji_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    if request.emoji_status_expiration_date.is_some() && custom_emoji_id.is_none() {
        return Err(ApiError::bad_request(
            "emoji_status_expiration_date requires emoji_status_custom_emoji_id",
        ));
    }

    let now = Utc::now().timestamp();
    if let Some(expiration_date) = request.emoji_status_expiration_date {
        if expiration_date <= now {
            return Err(ApiError::bad_request(
                "emoji_status_expiration_date must be in the future",
            ));
        }
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let _ = users::ensure_sim_user_record(&mut conn, request.user_id)?;
    ensure_sim_user_emoji_statuses_storage(&mut conn)?;

    conn.execute(
        "INSERT INTO sim_user_emoji_statuses
         (bot_id, user_id, emoji_status_custom_emoji_id, emoji_status_expiration_date, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(bot_id, user_id)
         DO UPDATE SET
            emoji_status_custom_emoji_id = excluded.emoji_status_custom_emoji_id,
            emoji_status_expiration_date = excluded.emoji_status_expiration_date,
            updated_at = excluded.updated_at",
        params![
            bot.id,
            request.user_id,
            custom_emoji_id,
            request.emoji_status_expiration_date,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_verify_user(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: VerifyUserRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let custom_description =
        normalize_verification_custom_description(request.custom_description.as_deref())?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_verifications_storage(&mut conn)?;
    users::ensure_sim_user_record(&mut conn, request.user_id)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_user_verifications
         (bot_id, user_id, custom_description, verified_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?4)
         ON CONFLICT(bot_id, user_id)
         DO UPDATE SET
             custom_description = excluded.custom_description,
             verified_at = excluded.verified_at,
             updated_at = excluded.updated_at",
        params![bot.id, request.user_id, custom_description, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}
