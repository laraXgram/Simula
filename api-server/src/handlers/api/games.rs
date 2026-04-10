use super::*;
use crate::generated::methods::{
    GetGameHighScoresRequest, SetGameScoreRequest,
};

use crate::handlers::client::messages;

pub fn handle_get_game_high_scores(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetGameHighScoresRequest = parse_request(params)?;
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let (chat_key, message_id) = resolve_game_target_message(
        &mut conn,
        bot.id,
        request.chat_id,
        request.message_id,
        request.inline_message_id.as_deref(),
    )?;

    let target_message = messages::load_message_value(&mut conn, &bot, message_id)?;
    if target_message.get("game").is_none() {
        return Err(ApiError::bad_request("target message is not a game message"));
    }

    let mut stmt = conn
        .prepare(
            "SELECT gs.user_id, gs.score, u.first_name, u.username
             FROM game_scores gs
             LEFT JOIN users u ON u.id = gs.user_id
             WHERE gs.bot_id = ?1 AND gs.chat_key = ?2 AND gs.message_id = ?3
             ORDER BY gs.score DESC, gs.updated_at ASC, gs.user_id ASC",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![bot.id, chat_key, message_id], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, Option<String>>(3)?,
            ))
        })
        .map_err(ApiError::internal)?;

    let mut scores: Vec<GameHighScore> = Vec::new();
    for (idx, row) in rows.enumerate() {
        let (user_id, score, first_name, username) = row.map_err(ApiError::internal)?;
        scores.push(GameHighScore {
            position: idx as i64 + 1,
            user: User {
                id: user_id,
                is_bot: false,
                first_name: first_name.unwrap_or_else(|| format!("User {}", user_id)),
                last_name: None,
                username,
                language_code: None,
                is_premium: None,
                added_to_attachment_menu: None,
                can_join_groups: None,
                can_read_all_group_messages: None,
                supports_inline_queries: None,
                can_connect_to_business: None,
                has_main_web_app: None,
                has_topics_enabled: None,
                allows_users_to_create_topics: None,
        can_manage_bots: None,
            },
            score,
        });
    }

    Ok(serde_json::to_value(scores).map_err(ApiError::internal)?)
}

pub fn handle_set_game_score(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetGameScoreRequest = parse_request(params)?;
    if request.score < 0 {
        return Err(ApiError::bad_request("score must be non-negative"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let (chat_key, message_id) = resolve_game_target_message(
        &mut conn,
        bot.id,
        request.chat_id,
        request.message_id,
        request.inline_message_id.as_deref(),
    )?;

    let mut target_message = messages::load_message_value(&mut conn, &bot, message_id)?;
    if target_message.get("game").is_none() {
        return Err(ApiError::bad_request("target message is not a game message"));
    }

    ensure_user(&mut conn, Some(request.user_id), None, None)?;

    let existing_score: Option<i64> = conn
        .query_row(
            "SELECT score FROM game_scores WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3 AND user_id = ?4",
            params![bot.id, chat_key, message_id, request.user_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let can_override = request.force.unwrap_or(false);
    if let Some(current) = existing_score {
        if request.score < current && !can_override {
            return Err(ApiError::bad_request("new score must be greater than or equal to current score unless force is true"));
        }
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO game_scores (bot_id, chat_key, message_id, user_id, score, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(bot_id, chat_key, message_id, user_id)
         DO UPDATE SET score = excluded.score, updated_at = excluded.updated_at",
        params![bot.id, chat_key, message_id, request.user_id, request.score, now],
    )
    .map_err(ApiError::internal)?;

    if request.inline_message_id.is_some() {
      return Ok(json!(true));
    }

    if request.disable_edit_message.unwrap_or(false) {
        return Ok(json!(true));
    }

    target_message["edit_date"] = json!(now);
    let update_value = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: serde_json::from_value(target_message.clone()).ok(),
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
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;

    persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;
    Ok(target_message)
}
