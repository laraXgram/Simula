use actix_web::web::Data;
use rusqlite::{params, OptionalExtension};
use serde_json::json;
use chrono::Utc;

use crate::database::{
    ensure_bot, lock_db, AppState,
};

use crate::types::{ApiError, ApiResult};

use crate::generated::types::{Update, User, Poll, PollAnswer, PollOption};
use crate::handlers::utils::storage::ensure_sim_verifications_storage;

use crate::handlers::client::types::messages::SimVotePollRequest;

use super::{users, webhook};

pub fn handle_sim_get_poll_voters(
    state: &Data<AppState>,
    token: &str,
    chat_id: i64,
    message_id: i64,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_verifications_storage(&mut conn)?;
    let chat_key = chat_id.to_string();

    let row: Option<(String, i64)> = conn
        .query_row(
            "SELECT id, is_anonymous FROM polls WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, chat_key, message_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((poll_id, is_anonymous)) = row else {
        return Err(ApiError::not_found("poll not found"));
    };

    if is_anonymous == 1 {
        return Ok(json!({
            "poll_id": poll_id,
            "anonymous": true,
            "voters": [],
        }));
    }

    let mut stmt = conn
        .prepare(
            "SELECT v.voter_user_id, u.first_name, u.username, v.option_ids_json
             FROM poll_votes v
             LEFT JOIN users u ON u.id = v.voter_user_id
             WHERE v.poll_id = ?1
             ORDER BY v.updated_at ASC",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![poll_id.clone()], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, String>(3)?,
            ))
        })
        .map_err(ApiError::internal)?;

    let mut voters = Vec::new();
    for row in rows {
        let (user_id, first_name, username, option_ids_json) = row.map_err(ApiError::internal)?;
        let option_ids: Vec<i64> = serde_json::from_str(&option_ids_json).unwrap_or_default();
        voters.push(json!({
            "user_id": user_id,
            "first_name": first_name.unwrap_or_else(|| "User".to_string()),
            "username": username,
            "option_ids": option_ids,
        }));
    }

    Ok(json!({
        "poll_id": poll_id,
        "anonymous": false,
        "voters": voters,
    }))
}

pub fn handle_sim_vote_poll(
    state: &Data<AppState>,
    token: &str,
    body: SimVotePollRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = users::ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;
    let chat_key = body.chat_id.to_string();

    let row: Option<(
        String,
        String,
        String,
        i64,
        i64,
        i64,
        String,
        i64,
        i64,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<i64>,
        i64,
        Option<String>,
        Option<String>,
        Option<String>,
    )> = conn
        .query_row(
            "SELECT p.id, p.question, p.options_json, p.total_voter_count, p.is_closed, p.is_anonymous, p.poll_type,
                    p.allows_multiple_answers, p.allows_revoting, p.correct_option_id, p.correct_option_ids_json,
                    p.explanation, p.description, p.open_period, p.close_date, p.created_at,
                    m.question_entities_json, m.explanation_entities_json, m.description_entities_json
             FROM polls p
             LEFT JOIN poll_metadata m ON m.poll_id = p.id
             WHERE p.bot_id = ?1 AND p.chat_key = ?2 AND p.message_id = ?3",
            params![bot.id, chat_key, body.message_id],
            |r| Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
                r.get(8)?,
                r.get(9)?,
                r.get(10)?,
                r.get(11)?,
                r.get(12)?,
                r.get(13)?,
                r.get(14)?,
                r.get(15)?,
                r.get(16)?,
                r.get(17)?,
                r.get(18)?,
            )),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((poll_id, question, options_json, _total_voter_count, is_closed, is_anonymous, poll_type, allows_multiple_answers, allows_revoting, correct_option_id, correct_option_ids_json, explanation, description, open_period, close_date, created_at, question_entities_json, explanation_entities_json, description_entities_json)) = row else {
        return Err(ApiError::not_found("poll not found"));
    };
    let poll_is_quiz = poll_type == "quiz";
    let effective_allows_revoting = if poll_is_quiz { 0 } else { allows_revoting };

    let now = Utc::now().timestamp();
    let auto_closed = close_date.map(|ts| now >= ts).unwrap_or(false)
        || open_period.map(|p| now >= created_at + p).unwrap_or(false);

    if is_closed == 1 || auto_closed {
        if auto_closed && is_closed == 0 {
            conn.execute(
                "UPDATE polls SET is_closed = 1 WHERE id = ?1 AND bot_id = ?2",
                params![poll_id, bot.id],
            )
            .map_err(ApiError::internal)?;
        }
        return Err(ApiError::bad_request("poll is closed"));
    }

    let mut option_ids = body.option_ids.clone();
    option_ids.sort_unstable();
    option_ids.dedup();

    if poll_is_quiz {
        if option_ids.is_empty() {
            return Err(ApiError::bad_request("quiz polls do not allow vote retraction"));
        }
        if allows_multiple_answers == 0 && option_ids.len() != 1 {
            return Err(ApiError::bad_request("quiz polls accept exactly one option"));
        }
        if allows_revoting != 0 {
            conn.execute(
                "UPDATE polls SET allows_revoting = 0 WHERE id = ?1",
                params![&poll_id],
            )
            .map_err(ApiError::internal)?;
        }
    }

    let existing_vote: Option<Vec<i64>> = conn
        .query_row(
            "SELECT option_ids_json FROM poll_votes WHERE poll_id = ?1 AND voter_user_id = ?2",
            params![poll_id, user.id],
            |r| r.get::<_, String>(0),
        )
        .optional()
        .map_err(ApiError::internal)?
        .map(|raw| serde_json::from_str::<Vec<i64>>(&raw).unwrap_or_default());

    if let Some(previous) = existing_vote.as_ref() {
        if poll_is_quiz {
            if previous != &option_ids {
                return Err(ApiError::bad_request("quiz poll vote cannot be changed"));
            }
        } else if effective_allows_revoting == 0 && previous != &option_ids {
            return Err(ApiError::bad_request("poll vote cannot be changed"));
        }
    }

    let mut options: Vec<PollOption> = serde_json::from_str(&options_json).map_err(ApiError::internal)?;
    let max_index = options.len() as i64;
    if option_ids.iter().any(|v| *v < 0 || *v >= max_index) {
        return Err(ApiError::bad_request("option_ids contains invalid index"));
    }

    if allows_multiple_answers == 0 && option_ids.len() > 1 {
        return Err(ApiError::bad_request("poll accepts only one option"));
    }

    if option_ids.is_empty() {
        conn.execute(
            "DELETE FROM poll_votes WHERE poll_id = ?1 AND voter_user_id = ?2",
            params![poll_id, user.id],
        )
        .map_err(ApiError::internal)?;
    } else {
        conn.execute(
            "INSERT OR REPLACE INTO poll_votes (poll_id, voter_user_id, option_ids_json, updated_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                poll_id,
                user.id,
                serde_json::to_string(&option_ids).map_err(ApiError::internal)?,
                Utc::now().timestamp(),
            ],
        )
        .map_err(ApiError::internal)?;
    }

    let (total_voter_count, counts) = {
        let mut total_voter_count: i64 = 0;
        let mut counts = vec![0i64; options.len()];
        let mut stmt = conn
            .prepare("SELECT option_ids_json FROM poll_votes WHERE poll_id = ?1")
            .map_err(ApiError::internal)?;
        let rows = stmt
            .query_map(params![poll_id], |r| r.get::<_, String>(0))
            .map_err(ApiError::internal)?;

        for row in rows {
            let raw = row.map_err(ApiError::internal)?;
            let ids: Vec<i64> = serde_json::from_str(&raw).unwrap_or_default();
            total_voter_count += 1;
            for id in ids {
                if let Some(slot) = counts.get_mut(id as usize) {
                    *slot += 1;
                }
            }
        }

        (total_voter_count, counts)
    };

    for (idx, option) in options.iter_mut().enumerate() {
        option.voter_count = counts[idx];
    }

    conn.execute(
        "UPDATE polls SET options_json = ?1, total_voter_count = ?2 WHERE id = ?3",
        params![serde_json::to_string(&options).map_err(ApiError::internal)?, total_voter_count, poll_id],
    )
    .map_err(ApiError::internal)?;

    let correct_option_ids = correct_option_ids_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Vec<i64>>(raw).ok())
        .or_else(|| correct_option_id.map(|id| vec![id]));
    let poll = Poll {
        id: poll_id.clone(),
        question,
        question_entities: question_entities_json
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok()),
        options,
        total_voter_count,
        is_closed: false,
        is_anonymous: is_anonymous == 1,
        r#type: poll_type,
        allows_multiple_answers: allows_multiple_answers == 1,
        allows_revoting: effective_allows_revoting == 1,
        correct_option_ids,
        explanation,
        explanation_entities: explanation_entities_json
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok()),
        open_period,
        close_date,
        description,
        description_entities: description_entities_json
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok()),
    };

    let poll_update = serde_json::to_value(Update {
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
        poll: Some(poll.clone()),
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;
    webhook::persist_and_dispatch_update(state, &mut conn, token, bot.id, poll_update)?;

    if is_anonymous == 1 {
        return Ok(serde_json::to_value(true).map_err(ApiError::internal)?);
    }

    let option_persistent_ids = option_ids
        .iter()
        .filter_map(|id| poll.options.get(*id as usize).map(|option| option.persistent_id.clone()))
        .collect::<Vec<String>>();

    let poll_answer_update = serde_json::to_value(Update {
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
        poll_answer: Some(PollAnswer {
            poll_id,
            voter_chat: None,
            user: Some(User {
                id: user.id,
                is_bot: false,
                first_name: user.first_name,
                last_name: None,
                username: user.username,
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
            }),
            option_ids,
            option_persistent_ids,
        }),
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;
    webhook::persist_and_dispatch_update(state, &mut conn, token, bot.id, poll_answer_update)?;

    Ok(serde_json::to_value(true).map_err(ApiError::internal)?)
}