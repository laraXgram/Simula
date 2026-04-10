use super::*;
use crate::generated::methods::{
    DeleteStoryRequest, EditStoryRequest, PostStoryRequest, RepostStoryRequest
};

pub fn handle_delete_story(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: DeleteStoryRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (record, _connection) = resolve_story_business_connection_for_request(
        &mut conn,
        bot.id,
        Some(request.business_connection_id.as_str()),
    )?;

    ensure_sim_story_storage(&mut conn)?;
    let deleted = conn
        .execute(
            "DELETE FROM sim_business_stories
             WHERE bot_id = ?1 AND business_connection_id = ?2 AND story_id = ?3",
            params![bot.id, &record.connection_id, request.story_id],
        )
        .map_err(ApiError::internal)?;

    if deleted == 0 {
        return Err(ApiError::bad_request("story was not found"));
    }

    Ok(json!(true))
}

pub fn handle_edit_story(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditStoryRequest =
        parse_request_ignoring_prefixed_fields(params, &["story_file"])?;

    let normalized_story_content = normalize_story_content_payload(
        state,
        token,
        params,
        &request.content.extra,
    )?;

    validate_story_content_payload(&normalized_story_content)?;
    validate_story_areas_payload(request.areas.as_ref())?;

    let explicit_caption_entities = request
        .caption_entities
        .as_ref()
        .map(|entities| serde_json::to_value(entities).map_err(ApiError::internal))
        .transpose()?;
    let (caption, caption_entities) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_caption_entities,
    );

    if let Some(value) = caption.as_ref() {
        if value.chars().count() > 2048 {
            return Err(ApiError::bad_request("caption is too long"));
        }
    }

    let normalized_content = crate::generated::types::InputStoryContent {
        extra: normalized_story_content.clone(),
    };

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (record, connection) = resolve_story_business_connection_for_request(
        &mut conn,
        bot.id,
        Some(request.business_connection_id.as_str()),
    )?;

    ensure_sim_story_storage(&mut conn)?;
    let existing = load_story_record_for_connection(
        &mut conn,
        bot.id,
        &record.connection_id,
        request.story_id,
    )?;
    if existing.is_none() {
        return Err(ApiError::bad_request("story was not found"));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_business_stories
         SET content_json = ?1,
             caption = ?2,
             caption_entities_json = ?3,
             areas_json = ?4,
             updated_at = ?5
         WHERE bot_id = ?6 AND business_connection_id = ?7 AND story_id = ?8",
        params![
            serde_json::to_string(&normalized_content).map_err(ApiError::internal)?,
            caption,
            caption_entities.map(|value| value.to_string()),
            request
                .areas
                .as_ref()
                .map(|areas| serde_json::to_string(areas).map_err(ApiError::internal))
                .transpose()?,
            now,
            bot.id,
            &record.connection_id,
            request.story_id,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(build_story_response_payload(
        story_chat_for_business_connection(&connection),
        request.story_id,
        Some(&normalized_story_content),
        caption.as_deref(),
    ))
}

pub fn handle_post_story(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: PostStoryRequest =
        parse_request_ignoring_prefixed_fields(params, &["story_file"])?;

    let normalized_story_content = normalize_story_content_payload(
        state,
        token,
        params,
        &request.content.extra,
    )?;

    ensure_story_active_period(request.active_period)?;
    validate_story_content_payload(&normalized_story_content)?;
    validate_story_areas_payload(request.areas.as_ref())?;

    let explicit_caption_entities = request
        .caption_entities
        .as_ref()
        .map(|entities| serde_json::to_value(entities).map_err(ApiError::internal))
        .transpose()?;
    let (caption, caption_entities) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_caption_entities,
    );

    if let Some(value) = caption.as_ref() {
        if value.chars().count() > 2048 {
            return Err(ApiError::bad_request("caption is too long"));
        }
    }

    let normalized_content = crate::generated::types::InputStoryContent {
        extra: normalized_story_content.clone(),
    };

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (record, connection) = resolve_story_business_connection_for_request(
        &mut conn,
        bot.id,
        Some(request.business_connection_id.as_str()),
    )?;

    ensure_sim_story_storage(&mut conn)?;
    let story_id = next_story_id_for_connection(&mut conn, bot.id, &record.connection_id)?;
    let now = Utc::now().timestamp();

    conn.execute(
        "INSERT INTO sim_business_stories
         (bot_id, business_connection_id, story_id, owner_chat_id,
          content_json, caption, caption_entities_json, areas_json,
          active_period, post_to_chat_page, protect_content,
          source_chat_id, source_story_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4,
                 ?5, ?6, ?7, ?8,
                 ?9, ?10, ?11,
                 NULL, NULL, ?12, ?12)",
        params![
            bot.id,
            &record.connection_id,
            story_id,
            connection.user.id,
            serde_json::to_string(&normalized_content).map_err(ApiError::internal)?,
            caption,
            caption_entities.map(|value| value.to_string()),
            request
                .areas
                .as_ref()
                .map(|areas| serde_json::to_string(areas).map_err(ApiError::internal))
                .transpose()?,
            request.active_period,
            if request.post_to_chat_page.unwrap_or(false) {
                1
            } else {
                0
            },
            if request.protect_content.unwrap_or(false) {
                1
            } else {
                0
            },
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(build_story_response_payload(
        story_chat_for_business_connection(&connection),
        story_id,
        Some(&normalized_story_content),
        caption.as_deref(),
    ))
}

pub fn handle_repost_story(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: RepostStoryRequest = parse_request(params)?;
    ensure_story_active_period(request.active_period)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (target_record, target_connection) = resolve_story_business_connection_for_request(
        &mut conn,
        bot.id,
        Some(request.business_connection_id.as_str()),
    )?;

    let source_connection_record = load_sim_business_connection_for_user(
        &mut conn,
        bot.id,
        request.from_chat_id,
    )?
    .ok_or_else(|| ApiError::bad_request("source business account is not managed by this bot"))?;
    let source_connection =
        build_business_connection(&mut conn, bot.id, &source_connection_record)?;
    ensure_business_right(
        &source_connection,
        |rights| rights.can_manage_stories,
        "not enough rights to manage source stories",
    )?;

    ensure_sim_story_storage(&mut conn)?;
    let source_story = load_story_record_for_chat(
        &mut conn,
        bot.id,
        request.from_chat_id,
        request.from_story_id,
    )?
    .ok_or_else(|| ApiError::bad_request("source story was not found"))?;

    let story_id = next_story_id_for_connection(&mut conn, bot.id, &target_record.connection_id)?;
    let now = Utc::now().timestamp();

    conn.execute(
        "INSERT INTO sim_business_stories
         (bot_id, business_connection_id, story_id, owner_chat_id,
          content_json, caption, caption_entities_json, areas_json,
          active_period, post_to_chat_page, protect_content,
          source_chat_id, source_story_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4,
                 ?5, ?6, ?7, ?8,
                 ?9, ?10, ?11,
                 ?12, ?13, ?14, ?14)",
        params![
            bot.id,
            &target_record.connection_id,
            story_id,
            target_connection.user.id,
            &source_story.content_json,
            &source_story.caption,
            &source_story.caption_entities_json,
            &source_story.areas_json,
            request.active_period,
            if request.post_to_chat_page.unwrap_or(false) {
                1
            } else {
                0
            },
            if request.protect_content.unwrap_or(false) {
                1
            } else {
                0
            },
            request.from_chat_id,
            request.from_story_id,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    let source_content_value = serde_json::from_str::<Value>(&source_story.content_json)
        .unwrap_or_else(|_| Value::Null);
    Ok(build_story_response_payload(
        story_chat_for_business_connection(&target_connection),
        story_id,
        if source_content_value.is_null() {
            None
        } else {
            Some(&source_content_value)
        },
        source_story.caption.as_deref(),
    ))
}