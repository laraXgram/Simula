use crate::types::ApiError;
use crate::generated::types::User;
use super::{chats, groups};

pub fn ensure_bot_is_chat_admin_or_owner(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
) -> Result<(), ApiError> {
    let bot_status = chats::load_chat_member_status(conn, bot_id, chat_key, bot_id)?;
    if !bot_status
        .as_deref()
        .map(groups::is_group_admin_or_owner_status)
        .unwrap_or(false)
    {
        return Err(ApiError::bad_request(
            "bot is not an administrator in this chat",
        ));
    }

    Ok(())
}

pub fn build_bot_user(bot: &crate::database::BotInfoRecord) -> User {
    User {
        id: bot.id,
        is_bot: true,
        first_name: bot.first_name.clone(),
        last_name: None,
        username: Some(bot.username.clone()),
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
    }
}

