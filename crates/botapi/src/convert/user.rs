//! TL `user` → Bot API `User`.

use simula_botapi_types::types as bot;
use simula_tl::types;

/// Bot-only capability fields are "returned only in getMe".
pub fn user(u: &types::User, for_get_me: bool) -> bot::User {
    let bot_field = |value: bool| (for_get_me && u.bot).then_some(value);
    bot::User {
        id: u.id,
        is_bot: u.bot,
        first_name: u.first_name.clone().unwrap_or_default(),
        last_name: u.last_name.clone(),
        username: u.username.clone(),
        language_code: u.lang_code.clone(),
        is_premium: u.premium,
        added_to_attachment_menu: u.attach_menu_enabled,
        can_join_groups: bot_field(!u.bot_nochats),
        can_read_all_group_messages: bot_field(u.bot_chat_history),
        supports_inline_queries: bot_field(u.bot_inline_placeholder.is_some()),
        can_connect_to_business: bot_field(u.bot_business),
        has_main_web_app: bot_field(u.bot_has_main_app),
        has_topics_enabled: bot_field(u.bot_forum_view),
        allows_users_to_create_topics: bot_field(u.bot_forum_can_manage_topics),
        can_manage_bots: bot_field(u.bot_can_manage_bots),
        supports_guest_queries: bot_field(u.bot_guestchat),
        supports_join_request_queries: bot_field(false),
    }
}
