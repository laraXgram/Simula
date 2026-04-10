#[derive(Debug, Clone)]
pub struct SimChatRecord {
    pub chat_key: String,
    pub chat_id: i64,
    pub chat_type: String,
    pub title: Option<String>,
    pub username: Option<String>,
    pub is_forum: bool,
    pub is_direct_messages: bool,
    pub parent_channel_chat_id: Option<i64>,
    pub direct_messages_enabled: bool,
    pub direct_messages_star_count: i64,
    pub channel_show_author_signature: bool,
    pub channel_paid_reactions_enabled: bool,
    pub linked_discussion_chat_id: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct SimChatMemberRecord {
    pub status: String,
    pub role: String,
    pub permissions_json: Option<String>,
    pub admin_rights_json: Option<String>,
    pub until_date: Option<i64>,
    pub custom_title: Option<String>,
    pub tag: Option<String>,
    pub joined_at: Option<i64>,
}
