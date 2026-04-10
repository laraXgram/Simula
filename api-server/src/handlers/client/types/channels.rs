use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct LinkedDiscussionTransportContext {
    pub channel_chat_key: String,
    pub channel_message_id: i64,
    pub discussion_root_message_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct ChannelAdminRights {
    pub can_manage_chat: bool,
    pub can_post_messages: bool,
    pub can_edit_messages: bool,
    pub can_delete_messages: bool,
    pub can_invite_users: bool,
    pub can_change_info: bool,
    pub can_manage_direct_messages: bool,
}

#[derive(Deserialize)]
pub struct SimOpenChannelDirectMessagesRequest {
    pub channel_chat_id: i64,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
}

#[derive(Deserialize)]
pub struct SimMarkChannelMessageViewRequest {
    pub chat_id: i64,
    pub message_id: i64,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
}

#[derive(Deserialize)]
pub struct SimResolveJoinRequestRequest {
    pub chat_id: i64,
    pub user_id: i64,
    pub actor_user_id: Option<i64>,
    pub actor_first_name: Option<String>,
    pub actor_username: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SimDirectMessagesTopicRecord {
    pub topic_id: i64,
    pub user_id: i64,
    pub created_at: i64,
    pub updated_at: i64,
    pub last_message_id: Option<i64>,
    pub last_message_date: Option<i64>,
}
