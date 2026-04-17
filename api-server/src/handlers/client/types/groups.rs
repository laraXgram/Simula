use serde::{Deserialize, Serialize};

use crate::generated::types::{Chat, ChatPermissions, User};

#[derive(Debug)]
pub struct GroupRuntimeSettings {
    pub chat_type: String,
    pub slow_mode_delay: i64,
    pub permissions: ChatPermissions,
}

#[derive(Deserialize)]
pub struct SimCreateGroupRequest {
    pub title: String,
    pub chat_type: Option<String>,
    pub owner_user_id: Option<i64>,
    pub owner_first_name: Option<String>,
    pub owner_username: Option<String>,
    pub initial_member_ids: Option<Vec<i64>>,
    pub username: Option<String>,
    pub description: Option<String>,
    pub is_forum: Option<bool>,
    pub show_author_signature: Option<bool>,
    pub message_history_visible: Option<bool>,
    pub slow_mode_delay: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimJoinGroupRequest {
    pub chat_id: i64,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
}

#[derive(Deserialize)]
pub struct SimLeaveGroupRequest {
    pub chat_id: i64,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
}

#[derive(Deserialize)]
pub struct SimUpdateGroupRequest {
    pub chat_id: i64,
    pub user_id: Option<i64>,
    pub actor_first_name: Option<String>,
    pub actor_username: Option<String>,
    pub chat_type: Option<String>,
    pub title: Option<String>,
    pub username: Option<String>,
    pub description: Option<String>,
    pub is_forum: Option<bool>,
    pub show_author_signature: Option<bool>,
    pub paid_star_reactions_enabled: Option<bool>,
    pub linked_chat_id: Option<i64>,
    pub direct_messages_enabled: Option<bool>,
    pub direct_messages_star_count: Option<i64>,
    pub message_history_visible: Option<bool>,
    pub slow_mode_delay: Option<i64>,
    pub permissions: Option<ChatPermissions>,
}

#[derive(Deserialize)]
pub struct SimDeleteGroupRequest {
    pub chat_id: i64,
    pub user_id: Option<i64>,
    pub actor_first_name: Option<String>,
    pub actor_username: Option<String>,
}

#[derive(Deserialize)]
pub struct SimCreateGroupInviteLinkRequest {
    pub chat_id: i64,
    pub user_id: Option<i64>,
    pub actor_first_name: Option<String>,
    pub actor_username: Option<String>,
    pub creates_join_request: Option<bool>,
    pub name: Option<String>,
    pub expire_date: Option<i64>,
    pub member_limit: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimJoinGroupByInviteLinkRequest {
    pub invite_link: String,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
}

#[derive(Deserialize)]
pub struct SimSetBotGroupMembershipRequest {
    pub chat_id: i64,
    pub actor_user_id: Option<i64>,
    pub actor_first_name: Option<String>,
    pub actor_username: Option<String>,
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct SimGroupSettingsResponse {
    pub show_author_signature: bool,
    pub paid_star_reactions_enabled: bool,
    pub message_history_visible: bool,
    pub slow_mode_delay: i64,
    pub permissions: ChatPermissions,
}

#[derive(Debug, Serialize)]
pub struct SimCreateGroupResponse {
    pub chat: Chat,
    pub owner: User,
    pub members: Vec<User>,
    pub settings: SimGroupSettingsResponse,
}

#[derive(Debug, Clone)]
pub struct SimChatInviteLinkRecord {
    pub invite_link: String,
    pub creator_user_id: i64,
    pub creates_join_request: bool,
    pub is_primary: bool,
    pub is_revoked: bool,
    pub name: Option<String>,
    pub expire_date: Option<i64>,
    pub member_limit: Option<i64>,
    pub subscription_period: Option<i64>,
    pub subscription_price: Option<i64>,
}
