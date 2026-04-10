use serde::Deserialize;

use crate::generated::types::BusinessBotRights;

#[derive(Debug, Clone)]
pub struct SimBusinessConnectionRecord {
    pub connection_id: String,
    pub user_id: i64,
    pub user_chat_id: i64,
    pub rights_json: String,
    pub is_enabled: bool,
    pub gift_settings_show_button: bool,
    pub gift_settings_types_json: Option<String>,
    pub star_balance: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone)]
pub struct SimBusinessProfileRecord {
    pub last_name: Option<String>,
    pub bio: Option<String>,
    pub profile_photo_file_id: Option<String>,
    pub public_profile_photo_file_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SimBusinessStoryRecord {
    pub business_connection_id: String,
    pub story_id: i64,
    pub owner_chat_id: i64,
    pub content_json: String,
    pub caption: Option<String>,
    pub caption_entities_json: Option<String>,
    pub areas_json: Option<String>,
}

#[derive(Deserialize)]
pub struct SimSetBusinessConnectionRequest {
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub business_connection_id: Option<String>,
    pub enabled: Option<bool>,
    pub rights: Option<BusinessBotRights>,
}

#[derive(Deserialize)]
pub struct SimRemoveBusinessConnectionRequest {
    pub user_id: Option<i64>,
    pub business_connection_id: Option<String>,
}