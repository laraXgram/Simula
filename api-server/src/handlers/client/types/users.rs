use serde::Deserialize;
use serde_json::Value;

#[derive(Debug)]
pub struct SimUserRecord {
    pub id: i64,
    pub first_name: String,
    pub username: Option<String>,
    pub last_name: Option<String>,
    pub is_premium: bool,
}

#[derive(Deserialize)]
pub struct SimUpsertUserRequest {
    pub id: Option<i64>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub username: Option<String>,
    pub phone_number: Option<String>,
    pub photo_url: Option<String>,
    pub bio: Option<String>,
    pub is_premium: Option<bool>,
    pub business_name: Option<String>,
    pub business_intro: Option<String>,
    pub business_location: Option<String>,
    pub gift_count: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimDeleteUserRequest {
    pub id: i64,
}

#[derive(Deserialize)]
pub struct SimSetUserProfileAudioRequest {
    pub user_id: i64,
    pub title: Option<String>,
    pub performer: Option<String>,
    pub file_name: Option<String>,
    pub mime_type: Option<String>,
    pub file_size: Option<i64>,
    pub duration: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimDeleteUserProfileAudioRequest {
    pub user_id: i64,
    pub file_id: String,
}

#[derive(Deserialize)]
pub struct SimUploadUserProfileAudioRequest {
    pub user_id: i64,
    pub audio: Value,
    pub title: Option<String>,
    pub performer: Option<String>,
    pub file_name: Option<String>,
    pub mime_type: Option<String>,
    pub duration: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimAddUserChatBoostsRequest {
    pub chat_id: i64,
    pub user_id: i64,
    pub count: Option<i64>,
    pub duration_days: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimRemoveUserChatBoostsRequest {
    pub chat_id: i64,
    pub user_id: i64,
    pub boost_ids: Option<Vec<String>>,
    pub remove_all: Option<bool>,
}