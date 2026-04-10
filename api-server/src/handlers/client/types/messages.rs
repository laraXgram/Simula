use serde::Deserialize;
use serde_json::Value;

use crate::generated::types::{
    ChatShared, KeyboardButtonRequestManagedBot,
    SuggestedPostParameters, UsersShared, WebAppData
};

#[derive(Deserialize)]
pub struct SimSendUserMessageRequest {
    pub chat_id: Option<i64>,
    pub message_thread_id: Option<i64>,
    pub direct_messages_topic_id: Option<i64>,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub sender_chat_id: Option<i64>,
    pub business_connection_id: Option<String>,
    pub text: String,
    pub parse_mode: Option<String>,
    pub suggested_post_parameters: Option<SuggestedPostParameters>,
    pub reply_to_message_id: Option<i64>,
    pub users_shared: Option<UsersShared>,
    pub chat_shared: Option<ChatShared>,
    pub web_app_data: Option<WebAppData>,
    pub managed_bot_request: Option<KeyboardButtonRequestManagedBot>,
}

#[derive(Deserialize)]
pub struct SimSendUserMediaRequest {
    pub chat_id: Option<i64>,
    pub message_thread_id: Option<i64>,
    pub direct_messages_topic_id: Option<i64>,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub sender_chat_id: Option<i64>,
    pub business_connection_id: Option<String>,
    pub media_kind: String,
    pub media: Value,
    pub caption: Option<String>,
    pub parse_mode: Option<String>,
    pub reply_to_message_id: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimEditUserMediaRequest {
    pub chat_id: i64,
    pub message_id: i64,
    pub media_kind: String,
    pub media: Value,
    pub caption: Option<Value>,
    pub parse_mode: Option<String>,
}

#[derive(Deserialize)]
pub struct SimSendUserDiceRequest {
    pub chat_id: Option<i64>,
    pub message_thread_id: Option<i64>,
    pub direct_messages_topic_id: Option<i64>,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub sender_chat_id: Option<i64>,
    pub emoji: Option<String>,
    pub reply_to_message_id: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimSendUserGameRequest {
    pub chat_id: Option<i64>,
    pub message_thread_id: Option<i64>,
    pub direct_messages_topic_id: Option<i64>,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub sender_chat_id: Option<i64>,
    pub game_short_name: String,
    pub reply_to_message_id: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimSendUserContactRequest {
    pub chat_id: Option<i64>,
    pub message_thread_id: Option<i64>,
    pub direct_messages_topic_id: Option<i64>,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub sender_chat_id: Option<i64>,
    pub phone_number: String,
    pub contact_first_name: String,
    pub contact_last_name: Option<String>,
    pub vcard: Option<String>,
    pub reply_to_message_id: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimSendUserLocationRequest {
    pub chat_id: Option<i64>,
    pub message_thread_id: Option<i64>,
    pub direct_messages_topic_id: Option<i64>,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub sender_chat_id: Option<i64>,
    pub latitude: f64,
    pub longitude: f64,
    pub horizontal_accuracy: Option<f64>,
    pub live_period: Option<i64>,
    pub heading: Option<i64>,
    pub proximity_alert_radius: Option<i64>,
    pub reply_to_message_id: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimSendUserVenueRequest {
    pub chat_id: Option<i64>,
    pub message_thread_id: Option<i64>,
    pub direct_messages_topic_id: Option<i64>,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub sender_chat_id: Option<i64>,
    pub latitude: f64,
    pub longitude: f64,
    pub title: String,
    pub address: String,
    pub foursquare_id: Option<String>,
    pub foursquare_type: Option<String>,
    pub google_place_id: Option<String>,
    pub google_place_type: Option<String>,
    pub reply_to_message_id: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimClearHistoryRequest {
    pub chat_id: i64,
    pub message_thread_id: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimSetUserReactionRequest {
    pub chat_id: i64,
    pub message_id: i64,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub reaction: Option<Vec<Value>>,
}

#[derive(Deserialize)]
pub struct SimVotePollRequest {
    pub chat_id: i64,
    pub message_id: i64,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub option_ids: Vec<i64>,
}

#[derive(Debug)]
pub struct StoredFile {
    pub file_id: String,
    pub file_unique_id: String,
    pub file_path: String,
    pub mime_type: Option<String>,
    pub file_size: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct StickerMeta {
    pub set_name: Option<String>,
    pub sticker_type: String,
    pub format: String,
    pub emoji: Option<String>,
    pub mask_position_json: Option<String>,
    pub custom_emoji_id: Option<String>,
    pub needs_repainting: bool,
}