use serde::Deserialize;

#[derive(Deserialize)]
pub struct SimCreateBotRequest {
    pub first_name: Option<String>,
    pub username: Option<String>,
}

#[derive(Deserialize)]
pub struct SimUpdateBotRequest {
    pub first_name: Option<String>,
    pub username: Option<String>,
}

#[derive(Deserialize)]
pub struct SimSetPrivacyModeRequest {
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct SimManagedBotRecord {
    pub owner_user_id: i64,
    pub managed_bot_id: i64,
    pub managed_token: String,
    pub managed_bot_username: String,
    pub managed_bot_first_name: String,
    pub created_at: i64,
    pub updated_at: i64,
}
