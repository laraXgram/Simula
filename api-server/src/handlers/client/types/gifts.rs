use serde::Deserialize;
use serde_json::Value;

use crate::generated::types::Gift;

#[derive(Debug, Clone)]
pub struct SimGiftCatalogEntry {
    pub gift: Gift,
    pub is_unlimited: bool,
    pub is_from_blockchain: bool,
}

#[derive(Debug, Clone)]
pub struct SimOwnedGiftRecord {
    pub owned_gift_id: String,
    pub sender_user_id: Option<i64>,
    pub gift_id: String,
    pub gift_json: String,
    pub gift_star_count: i64,
    pub is_unique: bool,
    pub is_unlimited: bool,
    pub is_from_blockchain: bool,
    pub send_date: i64,
    pub text: Option<String>,
    pub entities_json: Option<String>,
    pub is_private: bool,
    pub is_saved: bool,
    pub can_be_upgraded: bool,
    pub was_refunded: bool,
    pub convert_star_count: Option<i64>,
    pub prepaid_upgrade_star_count: Option<i64>,
    pub is_upgrade_separate: bool,
    pub unique_gift_number: Option<i64>,
    pub transfer_star_count: Option<i64>,
    pub next_transfer_date: Option<i64>,
}

#[derive(Debug, Clone, Default)]
pub struct SimOwnedGiftFilterOptions {
    pub exclude_unsaved: bool,
    pub exclude_saved: bool,
    pub exclude_unlimited: bool,
    pub exclude_limited_upgradable: bool,
    pub exclude_limited_non_upgradable: bool,
    pub exclude_from_blockchain: bool,
    pub exclude_unique: bool,
    pub sort_by_price: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimDeleteOwnedGiftRequest {
    pub owned_gift_id: String,
    pub user_id: Option<i64>,
    pub chat_id: Option<Value>,
}