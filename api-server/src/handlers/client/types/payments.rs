use serde::Deserialize;

#[derive(Deserialize)]
pub struct SimPayInvoiceRequest {
    pub chat_id: i64,
    pub message_id: i64,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub payment_method: Option<String>,
    pub outcome: Option<String>,
    pub tip_amount: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimPurchasePaidMediaRequest {
    pub chat_id: i64,
    pub message_id: i64,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub paid_media_payload: Option<String>,
}
