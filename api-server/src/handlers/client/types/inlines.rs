use serde::Deserialize;

#[derive(Deserialize)]
pub struct SimPressInlineButtonRequest {
    pub chat_id: i64,
    pub message_id: i64,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub callback_data: String,
}

#[derive(Deserialize)]
pub struct SimSendInlineQueryRequest {
    pub chat_id: Option<i64>,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub query: String,
    pub offset: Option<String>,
}

#[derive(Deserialize)]
pub struct SimChooseInlineResultRequest {
    pub inline_query_id: String,
    pub result_id: String,
}
