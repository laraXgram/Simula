use crate::types::ApiError;

pub fn ensure_sim_prepared_inline_messages_storage(
    conn: &mut rusqlite::Connection,
) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_prepared_inline_messages (
            bot_id               INTEGER NOT NULL,
            id                   TEXT NOT NULL,
            user_id              INTEGER NOT NULL,
            result_json          TEXT NOT NULL,
            allow_user_chats     INTEGER NOT NULL DEFAULT 1,
            allow_bot_chats      INTEGER NOT NULL DEFAULT 1,
            allow_group_chats    INTEGER NOT NULL DEFAULT 1,
            allow_channel_chats  INTEGER NOT NULL DEFAULT 1,
            expiration_date      INTEGER NOT NULL,
            created_at           INTEGER NOT NULL,
            updated_at           INTEGER NOT NULL,
            PRIMARY KEY (bot_id, id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );",
    )
    .map_err(ApiError::internal)
}

pub fn ensure_sim_prepared_keyboard_buttons_storage(
    conn: &mut rusqlite::Connection,
) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_prepared_keyboard_buttons (
            bot_id       INTEGER NOT NULL,
            id           TEXT NOT NULL,
            user_id      INTEGER NOT NULL,
            button_json  TEXT NOT NULL,
            created_at   INTEGER NOT NULL,
            updated_at   INTEGER NOT NULL,
            PRIMARY KEY (bot_id, id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );",
    )
    .map_err(ApiError::internal)
}

pub fn ensure_sim_web_app_query_answers_storage(
    conn: &mut rusqlite::Connection,
) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_web_app_query_answers (
            bot_id            INTEGER NOT NULL,
            web_app_query_id  TEXT NOT NULL,
            inline_message_id TEXT,
            result_json       TEXT NOT NULL,
            answered_at       INTEGER NOT NULL,
            PRIMARY KEY (bot_id, web_app_query_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );",
    )
    .map_err(ApiError::internal)
}