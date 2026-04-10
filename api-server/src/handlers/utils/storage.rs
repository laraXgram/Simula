use crate::types::ApiError;

pub fn ensure_sim_verifications_storage(
    conn: &mut rusqlite::Connection,
) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_user_verifications (
            bot_id               INTEGER NOT NULL,
            user_id              INTEGER NOT NULL,
            custom_description   TEXT,
            verified_at          INTEGER NOT NULL,
            updated_at           INTEGER NOT NULL,
            PRIMARY KEY (bot_id, user_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );
        CREATE TABLE IF NOT EXISTS sim_chat_verifications (
            bot_id               INTEGER NOT NULL,
            chat_key             TEXT NOT NULL,
            custom_description   TEXT,
            verified_at          INTEGER NOT NULL,
            updated_at           INTEGER NOT NULL,
            PRIMARY KEY (bot_id, chat_key),
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );",
    )
    .map_err(ApiError::internal)
}