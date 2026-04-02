use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use std::sync::{Mutex, MutexGuard};

use crate::types::ApiError;
use crate::websocket::WebSocketHub;

pub struct AppState {
    pub db: Mutex<Connection>,
    pub ws_hub: WebSocketHub,
}

#[derive(Debug, Clone)]
pub struct BotInfoRecord {
    pub id: i64,
    pub first_name: String,
    pub username: String,
}

pub fn lock_db(state: &actix_web::web::Data<AppState>) -> Result<MutexGuard<'_, Connection>, ApiError> {
    state
        .db
        .lock()
        .map_err(|_| ApiError::internal("database lock poisoned"))
}

pub fn ensure_bot(conn: &mut Connection, token: &str) -> Result<BotInfoRecord, ApiError> {
    let existing: Option<BotInfoRecord> = conn
        .query_row(
            "SELECT id, username, first_name FROM bots WHERE token = ?1",
            params![token],
            |row| {
                Ok(BotInfoRecord {
                    id: row.get(0)?,
                    username: row.get(1)?,
                    first_name: row.get(2)?,
                })
            },
        )
        .optional()
        .map_err(ApiError::internal)?;

    if let Some(bot) = existing {
        return Ok(bot);
    }

    let generated_username = format!("laragram_bot_{}", short_token_suffix(token));
    let first_name = "LaraGram Bot".to_string();
    let now = Utc::now().timestamp();

    conn.execute(
        "INSERT INTO bots (token, username, first_name, created_at) VALUES (?1, ?2, ?3, ?4)",
        params![token, generated_username, first_name, now],
    )
    .map_err(ApiError::internal)?;

    Ok(BotInfoRecord {
        id: conn.last_insert_rowid(),
        first_name,
        username: generated_username,
    })
}

pub fn ensure_chat(conn: &mut Connection, chat_key: &str) -> Result<(), ApiError> {
    conn.execute(
        "INSERT INTO chats (chat_key, chat_type, title)
         VALUES (?1, 'private', NULL)
         ON CONFLICT(chat_key) DO NOTHING",
        params![chat_key],
    )
    .map_err(ApiError::internal)?;
    Ok(())
}

pub fn init_database(conn: &mut Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        r#"
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS bots (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            token       TEXT NOT NULL UNIQUE,
            username    TEXT NOT NULL,
            first_name  TEXT NOT NULL,
            created_at  INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS users (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            username    TEXT,
            first_name  TEXT NOT NULL,
            created_at  INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS chats (
            chat_key    TEXT PRIMARY KEY,
            chat_type   TEXT NOT NULL,
            title       TEXT
        );

        CREATE TABLE IF NOT EXISTS messages (
            message_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id        INTEGER NOT NULL,
            chat_key      TEXT NOT NULL,
            from_user_id  INTEGER,
            text          TEXT NOT NULL,
            date          INTEGER NOT NULL,
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE TABLE IF NOT EXISTS updates (
            update_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id       INTEGER NOT NULL,
            update_json  TEXT NOT NULL,
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );

        CREATE TABLE IF NOT EXISTS webhooks (
            bot_id           INTEGER PRIMARY KEY,
            url              TEXT NOT NULL,
            secret_token     TEXT DEFAULT '',
            max_connections  INTEGER DEFAULT 40,
            ip_address       TEXT DEFAULT '',
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );

        CREATE TABLE IF NOT EXISTS files (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id        INTEGER NOT NULL,
            file_id       TEXT NOT NULL,
            file_unique_id TEXT NOT NULL,
            file_path     TEXT NOT NULL,
            local_path    TEXT,
            mime_type     TEXT,
            file_size     INTEGER,
            source        TEXT,
            created_at    INTEGER NOT NULL,
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            UNIQUE(bot_id, file_id),
            UNIQUE(bot_id, file_path)
        );

        CREATE TABLE IF NOT EXISTS message_reactions (
            bot_id         INTEGER NOT NULL,
            chat_key       TEXT NOT NULL,
            message_id     INTEGER NOT NULL,
            actor_user_id  INTEGER NOT NULL,
            actor_is_bot   INTEGER NOT NULL DEFAULT 0,
            reactions_json TEXT NOT NULL,
            updated_at     INTEGER NOT NULL,
            PRIMARY KEY (bot_id, chat_key, message_id, actor_user_id, actor_is_bot),
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE TABLE IF NOT EXISTS chat_reply_keyboards (
            bot_id       INTEGER NOT NULL,
            chat_key     TEXT NOT NULL,
            markup_json  TEXT NOT NULL,
            updated_at   INTEGER NOT NULL,
            PRIMARY KEY (bot_id, chat_key),
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE TABLE IF NOT EXISTS callback_queries (
            id            TEXT PRIMARY KEY,
            bot_id        INTEGER NOT NULL,
            chat_key      TEXT NOT NULL,
            message_id    INTEGER,
            from_user_id  INTEGER NOT NULL,
            data          TEXT,
            created_at    INTEGER NOT NULL,
            answered_at   INTEGER,
            answer_json   TEXT,
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE TABLE IF NOT EXISTS inline_queries (
            id            TEXT PRIMARY KEY,
            bot_id        INTEGER NOT NULL,
            chat_key      TEXT NOT NULL,
            from_user_id  INTEGER NOT NULL,
            query         TEXT NOT NULL,
            offset        TEXT NOT NULL DEFAULT '',
            created_at    INTEGER NOT NULL,
            answered_at   INTEGER,
            answer_json   TEXT,
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE TABLE IF NOT EXISTS shipping_queries (
            id            TEXT PRIMARY KEY,
            bot_id        INTEGER NOT NULL,
            chat_key      TEXT NOT NULL,
            from_user_id  INTEGER NOT NULL,
            payload       TEXT NOT NULL,
            created_at    INTEGER NOT NULL,
            answered_at   INTEGER,
            answer_json   TEXT,
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE TABLE IF NOT EXISTS pre_checkout_queries (
            id            TEXT PRIMARY KEY,
            bot_id        INTEGER NOT NULL,
            chat_key      TEXT NOT NULL,
            from_user_id  INTEGER NOT NULL,
            payload       TEXT NOT NULL,
            currency      TEXT NOT NULL,
            total_amount  INTEGER NOT NULL,
            created_at    INTEGER NOT NULL,
            answered_at   INTEGER,
            answer_json   TEXT,
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE TABLE IF NOT EXISTS invoices (
            bot_id                 INTEGER NOT NULL,
            chat_key               TEXT NOT NULL,
            message_id             INTEGER NOT NULL,
            title                  TEXT NOT NULL,
            description            TEXT NOT NULL,
            payload                TEXT NOT NULL,
            currency               TEXT NOT NULL,
            total_amount           INTEGER NOT NULL,
            need_shipping_address  INTEGER NOT NULL DEFAULT 0,
            is_flexible            INTEGER NOT NULL DEFAULT 0,
            created_at             INTEGER NOT NULL,
            paid_at                INTEGER,
            PRIMARY KEY (bot_id, chat_key, message_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE TABLE IF NOT EXISTS star_transactions_ledger (
            id                         TEXT PRIMARY KEY,
            bot_id                     INTEGER NOT NULL,
            user_id                    INTEGER NOT NULL,
            telegram_payment_charge_id TEXT NOT NULL,
            amount                     INTEGER NOT NULL,
            date                       INTEGER NOT NULL,
            kind                       TEXT NOT NULL,
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_star_ledger_bot_charge_kind
            ON star_transactions_ledger (bot_id, telegram_payment_charge_id, kind);

        CREATE TABLE IF NOT EXISTS star_subscriptions (
            bot_id                     INTEGER NOT NULL,
            user_id                    INTEGER NOT NULL,
            telegram_payment_charge_id TEXT NOT NULL,
            is_canceled                INTEGER NOT NULL,
            updated_at                 INTEGER NOT NULL,
            PRIMARY KEY (bot_id, user_id, telegram_payment_charge_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );

        CREATE TABLE IF NOT EXISTS inline_messages (
            inline_message_id TEXT PRIMARY KEY,
            bot_id            INTEGER NOT NULL,
            chat_key          TEXT NOT NULL,
            message_id        INTEGER NOT NULL,
            created_at        INTEGER NOT NULL,
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE TABLE IF NOT EXISTS polls (
            id                       TEXT PRIMARY KEY,
            bot_id                   INTEGER NOT NULL,
            chat_key                 TEXT NOT NULL,
            message_id               INTEGER NOT NULL,
            question                 TEXT NOT NULL,
            options_json             TEXT NOT NULL,
            total_voter_count        INTEGER NOT NULL DEFAULT 0,
            is_closed                INTEGER NOT NULL DEFAULT 0,
            is_anonymous             INTEGER NOT NULL DEFAULT 1,
            poll_type                TEXT NOT NULL DEFAULT 'regular',
            allows_multiple_answers  INTEGER NOT NULL DEFAULT 0,
            correct_option_id        INTEGER,
            explanation              TEXT,
            open_period              INTEGER,
            close_date               INTEGER,
            created_at               INTEGER NOT NULL,
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE TABLE IF NOT EXISTS poll_votes (
            poll_id          TEXT NOT NULL,
            voter_user_id    INTEGER NOT NULL,
            option_ids_json  TEXT NOT NULL,
            updated_at       INTEGER NOT NULL,
            PRIMARY KEY (poll_id, voter_user_id),
            FOREIGN KEY(poll_id) REFERENCES polls(id)
        );

        CREATE TABLE IF NOT EXISTS poll_metadata (
            poll_id                   TEXT PRIMARY KEY,
            question_entities_json    TEXT,
            explanation_entities_json TEXT,
            FOREIGN KEY(poll_id) REFERENCES polls(id)
        );
        "#,
    )?;

    ensure_column_exists(conn, "invoices", "max_tip_amount", "INTEGER NOT NULL DEFAULT 0")?;
    ensure_column_exists(conn, "invoices", "suggested_tip_amounts_json", "TEXT")?;
    ensure_column_exists(conn, "invoices", "start_parameter", "TEXT")?;
    ensure_column_exists(conn, "invoices", "provider_data", "TEXT")?;
    ensure_column_exists(conn, "invoices", "photo_url", "TEXT")?;
    ensure_column_exists(conn, "invoices", "photo_size", "INTEGER")?;
    ensure_column_exists(conn, "invoices", "photo_width", "INTEGER")?;
    ensure_column_exists(conn, "invoices", "photo_height", "INTEGER")?;
    ensure_column_exists(conn, "invoices", "need_name", "INTEGER NOT NULL DEFAULT 0")?;
    ensure_column_exists(conn, "invoices", "need_phone_number", "INTEGER NOT NULL DEFAULT 0")?;
    ensure_column_exists(conn, "invoices", "need_email", "INTEGER NOT NULL DEFAULT 0")?;
    ensure_column_exists(conn, "invoices", "send_phone_number_to_provider", "INTEGER NOT NULL DEFAULT 0")?;
    ensure_column_exists(conn, "invoices", "send_email_to_provider", "INTEGER NOT NULL DEFAULT 0")?;

    Ok(())
}

fn ensure_column_exists(
    conn: &mut Connection,
    table_name: &str,
    column_name: &str,
    column_sql: &str,
) -> Result<(), rusqlite::Error> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", table_name))?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let existing_name: String = row.get(1)?;
        if existing_name == column_name {
            return Ok(());
        }
    }

    conn.execute(
        &format!("ALTER TABLE {} ADD COLUMN {} {}", table_name, column_name, column_sql),
        [],
    )?;
    Ok(())
}

fn short_token_suffix(token: &str) -> String {
    let cleaned = token.replace(':', "").replace('-', "");
    cleaned
        .chars()
        .rev()
        .take(8)
        .collect::<String>()
        .chars()
        .rev()
        .collect()
}
