use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard};

use crate::types::ApiError;
use crate::websocket::WebSocketHub;

pub struct AppState {
    pub db: Mutex<Connection>,
    pub ws_hub: WebSocketHub,
    pub runtime_logs: Mutex<VecDeque<RuntimeRequestLogEntry>>,
    pub api_enabled: Mutex<bool>,
}

pub const MAX_RUNTIME_LOG_ENTRIES: usize = 1000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeRequestLogEntry {
    pub id: String,
    pub at: i64,
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub status: u16,
    pub duration_ms: u64,
    pub remote_addr: Option<String>,
    pub request: Option<Value>,
    pub response: Option<Value>,
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

pub fn push_runtime_request_log(
    state: &actix_web::web::Data<AppState>,
    entry: RuntimeRequestLogEntry,
) {
    if let Ok(mut logs) = state.runtime_logs.lock() {
        logs.push_front(entry);
        while logs.len() > MAX_RUNTIME_LOG_ENTRIES {
            logs.pop_back();
        }
    }
}

pub fn runtime_logs_snapshot(
    state: &actix_web::web::Data<AppState>,
    limit: usize,
) -> Vec<RuntimeRequestLogEntry> {
    let capped_limit = limit.min(MAX_RUNTIME_LOG_ENTRIES).max(1);
    match state.runtime_logs.lock() {
        Ok(logs) => logs.iter().take(capped_limit).cloned().collect(),
        Err(_) => Vec::new(),
    }
}

pub fn clear_runtime_logs(state: &actix_web::web::Data<AppState>) {
    if let Ok(mut logs) = state.runtime_logs.lock() {
        logs.clear();
    }
}

pub fn is_api_enabled(state: &actix_web::web::Data<AppState>) -> bool {
    state.api_enabled.lock().map(|value| *value).unwrap_or(true)
}

pub fn set_api_enabled(state: &actix_web::web::Data<AppState>, enabled: bool) {
    if let Ok(mut value) = state.api_enabled.lock() {
        *value = enabled;
    }
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

        CREATE TABLE IF NOT EXISTS sim_bot_runtime_settings (
            bot_id                INTEGER PRIMARY KEY,
            privacy_mode_enabled  INTEGER NOT NULL DEFAULT 1,
            updated_at            INTEGER NOT NULL,
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );

        CREATE TABLE IF NOT EXISTS bot_chat_menu_buttons (
            bot_id           INTEGER NOT NULL,
            scope_key        TEXT NOT NULL,
            menu_button_json TEXT NOT NULL,
            updated_at       INTEGER NOT NULL,
            PRIMARY KEY (bot_id, scope_key),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );

        CREATE TABLE IF NOT EXISTS users (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            username    TEXT,
            first_name  TEXT NOT NULL,
            last_name   TEXT,
            phone_number TEXT,
            photo_url   TEXT,
            bio         TEXT,
            is_premium  INTEGER NOT NULL DEFAULT 0,
            business_name TEXT,
            business_intro TEXT,
            business_location TEXT,
            gift_count  INTEGER NOT NULL DEFAULT 0,
            created_at  INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS chats (
            chat_key    TEXT PRIMARY KEY,
            chat_type   TEXT NOT NULL,
            title       TEXT
        );

        CREATE TABLE IF NOT EXISTS sim_chats (
            bot_id                  INTEGER NOT NULL,
            chat_key                TEXT NOT NULL,
            chat_id                 INTEGER NOT NULL,
            chat_type               TEXT NOT NULL,
            title                   TEXT,
            username                TEXT,
            description             TEXT,
            photo_file_id           TEXT,
            is_forum                INTEGER NOT NULL DEFAULT 0,
            is_direct_messages      INTEGER NOT NULL DEFAULT 0,
            parent_channel_chat_id  INTEGER,
            direct_messages_enabled INTEGER NOT NULL DEFAULT 0,
            direct_messages_star_count INTEGER NOT NULL DEFAULT 0,
            channel_show_author_signature INTEGER NOT NULL DEFAULT 0,
            channel_paid_reactions_enabled INTEGER NOT NULL DEFAULT 0,
            linked_discussion_chat_id INTEGER,
            message_history_visible INTEGER NOT NULL DEFAULT 1,
            slow_mode_delay         INTEGER NOT NULL DEFAULT 0,
            permissions_json        TEXT,
            sticker_set_name        TEXT,
            pinned_message_id       INTEGER,
            owner_user_id           INTEGER,
            created_at              INTEGER NOT NULL,
            updated_at              INTEGER NOT NULL,
            PRIMARY KEY (bot_id, chat_key),
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE TABLE IF NOT EXISTS sim_chat_pinned_messages (
            bot_id      INTEGER NOT NULL,
            chat_key    TEXT NOT NULL,
            message_id  INTEGER NOT NULL,
            pinned_at   INTEGER NOT NULL,
            PRIMARY KEY (bot_id, chat_key, message_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE INDEX IF NOT EXISTS idx_sim_chat_pins_recent
            ON sim_chat_pinned_messages (bot_id, chat_key, pinned_at DESC, message_id DESC);

        CREATE TABLE IF NOT EXISTS sim_chat_members (
            bot_id      INTEGER NOT NULL,
            chat_key    TEXT NOT NULL,
            user_id     INTEGER NOT NULL,
            status      TEXT NOT NULL,
            role        TEXT NOT NULL,
            permissions_json TEXT,
            admin_rights_json TEXT,
            until_date  INTEGER,
            custom_title TEXT,
            tag         TEXT,
            joined_at   INTEGER,
            updated_at  INTEGER NOT NULL,
            PRIMARY KEY (bot_id, chat_key, user_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE TABLE IF NOT EXISTS sim_banned_sender_chats (
            bot_id          INTEGER NOT NULL,
            chat_key        TEXT NOT NULL,
            sender_chat_id  INTEGER NOT NULL,
            created_at      INTEGER NOT NULL,
            updated_at      INTEGER NOT NULL,
            PRIMARY KEY (bot_id, chat_key, sender_chat_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE TABLE IF NOT EXISTS sim_chat_invite_links (
            bot_id               INTEGER NOT NULL,
            chat_key             TEXT NOT NULL,
            invite_link          TEXT NOT NULL,
            creator_user_id      INTEGER NOT NULL,
            creates_join_request INTEGER NOT NULL DEFAULT 0,
            is_primary           INTEGER NOT NULL DEFAULT 0,
            is_revoked           INTEGER NOT NULL DEFAULT 0,
            name                 TEXT,
            expire_date          INTEGER,
            member_limit         INTEGER,
            subscription_period  INTEGER,
            subscription_price   INTEGER,
            created_at           INTEGER NOT NULL,
            updated_at           INTEGER NOT NULL,
            PRIMARY KEY (bot_id, invite_link),
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE TABLE IF NOT EXISTS sim_chat_join_requests (
            bot_id       INTEGER NOT NULL,
            chat_key     TEXT NOT NULL,
            user_id      INTEGER NOT NULL,
            invite_link  TEXT,
            status       TEXT NOT NULL,
            created_at   INTEGER NOT NULL,
            updated_at   INTEGER NOT NULL,
            PRIMARY KEY (bot_id, chat_key, user_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE TABLE IF NOT EXISTS sim_channel_post_stats (
            bot_id      INTEGER NOT NULL,
            chat_key    TEXT NOT NULL,
            message_id  INTEGER NOT NULL,
            views       INTEGER NOT NULL DEFAULT 0,
            updated_at  INTEGER NOT NULL,
            PRIMARY KEY (bot_id, chat_key, message_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE TABLE IF NOT EXISTS sim_channel_post_viewers (
            bot_id         INTEGER NOT NULL,
            chat_key       TEXT NOT NULL,
            message_id     INTEGER NOT NULL,
            viewer_user_id INTEGER NOT NULL,
            viewed_at      INTEGER NOT NULL,
            PRIMARY KEY (bot_id, chat_key, message_id, viewer_user_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE INDEX IF NOT EXISTS idx_sim_channel_post_viewers_window
            ON sim_channel_post_viewers (bot_id, chat_key, message_id, viewed_at DESC);

        CREATE TABLE IF NOT EXISTS sim_linked_discussion_messages (
            bot_id                      INTEGER NOT NULL,
            discussion_chat_key         TEXT NOT NULL,
            discussion_message_id       INTEGER NOT NULL,
            discussion_root_message_id  INTEGER NOT NULL,
            channel_chat_key            TEXT NOT NULL,
            channel_message_id          INTEGER NOT NULL,
            created_at                  INTEGER NOT NULL,
            updated_at                  INTEGER NOT NULL,
            PRIMARY KEY (bot_id, discussion_chat_key, discussion_message_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(discussion_chat_key) REFERENCES chats(chat_key),
            FOREIGN KEY(channel_chat_key) REFERENCES chats(chat_key)
        );

        CREATE INDEX IF NOT EXISTS idx_sim_linked_discussion_channel
            ON sim_linked_discussion_messages (
                bot_id,
                channel_chat_key,
                channel_message_id,
                discussion_root_message_id,
                updated_at DESC
            );

        CREATE TABLE IF NOT EXISTS sim_business_connections (
            bot_id              INTEGER NOT NULL,
            connection_id       TEXT NOT NULL,
            user_id             INTEGER NOT NULL,
            user_chat_id        INTEGER NOT NULL,
            rights_json         TEXT NOT NULL,
            is_enabled          INTEGER NOT NULL DEFAULT 1,
            gift_settings_show_button INTEGER NOT NULL DEFAULT 1,
            gift_settings_types_json  TEXT,
            star_balance        INTEGER NOT NULL DEFAULT 0,
            created_at          INTEGER NOT NULL,
            updated_at          INTEGER NOT NULL,
            PRIMARY KEY (bot_id, connection_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_sim_business_connections_user
            ON sim_business_connections (bot_id, user_id);

        CREATE TABLE IF NOT EXISTS sim_business_account_profiles (
            bot_id                          INTEGER NOT NULL,
            user_id                         INTEGER NOT NULL,
            last_name                       TEXT,
            bio                             TEXT,
            profile_photo_file_id           TEXT,
            public_profile_photo_file_id    TEXT,
            updated_at                      INTEGER NOT NULL,
            PRIMARY KEY (bot_id, user_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );

        CREATE TABLE IF NOT EXISTS sim_business_read_messages (
            bot_id              INTEGER NOT NULL,
            connection_id       TEXT NOT NULL,
            chat_id             INTEGER NOT NULL,
            message_id          INTEGER NOT NULL,
            read_at             INTEGER NOT NULL,
            PRIMARY KEY (bot_id, connection_id, chat_id, message_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );

        CREATE TABLE IF NOT EXISTS sim_direct_message_topics (
            bot_id              INTEGER NOT NULL,
            chat_key            TEXT NOT NULL,
            topic_id            INTEGER NOT NULL,
            user_id             INTEGER NOT NULL,
            created_at          INTEGER NOT NULL,
            updated_at          INTEGER NOT NULL,
            last_message_id     INTEGER,
            last_message_date   INTEGER,
            PRIMARY KEY (bot_id, chat_key, topic_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_sim_direct_message_topics_user
            ON sim_direct_message_topics (bot_id, chat_key, user_id);

        CREATE TABLE IF NOT EXISTS sim_message_drafts (
            bot_id              INTEGER NOT NULL,
            chat_key            TEXT NOT NULL,
            message_thread_id   INTEGER NOT NULL,
            draft_id            INTEGER NOT NULL,
            message_id          INTEGER NOT NULL,
            updated_at          INTEGER NOT NULL,
            PRIMARY KEY (bot_id, chat_key, message_thread_id, draft_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key),
            FOREIGN KEY(message_id) REFERENCES messages(message_id)
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

        CREATE TABLE IF NOT EXISTS inline_query_cache (
            bot_id        INTEGER NOT NULL,
            query         TEXT NOT NULL,
            offset        TEXT NOT NULL DEFAULT '',
            from_user_id  INTEGER NOT NULL DEFAULT -1,
            answer_json   TEXT NOT NULL,
            cache_time    INTEGER NOT NULL,
            expires_at    INTEGER NOT NULL,
            is_personal   INTEGER NOT NULL DEFAULT 0,
            created_at    INTEGER NOT NULL,
            PRIMARY KEY (bot_id, query, offset, from_user_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
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

        CREATE TABLE IF NOT EXISTS sim_owned_gifts (
            bot_id                    INTEGER NOT NULL,
            owned_gift_id             TEXT NOT NULL,
            owner_user_id             INTEGER,
            owner_chat_id             INTEGER,
            sender_user_id            INTEGER,
            gift_id                   TEXT NOT NULL,
            gift_json                 TEXT NOT NULL,
            gift_star_count           INTEGER NOT NULL,
            is_unique                 INTEGER NOT NULL DEFAULT 0,
            is_unlimited              INTEGER NOT NULL DEFAULT 0,
            is_from_blockchain        INTEGER NOT NULL DEFAULT 0,
            send_date                 INTEGER NOT NULL,
            text                      TEXT,
            entities_json             TEXT,
            is_private                INTEGER NOT NULL DEFAULT 0,
            is_saved                  INTEGER NOT NULL DEFAULT 0,
            can_be_upgraded           INTEGER NOT NULL DEFAULT 1,
            was_refunded              INTEGER NOT NULL DEFAULT 0,
            convert_star_count        INTEGER,
            prepaid_upgrade_star_count INTEGER,
            is_upgrade_separate       INTEGER NOT NULL DEFAULT 0,
            unique_gift_number        INTEGER,
            transfer_star_count       INTEGER,
            next_transfer_date        INTEGER,
            created_at                INTEGER NOT NULL,
            updated_at                INTEGER NOT NULL,
            PRIMARY KEY (bot_id, owned_gift_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );

        CREATE INDEX IF NOT EXISTS idx_sim_owned_gifts_owner_user
            ON sim_owned_gifts (bot_id, owner_user_id, send_date DESC, owned_gift_id DESC);

        CREATE INDEX IF NOT EXISTS idx_sim_owned_gifts_owner_chat
            ON sim_owned_gifts (bot_id, owner_chat_id, send_date DESC, owned_gift_id DESC);

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
            allows_revoting          INTEGER NOT NULL DEFAULT 0,
            correct_option_id        INTEGER,
            correct_option_ids_json  TEXT,
            explanation              TEXT,
            description              TEXT,
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
            description_entities_json TEXT,
            FOREIGN KEY(poll_id) REFERENCES polls(id)
        );

        CREATE TABLE IF NOT EXISTS forum_topics (
            bot_id                INTEGER NOT NULL,
            chat_key              TEXT NOT NULL,
            message_thread_id     INTEGER NOT NULL,
            name                  TEXT NOT NULL,
            icon_color            INTEGER NOT NULL,
            icon_custom_emoji_id  TEXT,
            is_closed             INTEGER NOT NULL DEFAULT 0,
            created_at            INTEGER NOT NULL,
            updated_at            INTEGER NOT NULL,
            PRIMARY KEY (bot_id, chat_key, message_thread_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE TABLE IF NOT EXISTS forum_topic_general_states (
            bot_id      INTEGER NOT NULL,
            chat_key    TEXT NOT NULL,
            name        TEXT NOT NULL DEFAULT 'General',
            is_closed   INTEGER NOT NULL DEFAULT 0,
            is_hidden   INTEGER NOT NULL DEFAULT 0,
            updated_at  INTEGER NOT NULL,
            PRIMARY KEY (bot_id, chat_key),
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE TABLE IF NOT EXISTS sticker_sets (
            bot_id              INTEGER NOT NULL,
            name                TEXT NOT NULL,
            title               TEXT NOT NULL,
            sticker_type        TEXT NOT NULL DEFAULT 'regular',
            needs_repainting    INTEGER NOT NULL DEFAULT 0,
            owner_user_id       INTEGER NOT NULL,
            thumbnail_file_id   TEXT,
            thumbnail_format    TEXT,
            custom_emoji_id     TEXT,
            created_at          INTEGER NOT NULL,
            updated_at          INTEGER NOT NULL,
            PRIMARY KEY (bot_id, name),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );

        CREATE TABLE IF NOT EXISTS stickers (
            bot_id               INTEGER NOT NULL,
            file_id              TEXT NOT NULL,
            file_unique_id       TEXT NOT NULL,
            set_name             TEXT,
            sticker_type         TEXT NOT NULL DEFAULT 'regular',
            format               TEXT NOT NULL DEFAULT 'static',
            width                INTEGER NOT NULL DEFAULT 512,
            height               INTEGER NOT NULL DEFAULT 512,
            is_animated          INTEGER NOT NULL DEFAULT 0,
            is_video             INTEGER NOT NULL DEFAULT 0,
            emoji                TEXT,
            emoji_list_json      TEXT,
            keywords_json        TEXT,
            mask_position_json   TEXT,
            custom_emoji_id      TEXT,
            needs_repainting     INTEGER NOT NULL DEFAULT 0,
            position             INTEGER NOT NULL DEFAULT 0,
            created_at           INTEGER NOT NULL,
            updated_at           INTEGER NOT NULL,
            PRIMARY KEY (bot_id, file_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );

        CREATE INDEX IF NOT EXISTS idx_stickers_by_set ON stickers (bot_id, set_name, position, created_at);

        CREATE TABLE IF NOT EXISTS games (
            bot_id            INTEGER NOT NULL,
            chat_key          TEXT NOT NULL,
            message_id        INTEGER NOT NULL,
            game_short_name   TEXT NOT NULL,
            title             TEXT NOT NULL,
            description       TEXT NOT NULL,
            created_at        INTEGER NOT NULL,
            PRIMARY KEY (bot_id, chat_key, message_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE TABLE IF NOT EXISTS game_scores (
            bot_id            INTEGER NOT NULL,
            chat_key          TEXT NOT NULL,
            message_id        INTEGER NOT NULL,
            user_id           INTEGER NOT NULL,
            score             INTEGER NOT NULL,
            updated_at        INTEGER NOT NULL,
            PRIMARY KEY (bot_id, chat_key, message_id, user_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(chat_key) REFERENCES chats(chat_key)
        );

        CREATE INDEX IF NOT EXISTS idx_game_scores_ordered
            ON game_scores (bot_id, chat_key, message_id, score DESC, updated_at ASC);
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
    ensure_column_exists(conn, "polls", "allows_revoting", "INTEGER NOT NULL DEFAULT 0")?;
    ensure_column_exists(conn, "polls", "correct_option_ids_json", "TEXT")?;
    ensure_column_exists(conn, "polls", "description", "TEXT")?;
    ensure_column_exists(conn, "poll_metadata", "description_entities_json", "TEXT")?;
    ensure_column_exists(conn, "users", "last_name", "TEXT")?;
    ensure_column_exists(conn, "users", "phone_number", "TEXT")?;
    ensure_column_exists(conn, "users", "photo_url", "TEXT")?;
    ensure_column_exists(conn, "users", "bio", "TEXT")?;
    ensure_column_exists(conn, "users", "is_premium", "INTEGER NOT NULL DEFAULT 0")?;
    ensure_column_exists(conn, "users", "business_name", "TEXT")?;
    ensure_column_exists(conn, "users", "business_intro", "TEXT")?;
    ensure_column_exists(conn, "users", "business_location", "TEXT")?;
    ensure_column_exists(conn, "users", "gift_count", "INTEGER NOT NULL DEFAULT 0")?;
    ensure_column_exists(conn, "sim_chats", "sticker_set_name", "TEXT")?;
    ensure_column_exists(conn, "sim_chats", "pinned_message_id", "INTEGER")?;
    ensure_column_exists(conn, "sim_chats", "is_direct_messages", "INTEGER NOT NULL DEFAULT 0")?;
    ensure_column_exists(conn, "sim_chats", "parent_channel_chat_id", "INTEGER")?;
    ensure_column_exists(conn, "sim_chats", "direct_messages_enabled", "INTEGER NOT NULL DEFAULT 0")?;
    ensure_column_exists(conn, "sim_chats", "direct_messages_star_count", "INTEGER NOT NULL DEFAULT 0")?;
    ensure_column_exists(conn, "sim_chats", "channel_show_author_signature", "INTEGER NOT NULL DEFAULT 0")?;
    ensure_column_exists(conn, "sim_chats", "channel_paid_reactions_enabled", "INTEGER NOT NULL DEFAULT 0")?;
    ensure_column_exists(conn, "sim_chats", "linked_discussion_chat_id", "INTEGER")?;
    ensure_column_exists(conn, "sim_chat_members", "permissions_json", "TEXT")?;
    ensure_column_exists(conn, "sim_chat_members", "admin_rights_json", "TEXT")?;
    ensure_column_exists(conn, "sim_chat_members", "until_date", "INTEGER")?;
    ensure_column_exists(conn, "sim_chat_members", "custom_title", "TEXT")?;
    ensure_column_exists(conn, "sim_chat_members", "tag", "TEXT")?;
    ensure_column_exists(conn, "sim_chat_invite_links", "is_primary", "INTEGER NOT NULL DEFAULT 0")?;
    ensure_column_exists(conn, "sim_chat_invite_links", "subscription_period", "INTEGER")?;
    ensure_column_exists(conn, "sim_chat_invite_links", "subscription_price", "INTEGER")?;
    ensure_column_exists(conn, "sim_business_connections", "gift_settings_show_button", "INTEGER NOT NULL DEFAULT 1")?;
    ensure_column_exists(conn, "sim_business_connections", "gift_settings_types_json", "TEXT")?;
    ensure_column_exists(conn, "sim_business_connections", "star_balance", "INTEGER NOT NULL DEFAULT 0")?;
    ensure_column_exists(conn, "updates", "bot_visible", "INTEGER NOT NULL DEFAULT 1")?;

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
