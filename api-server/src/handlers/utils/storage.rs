use chrono::Utc;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use rusqlite::params;
use crate::types::ApiError;
use crate::handlers::{
    generate_telegram_file_id, generate_telegram_file_unique_id,
    client::types::messages::StoredFile
};

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

pub fn download_remote_file(url: &str) -> Result<(Vec<u8>, Option<String>), ApiError> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .map_err(ApiError::internal)?;

    let response = client
        .get(url)
        .send()
        .map_err(|_| ApiError::bad_request("failed to fetch remote file"))?;

    if !response.status().is_success() {
        return Err(ApiError::bad_request("remote file url returned non-200 status"));
    }

    let mime = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let bytes = response.bytes().map_err(ApiError::internal)?;
    if bytes.is_empty() {
        return Err(ApiError::bad_request("remote file is empty"));
    }

    Ok((bytes.to_vec(), mime))
}

pub fn store_binary_file(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    bytes: &[u8],
    mime_type: Option<&str>,
    source: Option<String>,
) -> Result<StoredFile, ApiError> {
    let now = Utc::now().timestamp();
    let file_id = generate_telegram_file_id("file");
    let file_unique_id = generate_telegram_file_unique_id();
    let file_path = format!("media/{}/{}", bot_id, file_id);

    let base_dir = media_storage_root();
    fs::create_dir_all(&base_dir).map_err(ApiError::internal)?;
    let local_path = base_dir.join(&file_id);
    fs::write(&local_path, bytes).map_err(ApiError::internal)?;

    conn.execute(
        "INSERT INTO files (bot_id, file_id, file_unique_id, file_path, local_path, mime_type, file_size, source, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            bot_id,
            file_id,
            file_unique_id,
            file_path,
            local_path.to_string_lossy().to_string(),
            mime_type,
            bytes.len() as i64,
            source,
            now
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(StoredFile {
        file_id,
        file_unique_id,
        file_path,
        mime_type: mime_type.map(|m| m.to_string()),
        file_size: Some(bytes.len() as i64),
    })
}

pub fn media_storage_root() -> PathBuf {
    std::env::var("FILE_STORAGE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| Path::new("files").to_path_buf())
}