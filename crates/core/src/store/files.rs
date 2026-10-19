//! File contents, upload parts, and the photos and documents made from them.

use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};

use crate::ids::UserId;

/// Store `data` (once per distinct content); returns its blob id.
pub fn put_blob(conn: &Connection, data: &[u8]) -> rusqlite::Result<i64> {
    let hash = Sha256::digest(data).to_vec();
    if let Some(id) = conn
        .query_row("SELECT id FROM blobs WHERE sha256 = ?1", [&hash], |row| {
            row.get(0)
        })
        .optional()?
    {
        return Ok(id);
    }
    conn.execute(
        "INSERT INTO blobs (sha256, size, data) VALUES (?1, ?2, ?3)",
        params![hash, data.len() as i64, data],
    )?;
    Ok(conn.last_insert_rowid())
}

pub fn blob_size(conn: &Connection, id: i64) -> rusqlite::Result<Option<i64>> {
    conn.query_row("SELECT size FROM blobs WHERE id = ?1", [id], |row| {
        row.get(0)
    })
    .optional()
}

/// `limit` bytes of a blob from `offset` (fewer at its end).
pub fn blob_range(
    conn: &Connection,
    id: i64,
    offset: i64,
    limit: i64,
) -> rusqlite::Result<Vec<u8>> {
    Ok(conn
        .query_row(
            "SELECT substr(data, ?2, ?3) FROM blobs WHERE id = ?1",
            params![id, offset + 1, limit],
            |row| row.get::<_, Option<Vec<u8>>>(0),
        )
        .optional()?
        .flatten()
        .unwrap_or_default())
}

pub fn blob(conn: &Connection, id: i64) -> rusqlite::Result<Option<Vec<u8>>> {
    conn.query_row("SELECT data FROM blobs WHERE id = ?1", [id], |row| {
        row.get(0)
    })
    .optional()
}

pub fn save_part(
    conn: &Connection,
    owner: UserId,
    file_id: i64,
    part: i32,
    total: Option<i32>,
    data: &[u8],
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO upload_parts (owner_id, file_id, part, total, data)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![owner.0, file_id, part, total, data],
    )?;
    Ok(())
}

/// The parts of an upload, by part number.
pub fn parts(
    conn: &Connection,
    owner: UserId,
    file_id: i64,
) -> rusqlite::Result<Vec<(i32, Vec<u8>)>> {
    let mut statement = conn.prepare(
        "SELECT part, data FROM upload_parts WHERE owner_id = ?1 AND file_id = ?2 ORDER BY part",
    )?;
    let rows = statement.query_map(params![owner.0, file_id], |row| {
        Ok((row.get(0)?, row.get(1)?))
    })?;
    rows.collect()
}

pub fn clear_parts(conn: &Connection, owner: UserId, file_id: i64) -> rusqlite::Result<()> {
    conn.execute(
        "DELETE FROM upload_parts WHERE owner_id = ?1 AND file_id = ?2",
        params![owner.0, file_id],
    )?;
    Ok(())
}

/// A stored photo or document.
#[derive(Debug, Clone)]
pub struct Media {
    pub id: i64,
    pub photo: bool,
    pub access_hash: i64,
    pub owner: UserId,
    pub date: i32,
    /// JSON-TL `Photo` or `Document`.
    pub body: serde_json::Value,
}

pub fn put_media(conn: &Connection, media: &Media) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO media (id, kind, access_hash, owner_id, date, body) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            media.id,
            if media.photo { "photo" } else { "document" },
            media.access_hash,
            media.owner.0,
            media.date,
            media.body.to_string()
        ],
    )?;
    Ok(())
}

pub fn media(conn: &Connection, id: i64) -> rusqlite::Result<Option<Media>> {
    conn.query_row(
        "SELECT id, kind, access_hash, owner_id, date, body FROM media WHERE id = ?1",
        [id],
        |row| {
            Ok(Media {
                id: row.get(0)?,
                photo: row.get::<_, String>(1)? == "photo",
                access_hash: row.get(2)?,
                owner: UserId(row.get(3)?),
                date: row.get(4)?,
                body: serde_json::from_str(&row.get::<_, String>(5)?).unwrap_or_default(),
            })
        },
    )
    .optional()
}

pub fn put_media_file(
    conn: &Connection,
    media: i64,
    thumb: &str,
    blob: i64,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO media_files (media_id, thumb, blob_id) VALUES (?1, ?2, ?3)",
        params![media, thumb, blob],
    )?;
    Ok(())
}

/// The blob of a medium's file (`""`) or thumbnail / photo size.
pub fn media_file(conn: &Connection, media: i64, thumb: &str) -> rusqlite::Result<Option<i64>> {
    conn.query_row(
        "SELECT blob_id FROM media_files WHERE media_id = ?1 AND thumb = ?2",
        params![media, thumb],
        |row| row.get(0),
    )
    .optional()
}
