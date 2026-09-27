//! Message boxes. A private-chat message is stored once per participant, each copy with
//! the id and perspective (`out`, `peer_id`) of that participant.

use rusqlite::{Connection, OptionalExtension, params};
use simula_tl::types;

use crate::ids::{Peer, UserId};

/// Reserve the next message id in an account's box.
pub fn next_id(conn: &Connection, owner: UserId) -> rusqlite::Result<i32> {
    conn.query_row(
        "UPDATE account_state SET next_message_id = next_message_id + 1
         WHERE account_id = ?1 RETURNING next_message_id - 1",
        [owner.0],
        |row| row.get(0),
    )
}

pub struct StoredMessage<'a> {
    pub owner: UserId,
    pub peer: Peer,
    pub global_id: i64,
    pub random_id: Option<i64>,
    pub message: &'a types::Message,
}

pub fn insert(conn: &Connection, row: &StoredMessage<'_>) -> rusqlite::Result<()> {
    let from = row
        .message
        .from_id
        .as_ref()
        .map(|p| Peer::from_tl(p).bot_api_id());
    conn.execute(
        "INSERT INTO messages (owner_id, id, peer, from_id, out, date, global_id, random_id, body)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            row.owner.0,
            row.message.id,
            row.peer.bot_api_id(),
            from,
            row.message.out,
            row.message.date,
            row.global_id,
            row.random_id,
            serde_json::to_string(row.message).expect("TL message serializes"),
        ],
    )?;
    Ok(())
}

pub fn get(conn: &Connection, owner: UserId, id: i32) -> rusqlite::Result<Option<types::Message>> {
    let body: Option<String> = conn
        .query_row(
            "SELECT body FROM messages WHERE owner_id = ?1 AND id = ?2 AND deleted = 0",
            params![owner.0, id],
            |row| row.get(0),
        )
        .optional()?;
    Ok(body.map(|b| serde_json::from_str(&b).expect("stored message is valid JSON-TL")))
}

/// A message in `owner`'s box, if it belongs to the dialog with `peer`.
pub fn get_in(
    conn: &Connection,
    owner: UserId,
    peer: Peer,
    id: i32,
) -> rusqlite::Result<Option<(i64, types::Message)>> {
    let row: Option<(i64, String)> = conn
        .query_row(
            "SELECT global_id, body FROM messages
             WHERE owner_id = ?1 AND id = ?2 AND peer = ?3 AND deleted = 0",
            params![owner.0, id, peer.bot_api_id()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    Ok(row.map(|(global_id, body)| {
        let message = serde_json::from_str(&body).expect("stored message is valid JSON-TL");
        (global_id, message)
    }))
}

/// The id of `owner`'s copy of a message, found through the copies' shared `global_id`.
pub fn copy_id(conn: &Connection, owner: UserId, global_id: i64) -> rusqlite::Result<Option<i32>> {
    conn.query_row(
        "SELECT id FROM messages WHERE owner_id = ?1 AND global_id = ?2 AND deleted = 0",
        params![owner.0, global_id],
        |row| row.get(0),
    )
    .optional()
}

/// A sender's earlier message with the same `random_id` (client retries are idempotent).
pub fn by_random_id(
    conn: &Connection,
    owner: UserId,
    random_id: i64,
) -> rusqlite::Result<Option<i32>> {
    conn.query_row(
        "SELECT id FROM messages WHERE owner_id = ?1 AND random_id = ?2",
        params![owner.0, random_id],
        |row| row.get(0),
    )
    .optional()
}
