//! Message boxes. A message in a private chat or basic group is stored once per
//! participant, each copy with the id and perspective (`out`, `peer_id`) of that
//! participant. Regular messages are stored as `message` objects; service messages
//! (a member joined, the title changed…) as tagged `messageService` objects.

use rusqlite::{Connection, OptionalExtension, params};
use simula_tl::{enums, types};

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

/// A service message (the action happened in `peer`).
pub fn insert_service(
    conn: &Connection,
    owner: UserId,
    peer: Peer,
    global_id: i64,
    message: &types::MessageService,
) -> rusqlite::Result<()> {
    let from = message
        .from_id
        .as_ref()
        .map(|p| Peer::from_tl(p).bot_api_id());
    let body = enums::Message::from(message.clone());
    conn.execute(
        "INSERT INTO messages (owner_id, id, peer, from_id, out, date, global_id, random_id, body)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8)",
        params![
            owner.0,
            message.id,
            peer.bot_api_id(),
            from,
            message.out,
            message.date,
            global_id,
            serde_json::to_string(&body).expect("TL message serializes"),
        ],
    )?;
    Ok(())
}

/// A stored body: tagged (service messages) or a plain `message`.
fn decode(body: &str) -> enums::Message {
    if let Ok(message) = serde_json::from_str::<enums::Message>(body) {
        return message;
    }
    let message: types::Message =
        serde_json::from_str(body).expect("stored message is valid JSON-TL");
    message.into()
}

fn regular(message: enums::Message) -> Option<types::Message> {
    match message {
        enums::Message::Message(m) => Some(*m),
        _ => None,
    }
}

/// Any message in `owner`'s box, service messages included.
pub fn get_any(
    conn: &Connection,
    owner: UserId,
    id: i32,
) -> rusqlite::Result<Option<enums::Message>> {
    let body: Option<String> = conn
        .query_row(
            "SELECT body FROM messages WHERE owner_id = ?1 AND id = ?2 AND deleted = 0",
            params![owner.0, id],
            |row| row.get(0),
        )
        .optional()?;
    Ok(body.map(|b| decode(&b)))
}

/// A regular message in `owner`'s box.
pub fn get(conn: &Connection, owner: UserId, id: i32) -> rusqlite::Result<Option<types::Message>> {
    Ok(get_any(conn, owner, id)?.and_then(regular))
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
    Ok(row.and_then(|(global_id, body)| regular(decode(&body)).map(|m| (global_id, m))))
}

/// A message in `owner`'s box with its `global_id`, whatever the dialog.
pub fn get_with_global(
    conn: &Connection,
    owner: UserId,
    id: i32,
) -> rusqlite::Result<Option<(i64, types::Message)>> {
    let row: Option<(i64, String)> = conn
        .query_row(
            "SELECT global_id, body FROM messages WHERE owner_id = ?1 AND id = ?2 AND deleted = 0",
            params![owner.0, id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    Ok(row.and_then(|(global_id, body)| regular(decode(&body)).map(|m| (global_id, m))))
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

/// Every live copy of a message: `(owner, id)` per box.
pub fn copies(conn: &Connection, global_id: i64) -> rusqlite::Result<Vec<(UserId, i32)>> {
    let mut statement = conn.prepare(
        "SELECT owner_id, id FROM messages WHERE global_id = ?1 AND deleted = 0 ORDER BY owner_id",
    )?;
    let rows = statement.query_map([global_id], |row| Ok((UserId(row.get(0)?), row.get(1)?)))?;
    rows.collect()
}

/// Store an edited copy (its body and edit date).
pub fn update(conn: &Connection, owner: UserId, message: &types::Message) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE messages SET body = ?3, edit_date = ?4 WHERE owner_id = ?1 AND id = ?2",
        params![
            owner.0,
            message.id,
            serde_json::to_string(message).expect("TL message serializes"),
            message.edit_date,
        ],
    )?;
    Ok(())
}

/// Remove a message from `owner`'s box. Ids are never reused.
pub fn delete(conn: &Connection, owner: UserId, id: i32) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE messages SET deleted = 1 WHERE owner_id = ?1 AND id = ?2",
        params![owner.0, id],
    )?;
    Ok(())
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
