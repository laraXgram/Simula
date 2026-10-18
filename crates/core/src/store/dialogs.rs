//! Dialogs of per-account boxes: which peers an account has messages with, and how far
//! each side has read.

use rusqlite::{Connection, OptionalExtension, params};

use crate::ids::{Peer, UserId};

/// `(peer, top message id, top message date)` of every dialog, newest first.
pub fn list(conn: &Connection, owner: UserId) -> rusqlite::Result<Vec<(Peer, i32, i32)>> {
    let mut statement = conn.prepare(
        "SELECT peer, MAX(id), date FROM messages WHERE owner_id = ?1 AND deleted = 0
         GROUP BY peer ORDER BY date DESC, MAX(id) DESC",
    )?;
    let rows = statement.query_map([owner.0], |row| {
        Ok((row.get::<_, i64>(0)?, row.get(1)?, row.get(2)?))
    })?;
    rows.filter_map(|row| match row {
        Ok((peer, id, date)) => Peer::from_bot_api_id(peer).map(|peer| Ok((peer, id, date))),
        Err(error) => Some(Err(error)),
    })
    .collect()
}

/// `(read_inbox_max_id, read_outbox_max_id)`.
pub fn read_state(conn: &Connection, owner: UserId, peer: Peer) -> rusqlite::Result<(i32, i32)> {
    Ok(conn
        .query_row(
            "SELECT read_inbox_max_id, read_outbox_max_id FROM dialogs WHERE owner_id = ?1 AND peer = ?2",
            params![owner.0, peer.bot_api_id()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .unwrap_or((0, 0)))
}

pub fn set_read_inbox(
    conn: &Connection,
    owner: UserId,
    peer: Peer,
    max_id: i32,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO dialogs (owner_id, peer, read_inbox_max_id) VALUES (?1, ?2, ?3)
         ON CONFLICT (owner_id, peer) DO UPDATE SET read_inbox_max_id = excluded.read_inbox_max_id",
        params![owner.0, peer.bot_api_id(), max_id],
    )?;
    Ok(())
}

pub fn set_read_outbox(
    conn: &Connection,
    owner: UserId,
    peer: Peer,
    max_id: i32,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO dialogs (owner_id, peer, read_outbox_max_id) VALUES (?1, ?2, ?3)
         ON CONFLICT (owner_id, peer) DO UPDATE SET read_outbox_max_id = excluded.read_outbox_max_id",
        params![owner.0, peer.bot_api_id(), max_id],
    )?;
    Ok(())
}

/// Incoming messages after `after` in the dialog.
pub fn unread_count(
    conn: &Connection,
    owner: UserId,
    peer: Peer,
    after: i32,
) -> rusqlite::Result<i32> {
    conn.query_row(
        "SELECT COUNT(*) FROM messages
         WHERE owner_id = ?1 AND peer = ?2 AND out = 0 AND deleted = 0 AND id > ?3",
        params![owner.0, peer.bot_api_id(), after],
        |row| row.get(0),
    )
}

/// Ids of the dialog's messages, newest first.
pub fn message_ids(conn: &Connection, owner: UserId, peer: Peer) -> rusqlite::Result<Vec<i32>> {
    let mut statement = conn.prepare(
        "SELECT id FROM messages WHERE owner_id = ?1 AND peer = ?2 AND deleted = 0 ORDER BY id DESC",
    )?;
    let rows = statement.query_map(params![owner.0, peer.bot_api_id()], |row| row.get(0))?;
    rows.collect()
}

/// The highest id `other` has for the messages `owner` received from it up to `max_id`.
pub fn counterpart_max_id(
    conn: &Connection,
    owner: UserId,
    other: UserId,
    max_id: i32,
) -> rusqlite::Result<Option<i32>> {
    conn.query_row(
        "SELECT MAX(o.id) FROM messages r
         JOIN messages o ON o.global_id = r.global_id AND o.owner_id = ?2
         WHERE r.owner_id = ?1 AND r.peer = ?3 AND r.out = 0 AND r.id <= ?4",
        params![owner.0, other.0, Peer::User(other).bot_api_id(), max_id],
        |row| row.get(0),
    )
}
