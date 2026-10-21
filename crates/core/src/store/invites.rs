//! Invite links and join requests.

use rusqlite::{Connection, OptionalExtension, Row, params};

use crate::ids::{Peer, UserId};

#[derive(Debug, Clone, PartialEq)]
pub struct Invite {
    pub hash: String,
    pub peer: Peer,
    pub admin: UserId,
    pub date: i32,
    pub expire_date: Option<i32>,
    pub usage_limit: Option<i32>,
    pub usage: i32,
    pub request_needed: bool,
    pub revoked: bool,
    pub permanent: bool,
    pub title: Option<String>,
}

const COLUMNS: &str = "hash, peer, admin_id, date, expire_date, usage_limit, usage, request_needed, revoked, permanent, title";

fn row(row: &Row<'_>) -> rusqlite::Result<Invite> {
    Ok(Invite {
        hash: row.get(0)?,
        peer: Peer::from_bot_api_id(row.get(1)?).expect("stored peers are valid"),
        admin: UserId(row.get(2)?),
        date: row.get(3)?,
        expire_date: row.get(4)?,
        usage_limit: row.get(5)?,
        usage: row.get(6)?,
        request_needed: row.get(7)?,
        revoked: row.get(8)?,
        permanent: row.get(9)?,
        title: row.get(10)?,
    })
}

/// Insert or replace an invite.
pub fn put(conn: &Connection, invite: &Invite) -> rusqlite::Result<()> {
    conn.execute(
        &format!(
            "INSERT OR REPLACE INTO invites ({COLUMNS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)"
        ),
        params![
            invite.hash,
            invite.peer.bot_api_id(),
            invite.admin.0,
            invite.date,
            invite.expire_date,
            invite.usage_limit,
            invite.usage,
            invite.request_needed,
            invite.revoked,
            invite.permanent,
            invite.title
        ],
    )?;
    Ok(())
}

pub fn get(conn: &Connection, hash: &str) -> rusqlite::Result<Option<Invite>> {
    conn.query_row(
        &format!("SELECT {COLUMNS} FROM invites WHERE hash = ?1"),
        [hash],
        row,
    )
    .optional()
}

/// `admin`'s live primary link in `peer`.
pub fn permanent(conn: &Connection, peer: Peer, admin: UserId) -> rusqlite::Result<Option<Invite>> {
    conn.query_row(
        &format!(
            "SELECT {COLUMNS} FROM invites WHERE peer = ?1 AND admin_id = ?2 AND permanent = 1 AND revoked = 0"
        ),
        params![peer.bot_api_id(), admin.0],
        row,
    )
    .optional()
}

/// A pending join request: `(date, invite hash, about)`.
pub fn request(
    conn: &Connection,
    peer: Peer,
    user: UserId,
) -> rusqlite::Result<Option<(i32, String, String)>> {
    conn.query_row(
        "SELECT date, invite, about FROM join_requests WHERE peer = ?1 AND user_id = ?2",
        params![peer.bot_api_id(), user.0],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .optional()
}

pub fn add_request(
    conn: &Connection,
    peer: Peer,
    user: UserId,
    date: i32,
    invite: &str,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO join_requests (peer, user_id, date, invite) VALUES (?1, ?2, ?3, ?4)",
        params![peer.bot_api_id(), user.0, date, invite],
    )?;
    Ok(())
}

pub fn remove_request(conn: &Connection, peer: Peer, user: UserId) -> rusqlite::Result<()> {
    conn.execute(
        "DELETE FROM join_requests WHERE peer = ?1 AND user_id = ?2",
        params![peer.bot_api_id(), user.0],
    )?;
    Ok(())
}

/// Pending requesters, newest first.
pub fn requesters(conn: &Connection, peer: Peer) -> rusqlite::Result<Vec<UserId>> {
    let mut statement = conn.prepare(
        "SELECT user_id FROM join_requests WHERE peer = ?1 ORDER BY date DESC, user_id DESC",
    )?;
    let rows = statement.query_map([peer.bot_api_id()], |row| Ok(UserId(row.get(0)?)))?;
    rows.collect()
}

/// Pending requests made through `hash`.
pub fn requests_via(conn: &Connection, hash: &str) -> rusqlite::Result<i32> {
    conn.query_row(
        "SELECT COUNT(*) FROM join_requests WHERE invite = ?1",
        [hash],
        |row| row.get(0),
    )
}

/// Chats where `user` has a pending request, with its date.
pub fn requests_of(conn: &Connection, user: UserId) -> rusqlite::Result<Vec<(Peer, i32)>> {
    let mut statement = conn.prepare("SELECT peer, date FROM join_requests WHERE user_id = ?1")?;
    let rows = statement.query_map([user.0], |row| {
        Ok((
            Peer::from_bot_api_id(row.get(0)?).expect("stored peers are valid"),
            row.get(1)?,
        ))
    })?;
    rows.collect()
}

/// The chat's links made by `admin`, live or `revoked`, newest first.
pub fn list(
    conn: &Connection,
    peer: Peer,
    admin: UserId,
    revoked: bool,
) -> rusqlite::Result<Vec<Invite>> {
    let mut statement = conn.prepare(&format!(
        "SELECT {COLUMNS} FROM invites WHERE peer = ?1 AND admin_id = ?2 AND revoked = ?3
         ORDER BY permanent DESC, date DESC, hash"
    ))?;
    let rows = statement.query_map(params![peer.bot_api_id(), admin.0, revoked], row)?;
    rows.collect()
}

pub fn delete(conn: &Connection, hash: &str) -> rusqlite::Result<()> {
    conn.execute("DELETE FROM invites WHERE hash = ?1", [hash])?;
    conn.execute("DELETE FROM invite_joins WHERE hash = ?1", [hash])?;
    Ok(())
}

pub fn add_join(
    conn: &Connection,
    hash: &str,
    user: UserId,
    date: i32,
    approved_by: Option<UserId>,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO invite_joins (hash, user_id, date, approved_by) VALUES (?1, ?2, ?3, ?4)",
        params![hash, user.0, date, approved_by.map(|u| u.0)],
    )?;
    Ok(())
}

/// Who joined through a link: `(user, date, approved by)`, newest first.
pub fn joins(
    conn: &Connection,
    hash: &str,
) -> rusqlite::Result<Vec<(UserId, i32, Option<UserId>)>> {
    let mut statement = conn.prepare(
        "SELECT user_id, date, approved_by FROM invite_joins WHERE hash = ?1 ORDER BY date DESC, user_id DESC",
    )?;
    let rows = statement.query_map([hash], |row| {
        Ok((
            UserId(row.get(0)?),
            row.get(1)?,
            row.get::<_, Option<i64>>(2)?.map(UserId),
        ))
    })?;
    rows.collect()
}

/// Pending requests: `(user, date, invite hash, about)`, newest first.
pub fn requests(
    conn: &Connection,
    peer: Peer,
) -> rusqlite::Result<Vec<(UserId, i32, String, String)>> {
    let mut statement = conn.prepare(
        "SELECT user_id, date, invite, about FROM join_requests WHERE peer = ?1 ORDER BY date DESC, user_id DESC",
    )?;
    let rows = statement.query_map([peer.bot_api_id()], |row| {
        Ok((UserId(row.get(0)?), row.get(1)?, row.get(2)?, row.get(3)?))
    })?;
    rows.collect()
}
