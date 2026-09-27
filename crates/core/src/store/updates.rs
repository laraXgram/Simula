//! Per-account pts and the log of pts-carrying updates.

use rusqlite::{Connection, params};
use simula_tl::enums;

use crate::ids::UserId;

/// Advance an account's pts by `count` and return the new value.
pub fn advance_pts(conn: &Connection, account: UserId, count: i32) -> rusqlite::Result<i32> {
    conn.query_row(
        "UPDATE account_state SET pts = pts + ?2 WHERE account_id = ?1 RETURNING pts",
        params![account.0, count],
        |row| row.get(0),
    )
}

pub fn state(conn: &Connection, account: UserId) -> rusqlite::Result<(i32, i32, i32)> {
    conn.query_row(
        "SELECT pts, qts, seq FROM account_state WHERE account_id = ?1",
        [account.0],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
}

pub fn log(
    conn: &Connection,
    account: UserId,
    pts: i32,
    pts_count: i32,
    date: i32,
    update: &enums::Update,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO updates_log (account_id, pts, pts_count, date, body) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![account.0, pts, pts_count, date, serde_json::to_string(update).expect("TL update serializes")],
    )?;
    Ok(())
}
