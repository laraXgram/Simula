//! Key/value bookkeeping and id sequences.

use rusqlite::{Connection, OptionalExtension, params};

pub fn get(conn: &Connection, key: &str) -> rusqlite::Result<Option<String>> {
    conn.query_row("SELECT value FROM meta WHERE key = ?1", [key], |row| {
        row.get(0)
    })
    .optional()
}

pub fn set(conn: &Connection, key: &str, value: &str) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO meta (key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key, value],
    )?;
    Ok(())
}

/// Next value of a named sequence, starting at `start`.
pub fn next(conn: &Connection, sequence: &str, start: i64) -> rusqlite::Result<i64> {
    let key = format!("seq:{sequence}");
    let value = match get(conn, &key)? {
        Some(current) => current.parse::<i64>().unwrap_or(start - 1) + 1,
        None => start,
    };
    set(conn, &key, &value.to_string())?;
    Ok(value)
}
