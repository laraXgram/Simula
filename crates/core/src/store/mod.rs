//! SQLite storage for one sandbox: connection setup, migrations, and row-level queries.
//! Queries take `&Connection`, so they work inside the handler's transaction.

pub mod accounts;
pub mod bot_commands;
pub mod channels;
pub mod chats;
pub mod dialogs;
pub mod files;
pub mod invites;
pub mod messages;
pub mod meta;
pub mod topics;
pub mod updates;

use std::path::Path;

use rusqlite::Connection;

const MIGRATIONS: &[&str] = &[
    include_str!("migrations/0001_init.sql"),
    include_str!("migrations/0002_bot_commands.sql"),
    include_str!("migrations/0003_dialogs.sql"),
    include_str!("migrations/0004_chats.sql"),
    include_str!("migrations/0005_channels.sql"),
    include_str!("migrations/0006_invites.sql"),
    include_str!("migrations/0007_forums.sql"),
    include_str!("migrations/0008_files.sql"),
    include_str!("migrations/0009_invite_joins.sql"),
];

/// Open (or create) a sandbox database. `None` keeps it in memory, for tests.
pub fn open(path: Option<&Path>) -> rusqlite::Result<Connection> {
    let mut conn = match path {
        Some(path) => Connection::open(path)?,
        None => Connection::open_in_memory()?,
    };
    conn.pragma_update(None, "foreign_keys", true)?;
    if path.is_some() {
        conn.pragma_update(None, "journal_mode", "wal")?;
        conn.pragma_update(None, "synchronous", "normal")?;
    }
    migrate(&mut conn)?;
    Ok(conn)
}

fn migrate(conn: &mut Connection) -> rusqlite::Result<()> {
    let applied: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    for (version, sql) in (1..).zip(MIGRATIONS).skip(applied as usize) {
        let tx = conn.transaction()?;
        tx.execute_batch(sql)?;
        tx.pragma_update(None, "user_version", version as i64)?;
        tx.commit()?;
    }
    Ok(())
}
