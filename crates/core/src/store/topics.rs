//! Forum topics.

use rusqlite::{Connection, OptionalExtension, Row, params};

use crate::ids::{ChannelId, UserId};

#[derive(Debug, Clone, PartialEq)]
pub struct Topic {
    pub channel: ChannelId,
    pub id: i32,
    pub title: String,
    pub icon_color: i32,
    pub icon_emoji_id: Option<i64>,
    pub creator: UserId,
    pub date: i32,
    pub closed: bool,
    pub hidden: bool,
    pub pinned: Option<i32>,
}

const COLUMNS: &str =
    "channel_id, id, title, icon_color, icon_emoji_id, creator_id, date, closed, hidden, pinned";

fn row(row: &Row<'_>) -> rusqlite::Result<Topic> {
    Ok(Topic {
        channel: ChannelId(row.get(0)?),
        id: row.get(1)?,
        title: row.get(2)?,
        icon_color: row.get(3)?,
        icon_emoji_id: row.get(4)?,
        creator: UserId(row.get(5)?),
        date: row.get(6)?,
        closed: row.get(7)?,
        hidden: row.get(8)?,
        pinned: row.get(9)?,
    })
}

/// Insert or replace a topic.
pub fn put(conn: &Connection, topic: &Topic) -> rusqlite::Result<()> {
    conn.execute(
        &format!(
            "INSERT OR REPLACE INTO forum_topics ({COLUMNS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"
        ),
        params![
            topic.channel.0,
            topic.id,
            topic.title,
            topic.icon_color,
            topic.icon_emoji_id,
            topic.creator.0,
            topic.date,
            topic.closed,
            topic.hidden,
            topic.pinned
        ],
    )?;
    Ok(())
}

pub fn get(conn: &Connection, channel: ChannelId, id: i32) -> rusqlite::Result<Option<Topic>> {
    conn.query_row(
        &format!("SELECT {COLUMNS} FROM forum_topics WHERE channel_id = ?1 AND id = ?2"),
        params![channel.0, id],
        row,
    )
    .optional()
}

/// Every topic of the forum, General included.
pub fn all(conn: &Connection, channel: ChannelId) -> rusqlite::Result<Vec<Topic>> {
    let mut statement = conn.prepare(&format!(
        "SELECT {COLUMNS} FROM forum_topics WHERE channel_id = ?1 ORDER BY id"
    ))?;
    let rows = statement.query_map([channel.0], row)?;
    rows.collect()
}

pub fn delete(conn: &Connection, channel: ChannelId, id: i32) -> rusqlite::Result<()> {
    conn.execute(
        "DELETE FROM forum_topics WHERE channel_id = ?1 AND id = ?2",
        params![channel.0, id],
    )?;
    Ok(())
}

/// Ids of the live messages of a thread (a topic, or a reply chain), newest first.
pub fn message_ids(
    conn: &Connection,
    channel: ChannelId,
    thread: i32,
) -> rusqlite::Result<Vec<i32>> {
    let mut statement = conn.prepare(
        "SELECT id FROM channel_messages
         WHERE channel_id = ?1 AND thread_id = ?2 AND deleted = 0 ORDER BY id DESC",
    )?;
    let rows = statement.query_map(params![channel.0, thread], |row| row.get(0))?;
    rows.collect()
}

/// The thread a stored channel message belongs to.
pub fn thread_of(conn: &Connection, channel: ChannelId, id: i32) -> rusqlite::Result<Option<i32>> {
    Ok(conn
        .query_row(
            "SELECT thread_id FROM channel_messages WHERE channel_id = ?1 AND id = ?2",
            params![channel.0, id],
            |row| row.get::<_, Option<i32>>(0),
        )
        .optional()?
        .flatten())
}
