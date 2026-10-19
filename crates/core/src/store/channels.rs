//! Supergroups and channels: the channel rows, everyone's status in them, and their
//! shared message boxes.

use rusqlite::{Connection, OptionalExtension, Row, params};
use simula_tl::{enums, types};

use crate::ids::{ChannelId, UserId};

#[derive(Debug, Clone, PartialEq)]
pub struct Channel {
    pub id: ChannelId,
    /// A broadcast channel; otherwise a supergroup.
    pub broadcast: bool,
    pub title: String,
    pub username: Option<String>,
    pub about: String,
    pub creator: UserId,
    pub date: i32,
    pub pts: i32,
    pub deleted: bool,
    pub extra: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Creator,
    Admin,
    Member,
    /// A member with some rights taken away.
    Restricted,
    /// Removed and kept out ("kicked"), until unbanned or `until_date`.
    Banned,
    Left,
}

impl Status {
    fn as_str(self) -> &'static str {
        match self {
            Status::Creator => "creator",
            Status::Admin => "admin",
            Status::Member => "member",
            Status::Restricted => "restricted",
            Status::Banned => "banned",
            Status::Left => "left",
        }
    }

    fn parse(s: &str) -> Self {
        match s {
            "creator" => Status::Creator,
            "admin" => Status::Admin,
            "restricted" => Status::Restricted,
            "banned" => Status::Banned,
            "left" => Status::Left,
            _ => Status::Member,
        }
    }

    /// Whether this is a current member (gets the channel's updates).
    pub fn is_member(self) -> bool {
        matches!(
            self,
            Status::Creator | Status::Admin | Status::Member | Status::Restricted
        )
    }

    pub fn is_admin(self) -> bool {
        matches!(self, Status::Creator | Status::Admin)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Member {
    pub user: UserId,
    pub status: Status,
    pub inviter: UserId,
    pub date: i32,
    pub promoted_by: Option<UserId>,
    pub kicked_by: Option<UserId>,
    pub admin_rights: Option<types::ChatAdminRights>,
    pub banned_rights: Option<types::ChatBannedRights>,
    pub rank: Option<String>,
}

impl Member {
    pub fn new(user: UserId, status: Status, inviter: UserId, date: i32) -> Self {
        Self {
            user,
            status,
            inviter,
            date,
            promoted_by: None,
            kicked_by: None,
            admin_rights: None,
            banned_rights: None,
            rank: None,
        }
    }
}

const CHANNEL_COLUMNS: &str =
    "id, broadcast, title, username, about, creator_id, date, pts, deleted, extra";

fn channel_row(row: &Row<'_>) -> rusqlite::Result<Channel> {
    Ok(Channel {
        id: ChannelId(row.get(0)?),
        broadcast: row.get(1)?,
        title: row.get(2)?,
        username: row.get(3)?,
        about: row.get(4)?,
        creator: UserId(row.get(5)?),
        date: row.get(6)?,
        pts: row.get(7)?,
        deleted: row.get(8)?,
        extra: serde_json::from_str(&row.get::<_, String>(9)?).unwrap_or_default(),
    })
}

pub fn insert(conn: &Connection, channel: &Channel) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO channels (id, broadcast, title, username, about, creator_id, date, extra)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            channel.id.0,
            channel.broadcast,
            channel.title,
            channel.username,
            channel.about,
            channel.creator.0,
            channel.date,
            channel.extra.to_string()
        ],
    )?;
    Ok(())
}

pub fn get(conn: &Connection, id: ChannelId) -> rusqlite::Result<Option<Channel>> {
    conn.query_row(
        &format!("SELECT {CHANNEL_COLUMNS} FROM channels WHERE id = ?1"),
        [id.0],
        channel_row,
    )
    .optional()
}

pub fn by_username(conn: &Connection, username: &str) -> rusqlite::Result<Option<Channel>> {
    conn.query_row(
        &format!("SELECT {CHANNEL_COLUMNS} FROM channels WHERE username = ?1 AND deleted = 0"),
        [username],
        channel_row,
    )
    .optional()
}

pub fn set_title(conn: &Connection, id: ChannelId, title: &str) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE channels SET title = ?2 WHERE id = ?1",
        params![id.0, title],
    )?;
    Ok(())
}

pub fn set_about(conn: &Connection, id: ChannelId, about: &str) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE channels SET about = ?2 WHERE id = ?1",
        params![id.0, about],
    )?;
    Ok(())
}

pub fn set_username(
    conn: &Connection,
    id: ChannelId,
    username: Option<&str>,
) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE channels SET username = ?2 WHERE id = ?1",
        params![id.0, username],
    )?;
    Ok(())
}

pub fn set_extra(
    conn: &Connection,
    id: ChannelId,
    extra: &serde_json::Value,
) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE channels SET extra = ?2 WHERE id = ?1",
        params![id.0, extra.to_string()],
    )?;
    Ok(())
}

pub fn set_deleted(conn: &Connection, id: ChannelId) -> rusqlite::Result<()> {
    conn.execute("UPDATE channels SET deleted = 1 WHERE id = ?1", [id.0])?;
    Ok(())
}

/// Advance the channel's pts by `count` and return the new value.
pub fn advance_pts(conn: &Connection, id: ChannelId, count: i32) -> rusqlite::Result<i32> {
    conn.query_row(
        "UPDATE channels SET pts = pts + ?2 WHERE id = ?1 RETURNING pts",
        params![id.0, count],
        |row| row.get(0),
    )
}

pub fn log_update(
    conn: &Connection,
    id: ChannelId,
    pts: i32,
    pts_count: i32,
    date: i32,
    update: &enums::Update,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO channel_updates (channel_id, pts, pts_count, date, body) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![id.0, pts, pts_count, date, serde_json::to_string(update).expect("TL update serializes")],
    )?;
    Ok(())
}

// Members

const MEMBER_COLUMNS: &str =
    "user_id, status, inviter_id, date, promoted_by, kicked_by, admin_rights, banned_rights, rank";

fn member_row(row: &Row<'_>) -> rusqlite::Result<Member> {
    Ok(Member {
        user: UserId(row.get(0)?),
        status: Status::parse(&row.get::<_, String>(1)?),
        inviter: UserId(row.get(2)?),
        date: row.get(3)?,
        promoted_by: row.get::<_, Option<i64>>(4)?.map(UserId),
        kicked_by: row.get::<_, Option<i64>>(5)?.map(UserId),
        admin_rights: from_json(row.get(6)?),
        banned_rights: from_json(row.get(7)?),
        rank: row.get(8)?,
    })
}

/// Everyone with a status in the channel (members first by join date).
pub fn members(conn: &Connection, channel: ChannelId) -> rusqlite::Result<Vec<Member>> {
    let mut statement = conn.prepare(&format!(
        "SELECT {MEMBER_COLUMNS} FROM channel_members WHERE channel_id = ?1 ORDER BY date, user_id"
    ))?;
    let rows = statement.query_map([channel.0], member_row)?;
    rows.collect()
}

/// Current members only.
pub fn current_members(conn: &Connection, channel: ChannelId) -> rusqlite::Result<Vec<Member>> {
    Ok(members(conn, channel)?
        .into_iter()
        .filter(|m| m.status.is_member())
        .collect())
}

pub fn member(
    conn: &Connection,
    channel: ChannelId,
    user: UserId,
) -> rusqlite::Result<Option<Member>> {
    conn.query_row(
        &format!(
            "SELECT {MEMBER_COLUMNS} FROM channel_members WHERE channel_id = ?1 AND user_id = ?2"
        ),
        params![channel.0, user.0],
        member_row,
    )
    .optional()
}

/// Insert or replace `member`'s row.
pub fn put_member(conn: &Connection, channel: ChannelId, member: &Member) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO channel_members (channel_id, user_id, status, inviter_id, date, promoted_by, kicked_by, admin_rights, banned_rights, rank)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
         ON CONFLICT (channel_id, user_id) DO UPDATE SET
           status = excluded.status, inviter_id = excluded.inviter_id, date = excluded.date,
           promoted_by = excluded.promoted_by, kicked_by = excluded.kicked_by,
           admin_rights = excluded.admin_rights, banned_rights = excluded.banned_rights,
           rank = excluded.rank",
        params![
            channel.0,
            member.user.0,
            member.status.as_str(),
            member.inviter.0,
            member.date,
            member.promoted_by.map(|u| u.0),
            member.kicked_by.map(|u| u.0),
            member.admin_rights.as_ref().map(to_json),
            member.banned_rights.as_ref().map(to_json),
            member.rank,
        ],
    )?;
    Ok(())
}

pub fn remove_member(conn: &Connection, channel: ChannelId, user: UserId) -> rusqlite::Result<()> {
    conn.execute(
        "DELETE FROM channel_members WHERE channel_id = ?1 AND user_id = ?2",
        params![channel.0, user.0],
    )?;
    Ok(())
}

/// Channels where `user` has a status (any), with it.
pub fn of_user(conn: &Connection, user: UserId) -> rusqlite::Result<Vec<(ChannelId, Status)>> {
    let mut statement =
        conn.prepare("SELECT channel_id, status FROM channel_members WHERE user_id = ?1")?;
    let rows = statement.query_map([user.0], |row| {
        Ok((
            ChannelId(row.get(0)?),
            Status::parse(&row.get::<_, String>(1)?),
        ))
    })?;
    rows.collect()
}

// Messages

/// Reserve the next message id in the channel's box.
pub fn next_message_id(conn: &Connection, channel: ChannelId) -> rusqlite::Result<i32> {
    conn.query_row(
        "UPDATE channels SET next_message_id = next_message_id + 1
         WHERE id = ?1 RETURNING next_message_id - 1",
        [channel.0],
        |row| row.get(0),
    )
}

/// A stored channel message and its author.
#[derive(Debug, Clone)]
pub struct Stored {
    pub author: UserId,
    pub message: enums::Message,
}

/// Store a new message; `thread` is its topic or reply chain (see `services::forums`).
pub fn insert_message(
    conn: &Connection,
    channel: ChannelId,
    author: UserId,
    random_id: Option<i64>,
    thread: Option<i32>,
    message: &enums::Message,
) -> rusqlite::Result<()> {
    let (id, date) = match message {
        enums::Message::Message(m) => (m.id, m.date),
        enums::Message::Service(m) => (m.id, m.date),
        enums::Message::Empty(m) => (m.id, 0),
    };
    conn.execute(
        "INSERT INTO channel_messages (channel_id, id, author_id, date, random_id, body, thread_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            channel.0,
            id,
            author.0,
            date,
            random_id,
            serde_json::to_string(message).expect("TL message serializes"),
            thread
        ],
    )?;
    Ok(())
}

pub fn message(conn: &Connection, channel: ChannelId, id: i32) -> rusqlite::Result<Option<Stored>> {
    conn.query_row(
        "SELECT author_id, body FROM channel_messages WHERE channel_id = ?1 AND id = ?2 AND deleted = 0",
        params![channel.0, id],
        |row| {
            Ok(Stored {
                author: UserId(row.get(0)?),
                message: serde_json::from_str(&row.get::<_, String>(1)?)
                    .expect("stored message is valid JSON-TL"),
            })
        },
    )
    .optional()
}

pub fn update_message(
    conn: &Connection,
    channel: ChannelId,
    message: &types::Message,
) -> rusqlite::Result<()> {
    let body = enums::Message::from(message.clone());
    conn.execute(
        "UPDATE channel_messages SET body = ?3, edit_date = ?4 WHERE channel_id = ?1 AND id = ?2",
        params![
            channel.0,
            message.id,
            serde_json::to_string(&body).expect("TL message serializes"),
            message.edit_date
        ],
    )?;
    Ok(())
}

pub fn delete_message(conn: &Connection, channel: ChannelId, id: i32) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE channel_messages SET deleted = 1 WHERE channel_id = ?1 AND id = ?2",
        params![channel.0, id],
    )?;
    Ok(())
}

/// Ids of the channel's live messages, newest first.
pub fn message_ids(conn: &Connection, channel: ChannelId) -> rusqlite::Result<Vec<i32>> {
    let mut statement = conn.prepare(
        "SELECT id FROM channel_messages WHERE channel_id = ?1 AND deleted = 0 ORDER BY id DESC",
    )?;
    let rows = statement.query_map([channel.0], |row| row.get(0))?;
    rows.collect()
}

/// `(id, date)` of the newest live message.
pub fn top(conn: &Connection, channel: ChannelId) -> rusqlite::Result<Option<(i32, i32)>> {
    conn.query_row(
        "SELECT id, date FROM channel_messages WHERE channel_id = ?1 AND deleted = 0
         ORDER BY id DESC LIMIT 1",
        [channel.0],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()
}

/// Messages after `after` that `reader` didn't send.
pub fn unread_count(
    conn: &Connection,
    channel: ChannelId,
    reader: UserId,
    after: i32,
) -> rusqlite::Result<i32> {
    conn.query_row(
        "SELECT COUNT(*) FROM channel_messages
         WHERE channel_id = ?1 AND author_id != ?2 AND deleted = 0 AND id > ?3",
        params![channel.0, reader.0, after],
        |row| row.get(0),
    )
}

/// Authors of messages in `(after, up_to]`, other than `reader`.
pub fn authors_between(
    conn: &Connection,
    channel: ChannelId,
    reader: UserId,
    after: i32,
    up_to: i32,
) -> rusqlite::Result<Vec<UserId>> {
    let mut statement = conn.prepare(
        "SELECT DISTINCT author_id FROM channel_messages
         WHERE channel_id = ?1 AND author_id != ?2 AND deleted = 0 AND id > ?3 AND id <= ?4",
    )?;
    let rows = statement.query_map(params![channel.0, reader.0, after, up_to], |row| {
        Ok(UserId(row.get(0)?))
    })?;
    rows.collect()
}

/// The author's earlier message with the same `random_id`.
pub fn by_random_id(
    conn: &Connection,
    channel: ChannelId,
    author: UserId,
    random_id: i64,
) -> rusqlite::Result<Option<i32>> {
    conn.query_row(
        "SELECT id FROM channel_messages WHERE channel_id = ?1 AND author_id = ?2 AND random_id = ?3",
        params![channel.0, author.0, random_id],
        |row| row.get(0),
    )
    .optional()
}

fn from_json<T: serde::de::DeserializeOwned>(text: Option<String>) -> Option<T> {
    text.and_then(|t| serde_json::from_str(&t).ok())
}

fn to_json(value: &impl serde::Serialize) -> String {
    serde_json::to_string(value).expect("TL object serializes")
}

/// Ids of the channel's pinned messages, newest first.
pub fn pinned_ids(conn: &Connection, channel: ChannelId) -> rusqlite::Result<Vec<i32>> {
    let mut statement = conn.prepare(
        "SELECT id FROM channel_messages WHERE channel_id = ?1 AND deleted = 0
         AND json_extract(body, '$.pinned') = 1 ORDER BY id DESC",
    )?;
    let rows = statement.query_map([channel.0], |row| row.get(0))?;
    rows.collect()
}
