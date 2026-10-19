//! Basic groups and their members.

use rusqlite::{Connection, OptionalExtension, params};

use crate::ids::{ChatId, UserId};

#[derive(Debug, Clone, PartialEq)]
pub struct Chat {
    pub id: ChatId,
    pub title: String,
    pub creator: UserId,
    pub date: i32,
    /// Bumped on every change of the member list, as `chatParticipants.version`.
    pub version: i32,
    pub deactivated: bool,
    pub extra: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Creator,
    Admin,
    Member,
}

impl Role {
    fn as_str(self) -> &'static str {
        match self {
            Role::Creator => "creator",
            Role::Admin => "admin",
            Role::Member => "member",
        }
    }

    fn parse(s: &str) -> Self {
        match s {
            "creator" => Role::Creator,
            "admin" => Role::Admin,
            _ => Role::Member,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Member {
    pub user: UserId,
    pub role: Role,
    pub inviter: UserId,
    pub date: i32,
}

pub fn insert(conn: &Connection, chat: &Chat) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO chats (id, title, creator_id, date, version, deactivated, extra)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            chat.id.0,
            chat.title,
            chat.creator.0,
            chat.date,
            chat.version,
            chat.deactivated,
            chat.extra.to_string()
        ],
    )?;
    Ok(())
}

pub fn get(conn: &Connection, id: ChatId) -> rusqlite::Result<Option<Chat>> {
    conn.query_row(
        "SELECT id, title, creator_id, date, version, deactivated, extra FROM chats WHERE id = ?1",
        [id.0],
        |row| {
            Ok(Chat {
                id: ChatId(row.get(0)?),
                title: row.get(1)?,
                creator: UserId(row.get(2)?),
                date: row.get(3)?,
                version: row.get(4)?,
                deactivated: row.get(5)?,
                extra: serde_json::from_str(&row.get::<_, String>(6)?).unwrap_or_default(),
            })
        },
    )
    .optional()
}

pub fn set_title(conn: &Connection, id: ChatId, title: &str) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE chats SET title = ?2 WHERE id = ?1",
        params![id.0, title],
    )?;
    Ok(())
}

pub fn set_extra(conn: &Connection, id: ChatId, extra: &serde_json::Value) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE chats SET extra = ?2 WHERE id = ?1",
        params![id.0, extra.to_string()],
    )?;
    Ok(())
}

/// The group was upgraded to a supergroup (or deleted): it takes no more messages.
pub fn deactivate(conn: &Connection, id: ChatId) -> rusqlite::Result<()> {
    conn.execute("UPDATE chats SET deactivated = 1 WHERE id = ?1", [id.0])?;
    Ok(())
}

/// Bump the member list's version and return the new one.
pub fn bump_version(conn: &Connection, id: ChatId) -> rusqlite::Result<i32> {
    conn.query_row(
        "UPDATE chats SET version = version + 1 WHERE id = ?1 RETURNING version",
        [id.0],
        |row| row.get(0),
    )
}

pub fn members(conn: &Connection, chat: ChatId) -> rusqlite::Result<Vec<Member>> {
    let mut statement = conn.prepare(
        "SELECT user_id, role, inviter_id, date FROM chat_members WHERE chat_id = ?1 ORDER BY date, user_id",
    )?;
    let rows = statement.query_map([chat.0], |row| {
        Ok(Member {
            user: UserId(row.get(0)?),
            role: Role::parse(&row.get::<_, String>(1)?),
            inviter: UserId(row.get(2)?),
            date: row.get(3)?,
        })
    })?;
    rows.collect()
}

pub fn member(conn: &Connection, chat: ChatId, user: UserId) -> rusqlite::Result<Option<Member>> {
    Ok(members(conn, chat)?.into_iter().find(|m| m.user == user))
}

pub fn add_member(conn: &Connection, chat: ChatId, member: &Member) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO chat_members (chat_id, user_id, role, inviter_id, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![chat.0, member.user.0, member.role.as_str(), member.inviter.0, member.date],
    )?;
    Ok(())
}

pub fn remove_member(conn: &Connection, chat: ChatId, user: UserId) -> rusqlite::Result<()> {
    conn.execute(
        "DELETE FROM chat_members WHERE chat_id = ?1 AND user_id = ?2",
        params![chat.0, user.0],
    )?;
    Ok(())
}

/// Basic groups `user` is in.
pub fn of_user(conn: &Connection, user: UserId) -> rusqlite::Result<Vec<ChatId>> {
    let mut statement = conn.prepare("SELECT chat_id FROM chat_members WHERE user_id = ?1")?;
    let rows = statement.query_map([user.0], |row| Ok(ChatId(row.get(0)?)))?;
    rows.collect()
}
