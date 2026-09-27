//! Account rows (users and bots).

use rusqlite::{Connection, OptionalExtension, Row, params};

use crate::ids::UserId;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Account {
    pub id: UserId,
    pub is_bot: bool,
    pub first_name: String,
    pub last_name: Option<String>,
    pub username: Option<String>,
    pub phone: Option<String>,
    pub lang_code: Option<String>,
    pub is_premium: bool,
    /// Bots only: `<id>:<secret>`.
    pub bot_token: Option<String>,
    /// Less common attributes (bot settings, profile details), as JSON.
    pub extra: serde_json::Value,
    pub created_at: i64,
}

const COLUMNS: &str = "id, is_bot, first_name, last_name, username, phone, lang_code, is_premium, bot_token, extra, created_at";

fn from_row(row: &Row<'_>) -> rusqlite::Result<Account> {
    let extra: String = row.get(9)?;
    Ok(Account {
        id: UserId(row.get(0)?),
        is_bot: row.get(1)?,
        first_name: row.get(2)?,
        last_name: row.get(3)?,
        username: row.get(4)?,
        phone: row.get(5)?,
        lang_code: row.get(6)?,
        is_premium: row.get(7)?,
        bot_token: row.get(8)?,
        extra: serde_json::from_str(&extra).unwrap_or_default(),
        created_at: row.get(10)?,
    })
}

pub fn insert(conn: &Connection, account: &Account) -> rusqlite::Result<()> {
    conn.execute(
        &format!("INSERT INTO accounts ({COLUMNS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)"),
        params![
            account.id.0,
            account.is_bot,
            account.first_name,
            account.last_name,
            account.username,
            account.phone,
            account.lang_code,
            account.is_premium,
            account.bot_token,
            account.extra.to_string(),
            account.created_at,
        ],
    )?;
    conn.execute("INSERT INTO account_state (account_id) VALUES (?1)", [account.id.0])?;
    Ok(())
}

pub fn get(conn: &Connection, id: UserId) -> rusqlite::Result<Option<Account>> {
    conn.query_row(&format!("SELECT {COLUMNS} FROM accounts WHERE id = ?1"), [id.0], from_row)
        .optional()
}

pub fn by_token(conn: &Connection, token: &str) -> rusqlite::Result<Option<Account>> {
    conn.query_row(&format!("SELECT {COLUMNS} FROM accounts WHERE bot_token = ?1"), [token], from_row)
        .optional()
}

pub fn by_username(conn: &Connection, username: &str) -> rusqlite::Result<Option<Account>> {
    let name = username.trim_start_matches('@');
    conn.query_row(&format!("SELECT {COLUMNS} FROM accounts WHERE username = ?1"), [name], from_row)
        .optional()
}

pub fn list(conn: &Connection) -> rusqlite::Result<Vec<Account>> {
    let mut stmt = conn.prepare(&format!("SELECT {COLUMNS} FROM accounts ORDER BY id"))?;
    stmt.query_map([], from_row)?.collect()
}
