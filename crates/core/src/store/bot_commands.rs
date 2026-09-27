//! Bots' command lists per scope and language.

use rusqlite::{Connection, OptionalExtension, params};
use simula_tl::enums;

use crate::ids::UserId;

pub fn get(
    conn: &Connection,
    bot: UserId,
    scope: &str,
    lang_code: &str,
) -> rusqlite::Result<Vec<enums::BotCommand>> {
    let json: Option<String> = conn
        .query_row(
            "SELECT commands FROM bot_commands WHERE bot_id = ?1 AND scope = ?2 AND lang_code = ?3",
            params![bot.0, scope, lang_code],
            |row| row.get(0),
        )
        .optional()?;
    Ok(json
        .map(|json| serde_json::from_str(&json).expect("stored commands are valid JSON-TL"))
        .unwrap_or_default())
}

/// Store a list; an empty one removes it.
pub fn set(
    conn: &Connection,
    bot: UserId,
    scope: &str,
    lang_code: &str,
    commands: &[enums::BotCommand],
) -> rusqlite::Result<()> {
    if commands.is_empty() {
        conn.execute(
            "DELETE FROM bot_commands WHERE bot_id = ?1 AND scope = ?2 AND lang_code = ?3",
            params![bot.0, scope, lang_code],
        )?;
        return Ok(());
    }
    conn.execute(
        "INSERT INTO bot_commands (bot_id, scope, lang_code, commands) VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT (bot_id, scope, lang_code) DO UPDATE SET commands = excluded.commands",
        params![
            bot.0,
            scope,
            lang_code,
            serde_json::to_string(commands).expect("TL commands serialize"),
        ],
    )?;
    Ok(())
}
