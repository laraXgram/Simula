//! Creating users and bots (a Control API operation; Telegram does this outside TL).

use rand::RngExt;
use serde_json::json;

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::UserId;
use crate::store::{self, accounts::Account};

/// Id ranges keep users and bots recognizable in logs.
const FIRST_USER_ID: i64 = 1_000_000_001;
const FIRST_BOT_ID: i64 = 7_000_000_001;

#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct NewUser {
    pub first_name: String,
    pub last_name: Option<String>,
    pub username: Option<String>,
    pub phone: Option<String>,
    pub lang_code: Option<String>,
    #[serde(default)]
    pub is_premium: bool,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct NewBot {
    pub first_name: String,
    /// Must end in "bot", like on Telegram.
    pub username: String,
    /// Keep a known token (e.g. from a bot's config); generated when absent.
    pub token: Option<String>,
    /// Privacy mode on by default, as for new bots from @BotFather.
    pub privacy_mode: Option<bool>,
    pub can_join_groups: Option<bool>,
}

pub fn create_user(ctx: &mut Ctx<'_>, new: NewUser) -> RpcResult<Account> {
    if new.first_name.trim().is_empty() {
        return Err(RpcError::bad_request("FIRSTNAME_INVALID"));
    }
    if let Some(username) = &new.username {
        check_username(ctx, username)?;
    }
    let account = Account {
        id: free_id(ctx, "user_id", FIRST_USER_ID)?,
        is_bot: false,
        first_name: new.first_name,
        last_name: new.last_name,
        username: new.username.map(strip_at),
        phone: new.phone,
        lang_code: new.lang_code,
        is_premium: new.is_premium,
        bot_token: None,
        extra: json!({}),
        created_at: ctx.clock.now_ms(),
    };
    store::accounts::insert(ctx.db, &account)?;
    Ok(account)
}

pub fn create_bot(ctx: &mut Ctx<'_>, new: NewBot) -> RpcResult<Account> {
    let username = strip_at(new.username);
    if !username.to_ascii_lowercase().ends_with("bot") {
        return Err(RpcError::bad_request("USERNAME_INVALID"));
    }
    check_username(ctx, &username)?;
    // A known token fixes the id: frameworks read the bot id from the token prefix.
    let (id, token) = match new.token {
        Some(token) => (token_id(&token)?, token),
        None => {
            let id = free_id(ctx, "bot_id", FIRST_BOT_ID)?;
            (id, generate_token(id))
        }
    };
    if store::accounts::get(ctx.db, id)?.is_some() {
        return Err(RpcError::bad_request("BOT_TOKEN_OCCUPIED"));
    }
    let account = Account {
        id,
        is_bot: true,
        first_name: new.first_name,
        last_name: None,
        username: Some(username),
        phone: None,
        lang_code: None,
        is_premium: false,
        bot_token: Some(token),
        extra: json!({ "bot": {
            "privacy_mode": new.privacy_mode.unwrap_or(true),
            "can_join_groups": new.can_join_groups.unwrap_or(true),
        } }),
        created_at: ctx.clock.now_ms(),
    };
    store::accounts::insert(ctx.db, &account)?;
    Ok(account)
}

fn check_username(ctx: &Ctx<'_>, username: &str) -> RpcResult<()> {
    let name = username.trim_start_matches('@');
    let valid = (5..=32).contains(&name.len())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && name.starts_with(|c: char| c.is_ascii_alphabetic());
    if !valid {
        return Err(RpcError::bad_request("USERNAME_INVALID"));
    }
    if store::accounts::by_username(ctx.db, name)?.is_some() {
        return Err(RpcError::bad_request("USERNAME_OCCUPIED"));
    }
    Ok(())
}

/// Next id of a sequence that isn't taken (ids from imported tokens can sit in its way).
fn free_id(ctx: &Ctx<'_>, sequence: &str, start: i64) -> RpcResult<UserId> {
    loop {
        let id = UserId(store::meta::next(ctx.db, sequence, start)?);
        if store::accounts::get(ctx.db, id)?.is_none() {
            return Ok(id);
        }
    }
}

fn token_id(token: &str) -> RpcResult<UserId> {
    let invalid = || RpcError::bad_request("BOT_TOKEN_INVALID");
    let (prefix, secret) = token.split_once(':').ok_or_else(invalid)?;
    let id: i64 = prefix.parse().map_err(|_| invalid())?;
    if id <= 0 || secret.len() < 8 {
        return Err(invalid());
    }
    Ok(UserId(id))
}

fn strip_at(username: String) -> String {
    username.trim_start_matches('@').to_owned()
}

/// `<bot id>:AA` + 33 URL-safe characters, the shape of real tokens.
fn generate_token(id: UserId) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-";
    let mut rng = rand::rng();
    let secret: String = (0..33)
        .map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char)
        .collect();
    format!("{}:AA{secret}", id.0)
}
