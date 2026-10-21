//! Creating users and bots (a Control API operation; Telegram does this outside TL),
//! editing their profiles, and deleting them.

use rand::RngExt;
use serde_json::json;

use simula_tl::types;

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::UserId;
use crate::services::updates;
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

/// What editing an account changes; `None` leaves a field as it is (an empty string
/// clears optional ones).
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct AccountChange {
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub username: Option<String>,
    pub phone: Option<String>,
    pub lang_code: Option<String>,
    /// A user's bio (70 characters at most).
    pub about: Option<String>,
    pub is_premium: Option<bool>,
    /// Bots: BotFather's settings.
    pub privacy_mode: Option<bool>,
    pub can_join_groups: Option<bool>,
}

const MAX_NAME_LENGTH: usize = 64;
const MAX_ABOUT_LENGTH: usize = 70;

/// Edit an account (the user's own profile, or the Control API). Everyone who knows the
/// account learns its new name and username (`updateUserName`).
pub fn update(ctx: &mut Ctx<'_>, id: UserId, change: AccountChange) -> RpcResult<Account> {
    let mut account = ctx.account(id)?;
    if account.is_deleted() {
        return Err(RpcError::bad_request("USER_DEACTIVATED"));
    }
    if let Some(first) = change.first_name {
        let first = first.trim();
        if first.is_empty() || first.chars().count() > MAX_NAME_LENGTH {
            return Err(RpcError::bad_request("FIRSTNAME_INVALID"));
        }
        account.first_name = first.to_owned();
    }
    if let Some(last) = change.last_name {
        let last = last.trim();
        if last.chars().count() > MAX_NAME_LENGTH {
            return Err(RpcError::bad_request("LASTNAME_INVALID"));
        }
        account.last_name = Some(last.to_owned()).filter(|l| !l.is_empty());
    }
    if let Some(username) = change.username {
        let username = strip_at(username);
        let same = account
            .username
            .as_deref()
            .is_some_and(|u| u.eq_ignore_ascii_case(&username));
        if username.is_empty() {
            if account.is_bot {
                return Err(RpcError::bad_request("USERNAME_INVALID"));
            }
            account.username = None;
        } else if !same {
            if account.is_bot && !username.to_ascii_lowercase().ends_with("bot") {
                return Err(RpcError::bad_request("USERNAME_INVALID"));
            }
            check_username(ctx, &username)?;
            account.username = Some(username);
        } else {
            account.username = Some(username);
        }
    }
    if let Some(phone) = change.phone {
        account.phone =
            Some(phone.trim().trim_start_matches('+').to_owned()).filter(|p| !p.is_empty());
    }
    if let Some(lang) = change.lang_code {
        account.lang_code = Some(lang.trim().to_owned()).filter(|l| !l.is_empty());
    }
    if let Some(about) = change.about {
        if about.chars().count() > MAX_ABOUT_LENGTH {
            return Err(RpcError::bad_request("ABOUT_TOO_LONG"));
        }
        account.extra["about"] = about.trim().into();
    }
    if let Some(premium) = change.is_premium {
        account.is_premium = premium;
    }
    if account.is_bot {
        if let Some(privacy) = change.privacy_mode {
            account.extra["bot"]["privacy_mode"] = privacy.into();
        }
        if let Some(groups) = change.can_join_groups {
            account.extra["bot"]["can_join_groups"] = groups.into();
        }
    }
    store::accounts::update(ctx.db, &account)?;
    let usernames: Vec<_> = account
        .username
        .iter()
        .map(|u| {
            types::Username {
                editable: true,
                active: true,
                username: u.clone(),
            }
            .into()
        })
        .collect();
    for viewer in audience(ctx, id)? {
        updates::push_plain(
            ctx,
            viewer,
            types::UpdateUserName {
                user_id: id.0,
                first_name: account.first_name.clone(),
                last_name: account.last_name.clone().unwrap_or_default(),
                usernames: usernames.clone(),
            }
            .into(),
            [id],
        );
    }
    Ok(account)
}

/// Delete an account. As on Telegram it becomes "Deleted Account": its messages and
/// memberships stay, its name, username, phone and bot token go, and nobody can write
/// to it or sign in as it.
pub fn delete(ctx: &mut Ctx<'_>, id: UserId) -> RpcResult<()> {
    let mut account = ctx.account(id)?;
    if account.is_deleted() {
        return Err(RpcError::bad_request("USER_DEACTIVATED"));
    }
    let audience = audience(ctx, id)?;
    account.first_name = String::new();
    account.last_name = None;
    account.username = None;
    account.phone = None;
    account.bot_token = None;
    account.extra["deleted"] = true.into();
    store::accounts::update(ctx.db, &account)?;
    for viewer in audience.into_iter().filter(|&v| v != id) {
        updates::push_plain(
            ctx,
            viewer,
            types::UpdateUser { user_id: id.0 }.into(),
            [id],
        );
    }
    Ok(())
}

/// Who knows the account: itself, whoever shares a dialog with it, and the members of
/// its groups and channels.
pub fn audience(ctx: &Ctx<'_>, id: UserId) -> RpcResult<Vec<UserId>> {
    let peer = crate::ids::Peer::User(id).bot_api_id();
    let mut statement = ctx.db.prepare(
        "SELECT ?1
         UNION SELECT owner_id FROM messages WHERE peer = ?2 OR from_id = ?2
         UNION SELECT b.user_id FROM chat_members a JOIN chat_members b ON a.chat_id = b.chat_id
               WHERE a.user_id = ?1
         UNION SELECT b.user_id FROM channel_members a JOIN channel_members b
               ON a.channel_id = b.channel_id WHERE a.user_id = ?1",
    )?;
    let rows = statement.query_map(rusqlite::params![id.0, peer], |row| Ok(UserId(row.get(0)?)))?;
    Ok(rows.collect::<Result<_, _>>()?)
}

pub fn check_username(ctx: &Ctx<'_>, username: &str) -> RpcResult<()> {
    let name = username.trim_start_matches('@');
    let valid = (5..=32).contains(&name.len())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && name.starts_with(|c: char| c.is_ascii_alphabetic());
    if !valid {
        return Err(RpcError::bad_request("USERNAME_INVALID"));
    }
    if store::accounts::by_username(ctx.db, name)?.is_some()
        || store::channels::by_username(ctx.db, name)?.is_some()
    {
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
