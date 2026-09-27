//! Callback buttons. Pressing one sends the bot `updateBotCallbackQuery` and makes the
//! presser's call wait for the bot's answer (see [`crate::waits`]); the answer is a
//! toast or alert, or a URL to open.

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::{Peer, UserId};
use crate::store;
use crate::waits::WaitKey;

/// Longest answer text Telegram shows.
const MAX_ANSWER_LENGTH: usize = 200;

/// `presser` presses the callback button carrying `data` on their message `msg_id`.
pub fn press(
    ctx: &mut Ctx<'_>,
    presser: UserId,
    peer: Peer,
    msg_id: i32,
    data: &[u8],
) -> RpcResult<()> {
    let Some((global_id, message)) = store::messages::get_in(ctx.db, presser, peer, msg_id)? else {
        return Err(RpcError::bad_request("MESSAGE_ID_INVALID"));
    };
    if !has_callback_button(message.reply_markup.as_ref(), data) {
        return Err(RpcError::bad_request("DATA_INVALID"));
    }
    let bot = match message.from_id.as_ref().map(Peer::from_tl) {
        Some(Peer::User(bot)) if ctx.account(bot)?.is_bot => bot,
        _ => return Err(RpcError::bad_request("BOT_RESPONSE_TIMEOUT")),
    };
    let Peer::User(_) = peer else {
        return Err(RpcError::not_implemented(
            "callback buttons in groups and channels",
        ));
    };
    // The bot sees the query on its own copy, in its chat with the presser.
    let bot_msg_id = store::messages::copy_id(ctx.db, bot, global_id)?
        .ok_or_else(|| RpcError::bad_request("MESSAGE_ID_INVALID"))?;
    let bot_chat = Peer::User(presser);

    let query_id = loop {
        let id = rand::random::<i64>() & i64::MAX;
        if id != 0 {
            break id;
        }
    };
    let update: enums::Update = types::UpdateBotCallbackQuery {
        query_id,
        user_id: presser.0,
        peer: bot_chat.to_tl(),
        msg_id: bot_msg_id,
        chat_instance: ctx.chat_instance(bot, bot_chat),
        data: Some(data.to_vec()),
        game_short_name: None,
    }
    .into();
    let origin = ctx.origin_for(bot);
    ctx.out.push(bot, update, [presser], origin);
    ctx.defer(
        WaitKey::CallbackQuery { bot, query_id },
        RpcError::bad_request("BOT_RESPONSE_TIMEOUT"),
    );
    Ok(())
}

/// The bot's answer to query `query_id`.
pub struct Answer {
    pub alert: bool,
    pub message: Option<String>,
    pub url: Option<String>,
    pub cache_time: i32,
}

pub fn answer(ctx: &mut Ctx<'_>, bot: UserId, query_id: i64, answer: Answer) -> RpcResult<()> {
    let key = WaitKey::CallbackQuery { bot, query_id };
    if !ctx.is_waiting(key) {
        return Err(RpcError::bad_request("QUERY_ID_INVALID"));
    }
    if answer
        .message
        .as_ref()
        .is_some_and(|m| m.chars().count() > MAX_ANSWER_LENGTH)
    {
        return Err(RpcError::bad_request("MESSAGE_TOO_LONG"));
    }
    if let Some(url) = &answer.url
        && !is_bot_link(url, ctx.account(bot)?.username.as_deref())
    {
        return Err(RpcError::bad_request("URL_INVALID"));
    }
    let reply: enums::messages::BotCallbackAnswer = types::messages::BotCallbackAnswer {
        alert: answer.alert,
        has_url: answer.url.is_some(),
        native_ui: false,
        message: answer.message.filter(|m| !m.is_empty()),
        url: answer.url,
        cache_time: answer.cache_time,
    }
    .into();
    ctx.answer(key, Ok(Box::new(reply)));
    Ok(())
}

fn has_callback_button(markup: Option<&enums::ReplyMarkup>, data: &[u8]) -> bool {
    let Some(enums::ReplyMarkup::ReplyInlineMarkup(inline)) = markup else {
        return false;
    };
    inline
        .rows
        .iter()
        .flat_map(|enums::KeyboardInlineButtonRow::KeyboardInlineButtonRow(row)| &row.buttons)
        .any(|enums::KeyboardInlineButton::KeyboardInlineButton(button)| {
            matches!(&button.r#type, enums::InlineButtonType::Callback(c) if c.data == data)
        })
}

/// Without a game, a callback answer may only open the bot's own `t.me` link
/// (`t.me/<bot>?start=…`).
fn is_bot_link(url: &str, username: Option<&str>) -> bool {
    let Some(username) = username else {
        return false;
    };
    let lower = url.to_ascii_lowercase();
    let rest = ["https://", "http://", ""]
        .iter()
        .find_map(|scheme| lower.strip_prefix(scheme))
        .unwrap_or(&lower);
    let Some(path) = rest
        .strip_prefix("t.me/")
        .or_else(|| rest.strip_prefix("telegram.me/"))
    else {
        return false;
    };
    let name = path.split(['?', '/', '#']).next().unwrap_or("");
    name.eq_ignore_ascii_case(username)
}

#[cfg(test)]
mod tests {
    use super::is_bot_link;

    #[test]
    fn only_the_bots_own_links() {
        assert!(is_bot_link(
            "https://t.me/echo_bot?start=x",
            Some("Echo_Bot")
        ));
        assert!(is_bot_link("t.me/echo_bot", Some("echo_bot")));
        assert!(!is_bot_link(
            "https://t.me/other_bot?start=x",
            Some("echo_bot")
        ));
        assert!(!is_bot_link("https://example.com", Some("echo_bot")));
    }
}
