//! tdlib error → Bot API description, following telegram-bot-api's
//! `Client::fail_query_with_error`: a few messages are rewritten, codes are normalized,
//! then the description gets a prefix ("Bad Request: …") and a lowercase first letter
//! unless it is an `UPPER_SNAKE` server code.

use crate::error::BotError;

pub fn format(code: i32, message: &str) -> BotError {
    if code == 429 {
        let retry = message.strip_prefix("Too Many Requests: retry after ").and_then(|s| s.parse().ok());
        let mut error = BotError::new(429, format!("Too Many Requests: retry after {}", retry.unwrap_or(1)));
        error.retry_after = Some(retry.unwrap_or(1));
        return error;
    }

    let mut code = code;
    // Informational and "not found" codes become Bad Request.
    if code < 400 || code == 404 {
        code = 400;
    }
    // A 403 carrying a raw server code (e.g. CHAT_ADMIN_REQUIRED) is reported as 400.
    if code == 403 && message.chars().all(|c| c == '_' || c.is_ascii_uppercase() || c.is_ascii_digit()) {
        code = 400;
    }

    let mut message = message.to_owned();
    if code == 400
        && let Some((new_code, rewritten)) = rewrite(&message)
    {
        code = new_code;
        message = rewritten.to_owned();
    }

    let prefix = match code {
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        500 => "Internal Server Error",
        _ => return BotError::new(400, format!("Bad Request: {message}")),
    };
    if message.starts_with(prefix) {
        return BotError::new(code as u16, message);
    }
    BotError::new(code as u16, format!("{prefix}: {}", lower_first(&message)))
}

/// Server codes telegram-bot-api replaces with readable messages.
fn rewrite(message: &str) -> Option<(i32, &'static str)> {
    Some(match message {
        "MESSAGE_NOT_MODIFIED" => (
            400,
            "message is not modified: specified new message content and reply markup are exactly the same as a current content and reply markup of the message",
        ),
        "WC_CONVERT_URL_INVALID" | "EXTERNAL_URL_INVALID" => (400, "Wrong HTTP URL specified"),
        "WEBPAGE_CURL_FAILED" => (400, "Failed to get HTTP URL content"),
        "WEBPAGE_MEDIA_EMPTY" => (400, "Wrong type of the web page content"),
        "MEDIA_GROUPED_INVALID" => (400, "Can't use the media of the specified type in the album"),
        "REPLY_MARKUP_TOO_LONG" => (400, "reply markup is too long"),
        "INPUT_USER_DEACTIVATED" => (403, "Forbidden: user is deactivated"),
        "USER_IS_BLOCKED" => (403, "bot was blocked by the user"),
        "USER_ADMIN_INVALID" => (400, "user is an administrator of the chat"),
        "CHAT_ABOUT_NOT_MODIFIED" => (400, "chat description is not modified"),
        "PACK_SHORT_NAME_INVALID" => (400, "invalid sticker set name is specified"),
        "PACK_SHORT_NAME_OCCUPIED" => (400, "sticker set name is already occupied"),
        "STICKER_EMOJI_INVALID" => (400, "invalid sticker emojis"),
        "QUERY_ID_INVALID" => (400, "query is too old and response timeout expired or query ID is invalid"),
        "MESSAGE_DELETE_FORBIDDEN" => (400, "message can't be deleted"),
        _ => return None,
    })
}

/// Lowercase the first letter, except for `UPPER_SNAKE` codes (second char `_` or uppercase).
fn lower_first(message: &str) -> String {
    let mut chars = message.chars();
    let (Some(first), second) = (chars.next(), message.chars().nth(1)) else { return message.to_owned() };
    if second.is_some_and(|c| c == '_' || c.is_ascii_uppercase()) {
        return message.to_owned();
    }
    first.to_lowercase().chain(chars).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorContext;
    use crate::errors::tdlib;

    fn translate(code: i32, message: &str, private_chat: bool) -> BotError {
        let (code, message) = tdlib::translate(code, message, ErrorContext { private_chat });
        format(code, &message)
    }

    #[test]
    fn matches_real_bot_api_descriptions() {
        let cases = [
            (400, "PEER_ID_INVALID", true, 403, "Forbidden: bot can't initiate conversation with a user"),
            (400, "PEER_ID_INVALID", false, 400, "Bad Request: PEER_ID_INVALID"),
            (400, "USER_IS_BOT", true, 403, "Forbidden: the bot can't send messages to the bot"),
            (400, "MESSAGE_TOO_LONG", true, 400, "Bad Request: message is too long"),
            (403, "USER_IS_BLOCKED", true, 403, "Forbidden: bot was blocked by the user"),
            (403, "CHAT_ADMIN_REQUIRED", false, 400, "Bad Request: CHAT_ADMIN_REQUIRED"),
            (400, "MESSAGE_NOT_MODIFIED", false, 400, "Bad Request: message is not modified: specified new message content and reply markup are exactly the same as a current content and reply markup of the message"),
        ];
        for (code, message, private_chat, want_code, want) in cases {
            let error = translate(code, message, private_chat);
            assert_eq!((error.code, error.description.as_str()), (want_code, want), "{message}");
        }
    }
}
