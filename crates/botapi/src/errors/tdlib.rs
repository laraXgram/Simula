//! MTProto error → tdlib error, as tdlib's managers translate them (see
//! `td/telegram/MessagesManager.cpp`, the error handling after sending a message).
//! Errors not listed pass through unchanged, as in tdlib.

use crate::error::ErrorContext;

pub fn translate(code: i32, message: &str, context: ErrorContext) -> (i32, String) {
    let (code, message) = match (code, message) {
        (400, "MESSAGE_TOO_LONG") => (400, "Message is too long"),
        (400, "MEDIA_CAPTION_TOO_LONG") => (400, "Message caption is too long"),
        (400, "INPUT_USER_DEACTIVATED") => (403, "User is deactivated"),
        (400, "USER_IS_BOT") if context.private_chat => {
            (403, "The bot can't send messages to the bot")
        }
        (400, "PEER_ID_INVALID") if context.private_chat => {
            (403, "Bot can't initiate conversation with a user")
        }
        (400, "REPLY_TO_MONOFORUM_PEER_INVALID") => {
            (400, "Wrong direct messages topic identifier specified")
        }
        (400, "CHAT_FORWARDS_RESTRICTED") => {
            (400, "Message has protected content and can't be forwarded")
        }
        (403, "CHAT_WRITE_FORBIDDEN") => (403, "Bot is not a member of the chat"),
        (403, "USER_IS_BLOCKED") => (403, "Bot was blocked by the user"),
        _ => return (code, message.to_owned()),
    };
    (code, message.to_owned())
}
