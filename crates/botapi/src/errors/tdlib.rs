//! MTProto error → tdlib error. tdlib rewrites errors of message sends (see
//! `MessagesManager::on_send_message_fail`); everything else passes through unchanged.

use crate::error::ErrorContext;

pub fn translate(code: i32, message: &str, context: ErrorContext) -> (i32, String) {
    let ErrorContext::Send { private_chat } = context else {
        return (code, message.to_owned());
    };
    let (code, message) = match (code, message) {
        (400, "MESSAGE_TOO_LONG") => (400, "Message is too long"),
        (400, "MEDIA_CAPTION_TOO_LONG") => (400, "Message caption is too long"),
        (400, "INPUT_USER_DEACTIVATED") => (403, "User is deactivated"),
        (400, "USER_IS_BOT") if private_chat => (403, "The bot can't send messages to the bot"),
        (400, "PEER_ID_INVALID") if private_chat => {
            (403, "Bot can't initiate conversation with a user")
        }
        (400, "REPLY_TO_MONOFORUM_PEER_INVALID") => {
            (400, "Wrong direct messages topic identifier specified")
        }
        (400, "CHAT_FORWARDS_RESTRICTED") => {
            (400, "Message has protected content and can't be forwarded")
        }
        (400, "MEDIA_EMPTY") => (400, "Wrong file identifier/HTTP URL specified"),
        (400, "PHOTO_EXT_INVALID") => (
            400,
            "Photo has unsupported extension. Use one of .jpg, .jpeg, .gif, .png, .tif or .bmp",
        ),
        (403, "CHAT_WRITE_FORBIDDEN") => (403, "Bot is not a member of the chat"),
        (403, "USER_IS_BLOCKED") => (403, "Bot was blocked by the user"),
        _ => return (code, message.to_owned()),
    };
    (code, message.to_owned())
}
