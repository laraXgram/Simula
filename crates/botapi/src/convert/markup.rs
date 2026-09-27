//! TL `ReplyMarkup` → Bot API `InlineKeyboardMarkup`. Bots only ever see inline keyboards
//! on messages (telegram-bot-api's `JsonReplyMarkup`); tdlib drops buttons with neither
//! text nor icon, and rows or keyboards left empty.

use simula_botapi_types::types as bot;
use simula_tl::enums;

pub fn inline_keyboard(markup: Option<&enums::ReplyMarkup>) -> Option<bot::InlineKeyboardMarkup> {
    let Some(enums::ReplyMarkup::ReplyInlineMarkup(inline)) = markup else {
        return None;
    };
    let rows: Vec<Vec<bot::InlineKeyboardButton>> = inline
        .rows
        .iter()
        .map(
            |enums::KeyboardInlineButtonRow::KeyboardInlineButtonRow(row)| {
                row.buttons.iter().filter_map(button).collect::<Vec<_>>()
            },
        )
        .filter(|row| !row.is_empty())
        .collect();
    (!rows.is_empty()).then(|| bot::InlineKeyboardMarkup {
        inline_keyboard: rows,
        force_reply: inline.force_reply.then_some(true),
    })
}

fn button(button: &enums::KeyboardInlineButton) -> Option<bot::InlineKeyboardButton> {
    let enums::KeyboardInlineButton::KeyboardInlineButton(button) = button;
    let (style, icon) = style(button.style.as_ref());
    if button.text.is_empty() && icon.is_none() {
        return None;
    }
    let base = bot::InlineKeyboardButton {
        text: button.text.clone(),
        icon_custom_emoji_id: icon,
        style,
        ..Default::default()
    };
    use enums::InlineButtonType as T;
    Some(match &button.r#type {
        T::Url(url) => bot::InlineKeyboardButton {
            url: Some(url.url.clone()),
            ..base
        },
        // Login buttons show up as plain URL buttons.
        T::UrlAuth(auth) => bot::InlineKeyboardButton {
            url: Some(auth.url.clone()),
            ..base
        },
        T::Callback(callback) => bot::InlineKeyboardButton {
            callback_data: Some(
                String::from_utf8(callback.data.clone()).unwrap_or_else(|_| "INVALID".into()),
            ),
            ..base
        },
        T::Game => bot::InlineKeyboardButton {
            callback_game: Some(Box::default()),
            ..base
        },
        T::Buy => bot::InlineKeyboardButton {
            pay: Some(true),
            ..base
        },
        T::SwitchInline(switch) if switch.same_peer => bot::InlineKeyboardButton {
            switch_inline_query_current_chat: Some(switch.query.clone()),
            ..base
        },
        T::SwitchInline(switch) => match chosen_chats(switch.peer_types.as_deref()) {
            None => bot::InlineKeyboardButton {
                switch_inline_query: Some(switch.query.clone()),
                ..base
            },
            Some([users, bots, groups, channels]) => bot::InlineKeyboardButton {
                switch_inline_query_chosen_chat: Some(Box::new(bot::SwitchInlineQueryChosenChat {
                    query: Some(switch.query.clone()),
                    allow_user_chats: Some(users),
                    allow_bot_chats: Some(bots),
                    allow_group_chats: Some(groups),
                    allow_channel_chats: Some(channels),
                })),
                ..base
            },
        },
        T::UserProfile(profile) => bot::InlineKeyboardButton {
            url: Some(format!("tg://user?id={}", profile.user_id)),
            ..base
        },
        T::WebView(web_app) => bot::InlineKeyboardButton {
            web_app: Some(Box::new(bot::WebAppInfo {
                url: web_app.url.clone(),
            })),
            ..base
        },
        T::Copy(copy) => bot::InlineKeyboardButton {
            copy_text: Some(Box::new(bot::CopyTextButton {
                text: copy.copy_text.clone(),
            })),
            ..base
        },
        T::Disabled => bot::InlineKeyboardButton {
            disabled: Some(Box::default()),
            ..base
        },
        // Input forms never reach stored messages.
        T::InputInlineButtonTypeUrlAuth(_) | T::InputInlineButtonTypeUserProfile(_) => {
            return None;
        }
    })
}

/// `[users, bots, groups, channels]`, or `None` when every kind is allowed (tdlib's
/// `TargetDialogTypes`; an empty list means all).
fn chosen_chats(peer_types: Option<&[enums::InlineQueryPeerType]>) -> Option<[bool; 4]> {
    use enums::InlineQueryPeerType as P;
    let types = peer_types.filter(|types| !types.is_empty())?;
    let mut allowed = [false; 4];
    for peer_type in types {
        match peer_type {
            P::PM => allowed[0] = true,
            P::BotPM => allowed[1] = true,
            P::Chat | P::Megagroup => allowed[2] = true,
            P::Broadcast => allowed[3] = true,
            P::SameBotPM => {}
        }
    }
    (allowed != [true; 4]).then_some(allowed)
}

/// `(style, icon_custom_emoji_id)` of a button.
fn style(style: Option<&enums::KeyboardButtonStyle>) -> (Option<String>, Option<String>) {
    let Some(enums::KeyboardButtonStyle::KeyboardButtonStyle(style)) = style else {
        return (None, None);
    };
    let name = if style.bg_primary {
        Some("primary")
    } else if style.bg_danger {
        Some("danger")
    } else if style.bg_success {
        Some("success")
    } else {
        None
    };
    let icon = style.icon.filter(|&id| id != 0).map(|id| id.to_string());
    (name.map(Into::into), icon)
}
