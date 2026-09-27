//! `reply_markup` → TL, in the real two steps. [`parse`] is telegram-bot-api's
//! `get_reply_markup`: which fields make which kind of button, and its errors. It runs
//! before the chat is resolved. [`Markup::into_tl`] is tdlib's `get_reply_markup` and
//! `get_input_reply_markup`: per-chat rules, row and keyboard limits, link checks.

use simula_botapi_types::types as bot;
use simula_tl::{enums, types};

use crate::error::{BotError, BotResult};
use crate::text::link;

/// tdlib keeps at most this many buttons per row, and per keyboard.
const MAX_ROW_BUTTONS: usize = 12;
const MAX_BUTTONS: usize = 300;

#[derive(Debug)]
pub enum Markup {
    Inline {
        rows: Vec<Vec<InlineButton>>,
        force_reply: bool,
    },
    Keyboard {
        rows: Vec<Vec<Button>>,
        persistent: bool,
        resize: bool,
        one_time: bool,
        personal: bool,
        force_reply: bool,
        placeholder: String,
    },
    Remove {
        personal: bool,
    },
    ForceReply {
        personal: bool,
        placeholder: String,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum Style {
    #[default]
    Default,
    Primary,
    Danger,
    Success,
}

#[derive(Debug)]
pub struct InlineButton {
    text: String,
    icon: i64,
    style: Style,
    kind: InlineKind,
}

#[derive(Debug)]
enum InlineKind {
    Url(String),
    Callback(String),
    Game,
    Buy,
    /// `[users, bots, groups, channels]` the query may be sent to.
    SwitchInline {
        query: String,
        chats: [bool; 4],
    },
    SwitchInlineCurrent(String),
    LoginUrl {
        url: String,
        request_write_access: bool,
        forward_text: String,
    },
    WebApp(String),
    CopyText(String),
    Disabled,
}

#[derive(Debug)]
pub struct Button {
    text: String,
    icon: i64,
    style: Style,
    kind: ButtonKind,
}

#[derive(Debug)]
enum ButtonKind {
    Text,
    RequestPhone,
    RequestLocation,
    RequestPoll { quiz: Option<bool> },
    WebApp(String),
    RequestUsers(bot::KeyboardButtonRequestUsers),
    RequestChat(bot::KeyboardButtonRequestChat),
    RequestManagedBot(bot::KeyboardButtonRequestManagedBot),
}

fn error(message: impl std::fmt::Display) -> BotError {
    BotError::bad_request(lower_first(&message.to_string()))
}

/// telegram-bot-api's `fail_query_with_error` casing for its own messages.
fn lower_first(message: &str) -> String {
    let mut chars = message.chars();
    chars
        .next()
        .map(|first| first.to_lowercase().chain(chars).collect())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// telegram-bot-api

pub fn parse(markup: Option<&bot::ReplyMarkup>) -> BotResult<Option<Markup>> {
    let Some(markup) = markup else {
        return Ok(None);
    };
    Ok(match markup {
        bot::ReplyMarkup::ReplyKeyboardMarkup(k) => {
            let personal = k.selective == Some(true);
            if !k.keyboard.is_empty() {
                let rows = k
                    .keyboard
                    .iter()
                    .map(|row| row.iter().map(keyboard_button).collect())
                    .collect::<BotResult<_>>()?;
                Some(Markup::Keyboard {
                    rows,
                    persistent: k.is_persistent == Some(true),
                    resize: k.resize_keyboard == Some(true),
                    one_time: k.one_time_keyboard == Some(true),
                    personal,
                    force_reply: k.force_reply == Some(true),
                    placeholder: k.input_field_placeholder.clone().unwrap_or_default(),
                })
            } else if k.force_reply == Some(true) {
                Some(Markup::ForceReply {
                    personal,
                    placeholder: k.input_field_placeholder.clone().unwrap_or_default(),
                })
            } else {
                None
            }
        }
        bot::ReplyMarkup::InlineKeyboardMarkup(k) => {
            if !k.inline_keyboard.is_empty() {
                let rows = k
                    .inline_keyboard
                    .iter()
                    .map(|row| row.iter().map(inline_button).collect())
                    .collect::<BotResult<_>>()?;
                Some(Markup::Inline {
                    rows,
                    force_reply: k.force_reply == Some(true),
                })
            } else if k.force_reply == Some(true) {
                Some(Markup::ForceReply {
                    personal: false,
                    placeholder: String::new(),
                })
            } else {
                None
            }
        }
        bot::ReplyMarkup::ReplyKeyboardRemove(r) => r.remove_keyboard.then(|| Markup::Remove {
            personal: r.selective == Some(true),
        }),
        bot::ReplyMarkup::ForceReply(f) => f.force_reply.then(|| Markup::ForceReply {
            personal: f.selective == Some(true),
            placeholder: f.input_field_placeholder.clone().unwrap_or_default(),
        }),
    })
}

/// telegram-bot-api's `get_button_style` (case-insensitive; "" and "default" allowed).
fn style(style: Option<&str>) -> BotResult<Style> {
    Ok(match style.unwrap_or("").to_ascii_lowercase().as_str() {
        "" | "default" => Style::Default,
        "primary" => Style::Primary,
        "danger" => Style::Danger,
        "success" => Style::Success,
        _ => return Err(error("Invalid button style specified")),
    })
}

fn icon(icon: Option<&str>) -> BotResult<i64> {
    icon.map_or(Ok(0), |id| {
        id.parse()
            .map_err(|_| error("Field \"icon_custom_emoji_id\" must be a valid Number"))
    })
}

fn inline_button(b: &bot::InlineKeyboardButton) -> BotResult<InlineButton> {
    let non_empty = |s: &Option<String>| s.clone().filter(|s| !s.is_empty());
    let kind = if let Some(url) = non_empty(&b.url) {
        InlineKind::Url(url)
    } else if let Some(data) = non_empty(&b.callback_data) {
        InlineKind::Callback(data)
    } else if b.callback_game.is_some() {
        InlineKind::Game
    } else if b.pay.is_some() {
        InlineKind::Buy
    } else if let Some(query) = &b.switch_inline_query {
        InlineKind::SwitchInline {
            query: query.clone(),
            chats: [true; 4],
        }
    } else if let Some(chosen) = &b.switch_inline_query_chosen_chat {
        InlineKind::SwitchInline {
            query: chosen.query.clone().unwrap_or_default(),
            chats: [
                chosen.allow_user_chats == Some(true),
                chosen.allow_bot_chats == Some(true),
                chosen.allow_group_chats == Some(true),
                chosen.allow_channel_chats == Some(true),
            ],
        }
    } else if let Some(query) = &b.switch_inline_query_current_chat {
        InlineKind::SwitchInlineCurrent(query.clone())
    } else if let Some(login) = &b.login_url {
        let bot_username = login.bot_username.as_deref().unwrap_or("");
        let bot_username = bot_username.strip_prefix('@').unwrap_or(bot_username);
        let has_bot = login
            .bot_username
            .as_deref()
            .is_some_and(|name| !name.is_empty());
        if has_bot
            && (bot_username.is_empty()
                || !bot_username
                    .chars()
                    .all(|c| c == '_' || c.is_ascii_alphanumeric()))
        {
            return Err(error("LoginUrl bot username is invalid"));
        }
        if has_bot {
            return Err(BotError::not_implemented("login_url buttons of other bots"));
        }
        InlineKind::LoginUrl {
            url: login.url.clone(),
            request_write_access: login.request_write_access == Some(true),
            forward_text: login.forward_text.clone().unwrap_or_default(),
        }
    } else if let Some(web_app) = &b.web_app {
        InlineKind::WebApp(web_app.url.clone())
    } else if let Some(copy) = &b.copy_text {
        InlineKind::CopyText(copy.text.clone())
    } else if b.disabled.is_some() {
        InlineKind::Disabled
    } else {
        return Err(error("Text buttons are not allowed in the inline keyboard"));
    };
    Ok(InlineButton {
        text: b.text.clone(),
        icon: icon(b.icon_custom_emoji_id.as_deref())?,
        style: style(b.style.as_deref())?,
        kind,
    })
}

fn keyboard_button(b: &bot::KeyboardButton) -> BotResult<Button> {
    let kind = if b.request_contact == Some(true) {
        ButtonKind::RequestPhone
    } else if b.request_location == Some(true) {
        ButtonKind::RequestLocation
    } else if let Some(poll) = &b.request_poll {
        ButtonKind::RequestPoll {
            quiz: match poll.r#type.as_deref() {
                Some("quiz") => Some(true),
                Some("regular") => Some(false),
                _ => None,
            },
        }
    } else if let Some(web_app) = &b.web_app {
        ButtonKind::WebApp(web_app.url.clone())
    } else if let Some(users) = &b.request_users {
        ButtonKind::RequestUsers((**users).clone())
    } else if let Some(chat) = &b.request_chat {
        ButtonKind::RequestChat((**chat).clone())
    } else if let Some(managed) = &b.request_managed_bot {
        ButtonKind::RequestManagedBot((**managed).clone())
    } else {
        ButtonKind::Text
    };
    Ok(Button {
        text: b.text.clone(),
        icon: icon(b.icon_custom_emoji_id.as_deref())?,
        style: style(b.style.as_deref())?,
        kind,
    })
}

// ---------------------------------------------------------------------------
// tdlib

/// Where the markup goes, as far as tdlib's rules care.
#[derive(Debug, Clone, Copy)]
pub struct Target {
    /// Request buttons (phone, location, users, …) only work in private chats, and
    /// keyboards are only "personal" outside them.
    pub private_chat: bool,
}

impl Markup {
    pub fn into_tl(self, target: Target) -> BotResult<Option<enums::ReplyMarkup>> {
        let personal_allowed = !target.private_chat;
        Ok(Some(match self {
            Markup::Inline { rows, force_reply } => {
                let rows = limit(rows, |b| b.text.is_empty() && b.icon == 0)
                    .into_iter()
                    .map(|row| {
                        let buttons = row
                            .into_iter()
                            .map(inline_to_tl)
                            .collect::<BotResult<Vec<_>>>()?;
                        Ok(types::KeyboardInlineButtonRow { buttons }.into())
                    })
                    .collect::<BotResult<Vec<_>>>()?;
                if rows.is_empty() {
                    return Ok(None);
                }
                types::ReplyInlineMarkup { force_reply, rows }.into()
            }
            Markup::Keyboard {
                rows,
                persistent,
                resize,
                one_time,
                personal,
                force_reply,
                placeholder,
            } => {
                let rows = limit(rows, |b| b.text.is_empty() && b.icon == 0)
                    .into_iter()
                    .map(|row| {
                        let buttons = row
                            .into_iter()
                            .map(|b| keyboard_to_tl(b, target))
                            .collect::<BotResult<Vec<_>>>()?;
                        Ok(types::KeyboardButtonRow { buttons }.into())
                    })
                    .collect::<BotResult<Vec<_>>>()?;
                if rows.is_empty() {
                    return Ok(None);
                }
                types::ReplyKeyboardMarkup {
                    resize,
                    single_use: one_time,
                    selective: personal && personal_allowed,
                    persistent,
                    force_reply,
                    rows,
                    placeholder: (!placeholder.is_empty()).then_some(placeholder),
                }
                .into()
            }
            Markup::Remove { personal } => types::ReplyKeyboardHide {
                selective: personal && personal_allowed,
            }
            .into(),
            Markup::ForceReply {
                personal,
                placeholder,
            } => types::ReplyKeyboardForceReply {
                single_use: false,
                selective: personal && personal_allowed,
                placeholder: (!placeholder.is_empty()).then_some(placeholder),
            }
            .into(),
        }))
    }
}

/// tdlib skips empty buttons and keeps at most 12 per row and 300 in all.
fn limit<B>(rows: Vec<Vec<B>>, is_empty: impl Fn(&B) -> bool) -> Vec<Vec<B>> {
    let mut total = 0;
    let mut out = Vec::new();
    for row in rows {
        let mut kept = Vec::new();
        for button in row {
            if is_empty(&button) {
                continue;
            }
            kept.push(button);
            total += 1;
            if kept.len() >= MAX_ROW_BUTTONS || total >= MAX_BUTTONS {
                break;
            }
        }
        if !kept.is_empty() {
            out.push(kept);
        }
        if total >= MAX_BUTTONS {
            break;
        }
    }
    out
}

fn style_to_tl(style: Style, icon: i64) -> Option<enums::KeyboardButtonStyle> {
    if style == Style::Default && icon == 0 {
        return None;
    }
    Some(
        types::KeyboardButtonStyle {
            bg_primary: style == Style::Primary,
            bg_danger: style == Style::Danger,
            bg_success: style == Style::Success,
            icon: (icon != 0).then_some(icon),
        }
        .into(),
    )
}

fn inline_to_tl(b: InlineButton) -> BotResult<enums::KeyboardInlineButton> {
    if b.text.is_empty() {
        return Err(error("Inline keyboard button text must be non-empty"));
    }
    use enums::InlineButtonType as T;
    let r#type: T = match b.kind {
        InlineKind::Url(url) => match link::user_id(&url) {
            Some(user_id) => types::InputInlineButtonTypeUserProfile {
                user_id: input_user(user_id),
            }
            .into(),
            None => {
                let url = link::check(&url, false, false)
                    .map_err(|e| error(format!("Inline keyboard button {e}")))?;
                types::InlineButtonTypeUrl { url }.into()
            }
        },
        InlineKind::Callback(data) => types::InlineButtonTypeCallback {
            requires_password: false,
            data: data.into_bytes(),
        }
        .into(),
        InlineKind::Game => T::Game,
        InlineKind::Buy => T::Buy,
        InlineKind::SwitchInline { query, chats } => {
            if chats == [false; 4] {
                return Err(error("At least one chat type must be allowed"));
            }
            types::InlineButtonTypeSwitchInline {
                same_peer: false,
                query,
                peer_types: peer_types(chats),
            }
            .into()
        }
        InlineKind::SwitchInlineCurrent(query) => types::InlineButtonTypeSwitchInline {
            same_peer: true,
            query,
            peer_types: None,
        }
        .into(),
        InlineKind::LoginUrl {
            url,
            request_write_access,
            forward_text,
        } => {
            if link::user_id(&url).is_some() {
                return Err(error("Link to a user can't be used in login URL buttons"));
            }
            let url = link::check(&url, true, true)
                .map_err(|e| error(format!("Inline keyboard button login {e}")))?;
            types::InputInlineButtonTypeUrlAuth {
                request_write_access,
                fwd_text: (!forward_text.is_empty()).then_some(forward_text),
                url,
                bot: None,
            }
            .into()
        }
        InlineKind::WebApp(url) => {
            if link::user_id(&url).is_some() {
                return Err(error("Link to a user can't be used in Web App URL buttons"));
            }
            let url = link::check(&url, true, true)
                .map_err(|e| error(format!("Inline keyboard button Web App {e}")))?;
            types::InlineButtonTypeWebView { url }.into()
        }
        InlineKind::CopyText(copy_text) => types::InlineButtonTypeCopy { copy_text }.into(),
        InlineKind::Disabled => T::Disabled,
    };
    Ok(types::KeyboardInlineButton {
        style: style_to_tl(b.style, b.icon),
        text: b.text,
        r#type,
    }
    .into())
}

/// tdlib's `TargetDialogTypes::get_input_peer_types`: no list when every kind is allowed.
fn peer_types(chats: [bool; 4]) -> Option<Vec<enums::InlineQueryPeerType>> {
    use enums::InlineQueryPeerType as P;
    if chats == [true; 4] {
        return None;
    }
    let [users, bots, groups, channels] = chats;
    let mut types = Vec::new();
    if users {
        types.push(P::PM);
    }
    if bots {
        types.push(P::BotPM);
    }
    if groups {
        types.extend([P::Chat, P::Megagroup]);
    }
    if channels {
        types.push(P::Broadcast);
    }
    Some(types)
}

fn keyboard_to_tl(b: Button, target: Target) -> BotResult<enums::KeyboardButton> {
    if b.text.is_empty() {
        return Err(error("Keyboard button text must be non-empty"));
    }
    let private_only = |what: &str| -> BotResult<()> {
        if target.private_chat {
            Ok(())
        } else {
            Err(error(what))
        }
    };
    use enums::ButtonType as T;
    let r#type: T = match b.kind {
        ButtonKind::Text => T::Default,
        ButtonKind::RequestPhone => {
            private_only("Phone number can be requested in private chats only")?;
            T::RequestPhone
        }
        ButtonKind::RequestLocation => {
            private_only("Location can be requested in private chats only")?;
            T::RequestGeoLocation
        }
        ButtonKind::RequestPoll { quiz } => {
            private_only("Poll can be requested in private chats only")?;
            types::ButtonTypeRequestPoll { quiz }.into()
        }
        ButtonKind::WebApp(url) => {
            private_only("Web App buttons can be used in private chats only")?;
            if link::user_id(&url).is_some() {
                return Err(error("Link to a user can't be used in Web App URL buttons"));
            }
            link::check(&url, true, true)
                .map_err(|e| error(format!("Keyboard button Web App {e}")))?;
            types::ButtonTypeSimpleWebView { url }.into()
        }
        ButtonKind::RequestUsers(r) => {
            private_only("Users can be requested in private chats only")?;
            types::InputButtonTypeRequestPeer {
                name_requested: r.request_name == Some(true),
                username_requested: r.request_username == Some(true),
                photo_requested: r.request_photo == Some(true),
                button_id: r.request_id as i32,
                peer_type: types::RequestPeerTypeUser {
                    bot: r.user_is_bot,
                    premium: r.user_is_premium,
                }
                .into(),
                max_quantity: r.max_quantity.unwrap_or(1).max(1) as i32,
            }
            .into()
        }
        ButtonKind::RequestChat(r) => {
            private_only("Chats can be requested in private chats only")?;
            let channel = r.chat_is_channel;
            let creator = r.chat_is_created == Some(true);
            let rights = |rights: &Option<Box<bot::ChatAdministratorRights>>| {
                rights
                    .as_deref()
                    .map(|rights| admin_rights(rights, channel))
            };
            // tdlib drops the user's rights requirement when the user must be the creator.
            let user_admin_rights = rights(&r.user_administrator_rights).filter(|_| !creator);
            let bot_admin_rights = rights(&r.bot_administrator_rights);
            let peer_type: enums::RequestPeerType = if channel {
                types::RequestPeerTypeBroadcast {
                    creator,
                    has_username: r.chat_has_username,
                    user_admin_rights,
                    bot_admin_rights,
                }
                .into()
            } else {
                types::RequestPeerTypeChat {
                    creator,
                    bot_participant: r.bot_is_member == Some(true),
                    has_username: r.chat_has_username,
                    forum: r.chat_is_forum,
                    user_admin_rights,
                    bot_admin_rights,
                }
                .into()
            };
            types::InputButtonTypeRequestPeer {
                name_requested: r.request_title == Some(true),
                username_requested: r.request_username == Some(true),
                photo_requested: r.request_photo == Some(true),
                button_id: r.request_id as i32,
                peer_type,
                max_quantity: 1,
            }
            .into()
        }
        ButtonKind::RequestManagedBot(r) => {
            private_only("Managed bots can be requested in private chats only")?;
            let name = r.suggested_name.filter(|s| !s.is_empty());
            let username = r.suggested_username.filter(|s| !s.is_empty());
            types::InputButtonTypeRequestPeer {
                name_requested: false,
                username_requested: false,
                photo_requested: false,
                button_id: r.request_id as i32,
                peer_type: types::RequestPeerTypeCreateBot {
                    bot_managed: true,
                    suggested_name: name,
                    suggested_username: username,
                }
                .into(),
                max_quantity: 1,
            }
            .into()
        }
    };
    Ok(types::KeyboardButton {
        style: style_to_tl(b.style, b.icon),
        text: b.text,
        r#type,
    }
    .into())
}

/// tdlib's `AdministratorRights` for a requested chat: rights that don't apply to the
/// chat kind are dropped, and any right implies "manage chat".
fn admin_rights(r: &bot::ChatAdministratorRights, channel: bool) -> enums::ChatAdminRights {
    let yes = |v: Option<bool>| v == Some(true);
    let mut rights = types::ChatAdminRights {
        change_info: r.can_change_info,
        post_messages: channel && yes(r.can_post_messages),
        edit_messages: channel && yes(r.can_edit_messages),
        delete_messages: r.can_delete_messages,
        ban_users: r.can_restrict_members,
        invite_users: r.can_invite_users,
        pin_messages: !channel && yes(r.can_pin_messages),
        add_admins: r.can_promote_members,
        anonymous: !channel && r.is_anonymous,
        manage_call: r.can_manage_video_chats,
        other: r.can_manage_chat,
        manage_topics: !channel && yes(r.can_manage_topics),
        post_stories: r.can_post_stories,
        edit_stories: r.can_edit_stories,
        delete_stories: r.can_delete_stories,
        manage_direct_messages: channel && yes(r.can_manage_direct_messages),
        manage_ranks: !channel && yes(r.can_manage_tags),
        manage_linked_peers: false,
        manage_welcome_messages: r.can_send_welcome_messages,
    };
    let any = serde_json::to_value(&rights)
        .ok()
        .and_then(|v| v.as_object().map(|o| o.values().any(|v| v == true)))
        .unwrap_or(false);
    rights.other |= any;
    rights.into()
}

/// A user by id, as tdlib addresses users it has no access hash for (bots may).
fn input_user(user_id: i64) -> enums::InputUser {
    types::InputUser {
        user_id,
        access_hash: 0,
    }
    .into()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn tl(markup: serde_json::Value, private_chat: bool) -> BotResult<Option<enums::ReplyMarkup>> {
        let markup: bot::ReplyMarkup = serde_json::from_value(markup).unwrap();
        parse(Some(&markup))?.map_or(Ok(None), |m| m.into_tl(Target { private_chat }))
    }

    fn err(markup: serde_json::Value) -> String {
        tl(markup, true).unwrap_err().description
    }

    #[test]
    fn inline_buttons_follow_telegram_bot_api_priorities() {
        let markup = tl(
            json!({ "inline_keyboard": [[
                { "text": "a", "url": "", "callback_data": "x" },
                { "text": "b", "pay": false },
                { "text": "c", "url": "Example.com" },
                { "text": "d", "url": "tg://user?id=42" }
            ]] }),
            true,
        )
        .unwrap()
        .unwrap();
        let enums::ReplyMarkup::ReplyInlineMarkup(inline) = markup else {
            panic!("inline markup expected");
        };
        let enums::KeyboardInlineButtonRow::KeyboardInlineButtonRow(row) = &inline.rows[0];
        let kinds: Vec<_> = row
            .buttons
            .iter()
            .map(|enums::KeyboardInlineButton::KeyboardInlineButton(b)| b.r#type.clone())
            .collect();
        assert!(matches!(&kinds[0], enums::InlineButtonType::Callback(c) if c.data == b"x"));
        assert!(matches!(kinds[1], enums::InlineButtonType::Buy));
        assert!(
            matches!(&kinds[2], enums::InlineButtonType::Url(u) if u.url == "http://example.com/")
        );
        assert!(matches!(
            kinds[3],
            enums::InlineButtonType::InputInlineButtonTypeUserProfile(_)
        ));
    }

    #[test]
    fn errors_use_telegram_texts() {
        assert_eq!(
            err(json!({ "inline_keyboard": [[{ "text": "plain" }]] })),
            "Bad Request: text buttons are not allowed in the inline keyboard"
        );
        assert_eq!(
            err(json!({ "inline_keyboard": [[{ "text": "a", "url": "nowhere" }]] })),
            "Bad Request: inline keyboard button URL 'nowhere' is invalid: Wrong HTTP URL"
        );
        assert_eq!(
            err(
                json!({ "inline_keyboard": [[{ "text": "a", "web_app": { "url": "http://a.com" } }]] })
            ),
            "Bad Request: inline keyboard button Web App URL 'http://a.com' is invalid: Only HTTPS links are allowed"
        );
        assert_eq!(
            err(
                json!({ "inline_keyboard": [[{ "text": "a", "callback_data": "x", "style": "pink" }]] })
            ),
            "Bad Request: invalid button style specified"
        );
        let group = tl(
            json!({ "keyboard": [[{ "text": "phone", "request_contact": true }]] }),
            false,
        );
        assert_eq!(
            group.unwrap_err().description,
            "Bad Request: phone number can be requested in private chats only"
        );
    }

    #[test]
    fn empty_buttons_are_skipped_and_rows_capped() {
        let row: Vec<_> = (0..20).map(|i| json!(i.to_string())).collect();
        let markup = tl(json!({ "keyboard": [[""], row] }), true)
            .unwrap()
            .unwrap();
        let enums::ReplyMarkup::ReplyKeyboardMarkup(keyboard) = markup else {
            panic!("reply keyboard expected");
        };
        assert_eq!(keyboard.rows.len(), 1, "the empty-only row disappears");
        let enums::KeyboardButtonRow::KeyboardButtonRow(row) = &keyboard.rows[0];
        assert_eq!(row.buttons.len(), MAX_ROW_BUTTONS);
        assert_eq!(tl(json!({ "inline_keyboard": [] }), true).unwrap(), None);
    }

    #[test]
    fn personal_keyboards_only_outside_private_chats() {
        let remove = json!({ "remove_keyboard": true, "selective": true });
        let selective = |markup| match markup {
            Some(enums::ReplyMarkup::ReplyKeyboardHide(hide)) => hide.selective,
            other => panic!("unexpected {other:?}"),
        };
        assert!(!selective(tl(remove.clone(), true).unwrap()));
        assert!(selective(tl(remove, false).unwrap()));
    }
}
