//! Keyboards attached to messages. Clients send input forms of some buttons (users by
//! `InputUser`, peer requests with the fields to share); messages store the forms every
//! client receives. Only bots can attach keyboards.

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::UserId;
use crate::services::peers;

/// Telegram's limit for inline button callback data.
const MAX_CALLBACK_DATA: usize = 64;

/// The markup as stored, or `None` when the sender can't attach one.
pub fn normalize(
    ctx: &Ctx<'_>,
    sender: UserId,
    markup: Option<enums::ReplyMarkup>,
) -> RpcResult<Option<enums::ReplyMarkup>> {
    let Some(markup) = markup else {
        return Ok(None);
    };
    if !ctx.account(sender)?.is_bot {
        return Ok(None);
    }
    Ok(Some(match markup {
        enums::ReplyMarkup::ReplyInlineMarkup(mut inline) => {
            let mut next_button_id = 1;
            for enums::KeyboardInlineButtonRow::KeyboardInlineButtonRow(row) in &mut inline.rows {
                for enums::KeyboardInlineButton::KeyboardInlineButton(button) in &mut row.buttons {
                    button.r#type = inline_button(
                        ctx,
                        sender,
                        std::mem::take(&mut button.r#type),
                        &mut next_button_id,
                    )?;
                }
            }
            enums::ReplyMarkup::ReplyInlineMarkup(inline)
        }
        enums::ReplyMarkup::ReplyKeyboardMarkup(mut keyboard) => {
            for enums::KeyboardButtonRow::KeyboardButtonRow(row) in &mut keyboard.rows {
                for enums::KeyboardButton::KeyboardButton(button) in &mut row.buttons {
                    if let enums::ButtonType::InputButtonTypeRequestPeer(request) = &button.r#type {
                        button.r#type = types::ButtonTypeRequestPeer {
                            button_id: request.button_id,
                            peer_type: request.peer_type.clone(),
                            max_quantity: request.max_quantity,
                        }
                        .into();
                    }
                }
            }
            enums::ReplyMarkup::ReplyKeyboardMarkup(keyboard)
        }
        other => other,
    }))
}

fn inline_button(
    ctx: &Ctx<'_>,
    sender: UserId,
    button: enums::InlineButtonType,
    next_button_id: &mut i32,
) -> RpcResult<enums::InlineButtonType> {
    use enums::InlineButtonType as T;
    Ok(match button {
        T::Callback(callback) => {
            if callback.data.is_empty() || callback.data.len() > MAX_CALLBACK_DATA {
                return Err(RpcError::bad_request("BUTTON_DATA_INVALID"));
            }
            T::Callback(callback)
        }
        T::InputInlineButtonTypeUserProfile(profile) => {
            let user = peers::resolve_input_user(ctx, sender, &profile.user_id)
                .map_err(|_| RpcError::bad_request("BUTTON_USER_INVALID"))?;
            types::InlineButtonTypeUserProfile { user_id: user.0 }.into()
        }
        T::InputInlineButtonTypeUrlAuth(auth) => {
            let button_id = *next_button_id;
            *next_button_id += 1;
            types::InlineButtonTypeUrlAuth {
                fwd_text: auth.fwd_text,
                url: auth.url,
                button_id,
            }
            .into()
        }
        other => other,
    })
}

/// Users a stored markup refers to (profile buttons), so viewers receive them.
pub fn users(markup: Option<&enums::ReplyMarkup>) -> Vec<UserId> {
    let Some(enums::ReplyMarkup::ReplyInlineMarkup(inline)) = markup else {
        return Vec::new();
    };
    inline
        .rows
        .iter()
        .flat_map(|enums::KeyboardInlineButtonRow::KeyboardInlineButtonRow(row)| &row.buttons)
        .filter_map(
            |enums::KeyboardInlineButton::KeyboardInlineButton(button)| match &button.r#type {
                enums::InlineButtonType::UserProfile(profile) => Some(UserId(profile.user_id)),
                _ => None,
            },
        )
        .collect()
}
