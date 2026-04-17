use actix_web::web::Data;
use chrono::Utc;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use std::collections::HashMap;

use crate::database::AppState;
use crate::types::{ApiError, ApiResult};

pub mod api;
pub mod client;
pub mod utils;

use client::types::users::SimUserRecord;
use api::*;

pub fn dispatch_method(
    state: &Data<AppState>,
    token: &str,
    method: &str,
    params: HashMap<String, Value>,
) -> ApiResult {
    match method.to_ascii_lowercase().as_str() {
        "getme" => bot::handle_get_me(state, token, &params),
        "sendmessage" => messages::handle_send_message(state, token, &params),
        "forwardmessage" => messages::handle_forward_message(state, token, &params),
        "forwardmessages" => messages::handle_forward_messages(state, token, &params),
        "copymessage" => messages::handle_copy_message(state, token, &params),
        "copymessages" => messages::handle_copy_messages(state, token, &params),
        "sendphoto" => messages::handle_send_photo(state, token, &params),
        "sendaudio" => messages::handle_send_audio(state, token, &params),
        "senddocument" => messages::handle_send_document(state, token, &params),
        "sendvideo" => messages::handle_send_video(state, token, &params),
        "sendvoice" => messages::handle_send_voice(state, token, &params),
        "sendcontact" => messages::handle_send_contact(state, token, &params),
        "sendlocation" => messages::handle_send_location(state, token, &params),
        "sendvenue" => messages::handle_send_venue(state, token, &params),
        "sendchataction" => messages::handle_send_chat_action(state, token, &params),
        "senddice" => messages::handle_send_dice(state, token, &params),
        "sendgame" => messages::handle_send_game(state, token, &params),
        "sendanimation" => messages::handle_send_animation(state, token, &params),
        "sendvideonote" => messages::handle_send_video_note(state, token, &params),
        "sendsticker" => messages::handle_send_sticker(state, token, &params),
        "sendpoll" => messages::handle_send_poll(state, token, &params),
        "sendchecklist" => messages::handle_send_checklist(state, token, &params),
        "sendinvoice" => messages::handle_send_invoice(state, token, &params),
        "sendpaidmedia" => messages::handle_send_paid_media(state, token, &params),
        "sendmediagroup" => messages::handle_send_media_group(state, token, &params),
        "sendmessagedraft" => messages::handle_send_message_draft(state, token, &params),
        "editmessagetext" => messages::handle_edit_message_text(state, token, &params),
        "editmessagecaption" => messages::handle_edit_message_caption(state, token, &params),
        "editmessagemedia" => messages::handle_edit_message_media(state, token, &params),
        "editmessagelivelocation" => messages::handle_edit_message_live_location(state, token, &params),
        "stopmessagelivelocation" => messages::handle_stop_message_live_location(state, token, &params),
        "editmessagechecklist" => messages::handle_edit_message_checklist(state, token, &params),
        "editmessagereplymarkup" => messages::handle_edit_message_reply_markup(state, token, &params),
        "deletemessage" => messages::handle_delete_message(state, token, &params),
        "deletemessages" => messages::handle_delete_messages(state, token, &params),
        "banchatmember" => chats::handle_ban_chat_member(state, token, &params),
        "unbanchatmember" => chats::handle_unban_chat_member(state, token, &params),
        "restrictchatmember" => chats::handle_restrict_chat_member(state, token, &params),
        "promotechatmember" => chats::handle_promote_chat_member(state, token, &params),
        "setchatadministratorcustomtitle" => chats::handle_set_chat_administrator_custom_title(state, token, &params),
        "setchatmembertag" => chats::handle_set_chat_member_tag(state, token, &params),
        "banchatsenderchat" => chats::handle_ban_chat_sender_chat(state, token, &params),
        "unbanchatsenderchat" => chats::handle_unban_chat_sender_chat(state, token, &params),
        "setchatpermissions" => chats::handle_set_chat_permissions(state, token, &params),
        "exportchatinvitelink" => links::handle_export_chat_invite_link(state, token, &params),
        "createchatinvitelink" => links::handle_create_chat_invite_link(state, token, &params),
        "editchatinvitelink" => links::handle_edit_chat_invite_link(state, token, &params),
        "revokechatinvitelink" => links::handle_revoke_chat_invite_link(state, token, &params),
        "createchatsubscriptioninvitelink" => links::handle_create_chat_subscription_invite_link(state, token, &params),
        "editchatsubscriptioninvitelink" => links::handle_edit_chat_subscription_invite_link(state, token, &params),
        "approvechatjoinrequest" => chats::handle_approve_chat_join_request(state, token, &params),
        "declinechatjoinrequest" => chats::handle_decline_chat_join_request(state, token, &params),
        "getchat" => chats::handle_get_chat(state, token, &params),
        "getchatadministrators" => chats::handle_get_chat_administrators(state, token, &params),
        "getchatmembercount" => chats::handle_get_chat_member_count(state, token, &params),
        "getchatmember" => chats::handle_get_chat_member(state, token, &params),
        "getbusinessconnection" => bussines::handle_get_business_connection(state, token, &params),
        "getmanagedbottoken" => bot::handle_get_managed_bot_token(state, token, &params),
        "replacemanagedbottoken" => bot::handle_replace_managed_bot_token(state, token, &params),
        "getuserchatboosts" => users::handle_get_user_chat_boosts(state, token, &params),
        "setchatmenubutton" => chats::handle_set_chat_menu_button(state, token, &params),
        "getchatmenubutton" => chats::handle_get_chat_menu_button(state, token, &params),
        "setchatphoto" => chats::handle_set_chat_photo(state, token, &params),
        "deletechatphoto" => chats::handle_delete_chat_photo(state, token, &params),
        "setchattitle" => chats::handle_set_chat_title(state, token, &params),
        "setchatdescription" => chats::handle_set_chat_description(state, token, &params),
        "setchatstickerset" => chats::handle_set_chat_sticker_set(state, token, &params),
        "deletechatstickerset" => chats::handle_delete_chat_sticker_set(state, token, &params),
        "getforumtopiciconstickers" => topics::handle_get_forum_topic_icon_stickers(state, token, &params),
        "createforumtopic" => topics::handle_create_forum_topic(state, token, &params),
        "editforumtopic" => topics::handle_edit_forum_topic(state, token, &params),
        "closeforumtopic" => topics::handle_close_forum_topic(state, token, &params),
        "reopenforumtopic" => topics::handle_reopen_forum_topic(state, token, &params),
        "deleteforumtopic" => topics::handle_delete_forum_topic(state, token, &params),
        "unpinallforumtopicmessages" => {
            topics::handle_unpin_all_forum_topic_messages(state, token, &params)
        }
        "editgeneralforumtopic" => topics::handle_edit_general_forum_topic(state, token, &params),
        "closegeneralforumtopic" => topics::handle_close_general_forum_topic(state, token, &params),
        "reopengeneralforumtopic" => topics::handle_reopen_general_forum_topic(state, token, &params),
        "hidegeneralforumtopic" => topics::handle_hide_general_forum_topic(state, token, &params),
        "unhidegeneralforumtopic" => topics::handle_unhide_general_forum_topic(state, token, &params),
        "unpinallgeneralforumtopicmessages" => {
            topics::handle_unpin_all_general_forum_topic_messages(state, token, &params)
        }
        "pinchatmessage" => chats::handle_pin_chat_message(state, token, &params),
        "unpinchatmessage" => chats::handle_unpin_chat_message(state, token, &params),
        "unpinallchatmessages" => chats::handle_unpin_all_chat_messages(state, token, &params),
        "leavechat" => chats::handle_leave_chat(state, token, &params),
        "getfile" => files::handle_get_file(state, token, &params),
        "getuserprofilephotos" => users::handle_get_user_profile_photos(state, token, &params),
        "getuserprofileaudios" => users::handle_get_user_profile_audios(state, token, &params),
        "setuseremojistatus" => users::handle_set_user_emoji_status(state, token, &params),
        "getupdates" => bot::handle_get_updates(state, token, &params),
        "getwebhookinfo" => bot::handle_get_webhook_info(state, token, &params),
        "setwebhook" => bot::handle_set_webhook(state, token, &params),
        "deletewebhook" => bot::handle_delete_webhook(state, token, &params),
        "logout" => bot::handle_log_out(state, token, &params),
        "close" => bot::handle_close(state, token, &params),
        "setmessagereaction" => messages::handle_set_message_reaction(state, token, &params),
        "stoppoll" => messages::handle_stop_poll(state, token, &params),
        "approvesuggestedpost" => channels::handle_approve_suggested_post(state, token, &params),
        "declinesuggestedpost" => channels::handle_decline_suggested_post(state, token, &params),
        "answercallbackquery" => callback::handle_answer_callback_query(state, token, &params),
        "answerwebappquery" => webapp::handle_answer_web_app_query(state, token, &params),
        "answerinlinequery" => inline::handle_answer_inline_query(state, token, &params),
        "answershippingquery" => payments::handle_answer_shipping_query(state, token, &params),
        "answerprecheckoutquery" => payments::handle_answer_pre_checkout_query(state, token, &params),
        "createinvoicelink" => payments::handle_create_invoice_link(state, token, &params),
        "getmystarbalance" => bot::handle_get_my_star_balance(state, token, &params),
        "getstartransactions" => payments::handle_get_star_transactions(state, token, &params),
        "setmycommands" => bot::handle_set_my_commands(state, token, &params),
        "getmycommands" => bot::handle_get_my_commands(state, token, &params),
        "deletemycommands" => bot::handle_delete_my_commands(state, token, &params),
        "setmyname" => bot::handle_set_my_name(state, token, &params),
        "getmyname" => bot::handle_get_my_name(state, token, &params),
        "setmydescription" => bot::handle_set_my_description(state, token, &params),
        "getmydescription" => bot::handle_get_my_description(state, token, &params),
        "setmyshortdescription" => bot::handle_set_my_short_description(state, token, &params),
        "getmyshortdescription" => bot::handle_get_my_short_description(state, token, &params),
        "setmyprofilephoto" => bot::handle_set_my_profile_photo(state, token, &params),
        "removemyprofilephoto" => bot::handle_remove_my_profile_photo(state, token, &params),
        "setmydefaultadministratorrights" => {
            bot::handle_set_my_default_administrator_rights(state, token, &params)
        }
        "getmydefaultadministratorrights" => {
            bot::handle_get_my_default_administrator_rights(state, token, &params)
        }
        "refundstarpayment" => payments::handle_refund_star_payment(state, token, &params),
        "edituserstarsubscription" => users::handle_edit_user_star_subscription(state, token, &params),
        "savepreparedinlinemessage" => webapp::handle_save_prepared_inline_message(state, token, &params),
        "savepreparedkeyboardbutton" => webapp::handle_save_prepared_keyboard_button(state, token, &params),
        "setpassportdataerrors" => passport::handle_set_passport_data_errors(state, token, &params),
        "verifyuser" => users::handle_verify_user(state, token, &params),
        "verifychat" => chats::handle_verify_chat(state, token, &params),
        "removeuserverification" => users::handle_remove_user_verification(state, token, &params),
        "removechatverification" => chats::handle_remove_chat_verification(state, token, &params),
        "getstickerset" => stickers::handle_get_sticker_set(state, token, &params),
        "getcustomemojistickers" => stickers::handle_get_custom_emoji_stickers(state, token, &params),
        "uploadstickerfile" => stickers::handle_upload_sticker_file(state, token, &params),
        "createnewstickerset" => stickers::handle_create_new_sticker_set(state, token, &params),
        "addstickertoset" => stickers::handle_add_sticker_to_set(state, token, &params),
        "setstickerpositioninset" => stickers::handle_set_sticker_position_in_set(state, token, &params),
        "deletestickerfromset" => stickers::handle_delete_sticker_from_set(state, token, &params),
        "replacestickerinset" => stickers::handle_replace_sticker_in_set(state, token, &params),
        "setgamescore" => games::handle_set_game_score(state, token, &params),
        "getgamehighscores" => games::handle_get_game_high_scores(state, token, &params),
        "readbusinessmessage" => bussines::handle_read_business_message(state, token, &params),
        "deletebusinessmessages" => bussines::handle_delete_business_messages(state, token, &params),
        "setbusinessaccountname" => bussines::handle_set_business_account_name(state, token, &params),
        "setbusinessaccountusername" => bussines::handle_set_business_account_username(state, token, &params),
        "setbusinessaccountbio" => bussines::handle_set_business_account_bio(state, token, &params),
        "setbusinessaccountprofilephoto" => bussines::handle_set_business_account_profile_photo(state, token, &params),
        "removebusinessaccountprofilephoto" => {
            bussines::handle_remove_business_account_profile_photo(state, token, &params)
        }
        "setbusinessaccountgiftsettings" => {
            bussines::handle_set_business_account_gift_settings(state, token, &params)
        }
        "getbusinessaccountstarbalance" => {
            bussines::handle_get_business_account_star_balance(state, token, &params)
        }
        "transferbusinessaccountstars" => {
            bussines::handle_transfer_business_account_stars(state, token, &params)
        }
        "getbusinessaccountgifts" => bussines::handle_get_business_account_gifts(state, token, &params),
        "getavailablegifts" => gifts::handle_get_available_gifts(state, token, &params),
        "sendgift" => messages::handle_send_gift(state, token, &params),
        "giftpremiumsubscription" => gifts::handle_gift_premium_subscription(state, token, &params),
        "getusergifts" => gifts::handle_get_user_gifts(state, token, &params),
        "getchatgifts" => gifts::handle_get_chat_gifts(state, token, &params),
        "convertgifttostars" => gifts::handle_convert_gift_to_stars(state, token, &params),
        "upgradegift" => gifts::handle_upgrade_gift(state, token, &params),
        "transfergift" => gifts::handle_transfer_gift(state, token, &params),
        "poststory" => stories::handle_post_story(state, token, &params),
        "repoststory" => stories::handle_repost_story(state, token, &params),
        "editstory" => stories::handle_edit_story(state, token, &params),
        "deletestory" => stories::handle_delete_story(state, token, &params),
        "setstickeremojilist" => stickers::handle_set_sticker_emoji_list(state, token, &params),
        "setstickerkeywords" => stickers::handle_set_sticker_keywords(state, token, &params),
        "setstickermaskposition" =>stickers:: handle_set_sticker_mask_position(state, token, &params),
        "setstickersettitle" => stickers::handle_set_sticker_set_title(state, token, &params),
        "setstickersetthumbnail" => stickers::handle_set_sticker_set_thumbnail(state, token, &params),
        "setcustomemojistickersetthumbnail" => stickers::handle_set_custom_emoji_sticker_set_thumbnail(state, token, &params),
        "deletestickerset" => stickers::handle_delete_sticker_set(state, token, &params),
        _ => Err(ApiError::not_found(format!("method {} not found", method))),
    }
}

fn chat_id_to_chat_key(chat_id: i64) -> String {
    format!("chat:{}", chat_id)
}

fn ensure_default_user(conn: &mut rusqlite::Connection) -> Result<SimUserRecord, ApiError> {
    // Keep bootstrap resilient even if legacy data already consumed `test_user`
    // in bots/chats. Existing default user keeps its current username.
    if client::users::load_sim_user_record(conn, 10001)?.is_some() {
        return client::users::ensure_user(conn, Some(10001), Some("Test User".to_string()), None);
    }

    let preferred_username = "test_user".to_string();
    let default_username = match client::bot::ensure_username_available_globally(
        conn,
        &preferred_username,
        None,
        Some(10001),
        None,
    ) {
        Ok(()) => Some(preferred_username),
        Err(error) if error.code == 400 => None,
        Err(error) => return Err(error),
    };

    client::users::ensure_user(
        conn,
        Some(10001),
        Some("Test User".to_string()),
        default_username,
    )
}

fn token_suffix(token: &str) -> String {
    token
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .rev()
        .take(8)
        .collect::<String>()
        .chars()
        .rev()
        .collect()
}

fn generate_telegram_numeric_id() -> String {
    let ts = Utc::now().timestamp_micros().unsigned_abs();
    let bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
    let mut mix: u64 = 0;
    for b in bytes.iter().take(8) {
        mix = (mix << 8) | u64::from(*b);
    }
    format!("{}{}", ts, mix % 1_000_000)
}

fn generate_telegram_file_id(kind: &str) -> String {
    use sha2::{Digest, Sha256};

    let raw = format!("{}:{}:{}", kind, Utc::now().timestamp_nanos_opt().unwrap_or_default(), uuid::Uuid::new_v4());
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    let digest = hasher.finalize();
    let hexed = hex::encode(digest);
    format!("AgACAgQAAxk{}", &hexed[..48])
}

fn generate_telegram_file_unique_id() -> String {
    use sha2::{Digest, Sha256};

    let raw = format!("{}:{}", Utc::now().timestamp_micros(), uuid::Uuid::new_v4());
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    let digest = hasher.finalize();
    let hexed = hex::encode(digest);
    format!("AQAD{}", &hexed[..24])
}

fn generate_telegram_token() -> String {
    let left = ((Utc::now().timestamp_millis().abs() as u64) % 900_000_000) + 100_000_000;
    let right = format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple());
    let compact = right.chars().take(35).collect::<String>();
    format!("{}:{}", left, compact)
}

fn sanitize_username(input: &str) -> String {
    input
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect::<String>()
        .to_lowercase()
}

fn coerce_string_scalars(value: Value) -> Value {
    match value {
        Value::String(raw) => {
            let trimmed = raw.trim();
            if trimmed.eq_ignore_ascii_case("true") {
                return Value::Bool(true);
            }
            if trimmed.eq_ignore_ascii_case("false") {
                return Value::Bool(false);
            }
            if trimmed.eq_ignore_ascii_case("null") {
                return Value::Null;
            }
            if let Ok(integer) = trimmed.parse::<i64>() {
                return Value::Number(integer.into());
            }
            if let Ok(float_number) = trimmed.parse::<f64>() {
                if let Some(number) = serde_json::Number::from_f64(float_number) {
                    return Value::Number(number);
                }
            }
            Value::String(raw)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(coerce_string_scalars).collect()),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, item)| (key, coerce_string_scalars(item)))
                .collect(),
        ),
        other => other,
    }
}

fn coerce_scalar_strings(value: Value) -> Value {
    match value {
        Value::Number(number) => Value::String(number.to_string()),
        Value::Bool(flag) => Value::String(flag.to_string()),
        Value::Array(items) => Value::Array(items.into_iter().map(coerce_scalar_strings).collect()),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, item)| (key, coerce_scalar_strings(item)))
                .collect(),
        ),
        other => other,
    }
}

fn parse_request_ignoring_prefixed_fields<T: DeserializeOwned>(
    params: &HashMap<String, Value>,
    ignored_prefixes: &[&str],
) -> Result<T, ApiError> {
    if ignored_prefixes.is_empty() {
        return parse_request(params);
    }

    let filtered = params
        .iter()
        .filter(|(key, _)| {
            !ignored_prefixes
                .iter()
                .any(|prefix| key.starts_with(prefix))
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<HashMap<String, Value>>();

    parse_request(&filtered)
}

fn normalize_request_decode_error(message: &str) -> String {
    if message.contains("expected struct InputFile") {
        return "can't parse InputFile JSON object".to_string();
    }
    if let Some((_, tail)) = message.split_once("unknown field `") {
        if let Some((field, _)) = tail.split_once('`') {
            return format!(
                "can't parse request JSON object: unknown field \"{}\"",
                field
            );
        }
        return "can't parse request JSON object: unknown field".to_string();
    }
    if let Some((_, tail)) = message.split_once("missing field `") {
        if let Some((field, _)) = tail.split_once('`') {
            return format!(
                "can't parse request JSON object: missing required field \"{}\"",
                field
            );
        }
        return "can't parse request JSON object: missing required field".to_string();
    }
    if message.contains("invalid type") {
        return "can't parse request JSON object: invalid field type".to_string();
    }
    if message.contains("invalid value") {
        return "can't parse request JSON object: invalid field value".to_string();
    }
    message.to_string()
}

fn value_to_optional_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => Some(value.to_string()),
    }
}

fn sql_value_to_rusqlite(v: &Value) -> rusqlite::types::Value {
    match v {
        Value::Null => rusqlite::types::Value::Null,
        Value::Bool(b) => rusqlite::types::Value::Integer(if *b { 1 } else { 0 }),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                rusqlite::types::Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                rusqlite::types::Value::Real(f)
            } else {
                rusqlite::types::Value::Null
            }
        }
        Value::String(s) => rusqlite::types::Value::Text(s.clone()),
        _ => rusqlite::types::Value::Text(v.to_string()),
    }
}

fn parse_request<T: DeserializeOwned>(params: &HashMap<String, Value>) -> Result<T, ApiError> {
    let object = Map::from_iter(params.iter().map(|(k, v)| (k.clone(), v.clone())));
    decode_request_value(Value::Object(object))
}

fn decode_request_value<T: DeserializeOwned>(payload: Value) -> Result<T, ApiError> {
    match serde_json::from_value(payload.clone()) {
        Ok(decoded) => Ok(decoded),
        Err(strict_error) => {
            let variants = vec![
                coerce_text_like_fields(payload.clone()),
                coerce_text_like_fields(coerce_string_scalars(payload.clone())),
                coerce_string_scalars(payload.clone()),
                coerce_scalar_strings(payload.clone()),
                coerce_scalar_strings(coerce_string_scalars(payload)),
            ];

            for candidate in variants {
                if let Ok(decoded) = serde_json::from_value(candidate) {
                    return Ok(decoded);
                }
            }

            Err(ApiError::bad_request(normalize_request_decode_error(
                &strict_error.to_string(),
            )))
        }
    }
}

fn coerce_text_like_fields(value: Value) -> Value {
    coerce_text_like_fields_for_key(None, value)
}

fn coerce_text_like_fields_for_key(field_name: Option<&str>, value: Value) -> Value {
    match value {
        Value::Number(number) if field_name.map(is_text_like_field_name).unwrap_or(false) => {
            Value::String(number.to_string())
        }
        Value::Bool(flag) if field_name.map(is_text_like_field_name).unwrap_or(false) => {
            Value::String(flag.to_string())
        }
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|item| coerce_text_like_fields_for_key(field_name, item))
                .collect(),
        ),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, item)| {
                    let coerced = coerce_text_like_fields_for_key(Some(key.as_str()), item);
                    (key, coerced)
                })
                .collect(),
        ),
        other => other,
    }
}

fn is_text_like_field_name(field_name: &str) -> bool {
    matches!(
        field_name,
        "text"
            | "caption"
            | "question"
            | "description"
            | "title"
            | "comment"
            | "payload"
            | "first_name"
            | "last_name"
            | "username"
            | "phone_number"
            | "emoji"
            | "name"
            | "url"
            | "currency"
            | "provider_token"
            | "invoice_payload"
            | "business_connection_id"
            | "gift_id"
            | "owned_gift_id"
    )
}

fn publish_sim_client_event(
    state: &Data<AppState>,
    token: &str,
    event_payload: Value,
) {
    state.ws_hub.publish_json(token, &event_payload);
}
