//! Behaviour the generated round-trip suite can't express: real Telegram payloads,
//! request coercion, union choice from wire JSON, and documented limits.

use serde_json::{Value, json};
use simula_botapi_types::types::{
    InlineQueryResult, MaybeInaccessibleMessage, Message, ReplyMarkup, Update,
};
use simula_botapi_types::{
    ChatId, InputFile, Method, ParamKind, Rule, Validate, methods, registry,
};

/// Telegram JSON → types → JSON must be lossless.
fn assert_lossless<T: serde::Serialize + serde::de::DeserializeOwned>(wire: Value) -> T {
    let value: T = serde_json::from_value(wire.clone()).expect("decode");
    assert_eq!(serde_json::to_value(&value).unwrap(), wire);
    value
}

#[test]
fn private_message_update_is_lossless() {
    let update: Update = assert_lossless(json!({
        "update_id": 10000,
        "message": {
            "message_id": 1365,
            "from": { "id": 1111111, "is_bot": false, "first_name": "Test", "username": "test", "language_code": "en" },
            "chat": { "id": 1111111, "first_name": "Test", "username": "test", "type": "private" },
            "date": 1441645532,
            "text": "/start hello",
            "entities": [{ "offset": 0, "length": 6, "type": "bot_command" }]
        }
    }));
    assert_eq!(
        update.message.unwrap().text.as_deref(),
        Some("/start hello")
    );
}

#[test]
fn inaccessible_callback_message_is_told_apart_by_date_zero() {
    let chat = json!({ "id": -1001, "title": "Group", "type": "supergroup" });
    let inaccessible: MaybeInaccessibleMessage =
        assert_lossless(json!({ "chat": chat, "message_id": 5, "date": 0 }));
    assert!(matches!(
        inaccessible,
        MaybeInaccessibleMessage::InaccessibleMessage(_)
    ));

    let regular: MaybeInaccessibleMessage =
        assert_lossless(json!({ "chat": chat, "message_id": 5, "date": 1700000000 }));
    assert!(matches!(regular, MaybeInaccessibleMessage::Message(_)));
}

#[test]
fn cached_and_url_photo_results_share_a_type_tag() {
    let cached: InlineQueryResult =
        assert_lossless(json!({ "type": "photo", "id": "1", "photo_file_id": "AgAD" }));
    assert!(matches!(cached, InlineQueryResult::CachedPhoto(_)));

    let by_url: InlineQueryResult = assert_lossless(json!({
        "type": "photo", "id": "2", "photo_url": "https://x/p.jpg", "thumbnail_url": "https://x/t.jpg"
    }));
    assert!(matches!(by_url, InlineQueryResult::Photo(_)));

    let unknown =
        serde_json::from_value::<InlineQueryResult>(json!({ "type": "hologram", "id": "3" }));
    assert!(unknown.is_err());
}

#[test]
fn reply_markup_variants_including_force_reply_on_keyboards() {
    let inline: ReplyMarkup = assert_lossless(json!({
        "inline_keyboard": [[{ "text": "Go", "callback_data": "go" }]],
        "force_reply": true
    }));
    assert!(matches!(inline, ReplyMarkup::InlineKeyboardMarkup(_)));
    let force: ReplyMarkup = assert_lossless(json!({ "force_reply": true }));
    assert!(matches!(force, ReplyMarkup::ForceReply(_)));
    let remove: ReplyMarkup = assert_lossless(json!({ "remove_keyboard": true }));
    assert!(matches!(remove, ReplyMarkup::ReplyKeyboardRemove(_)));
}

#[test]
fn unset_optionals_and_false_flags_are_omitted() {
    let message = Message {
        message_id: 1,
        date: 2,
        ..Default::default()
    };
    let wire = serde_json::to_value(&message).unwrap();
    let mut keys: Vec<&str> = wire
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, ["chat", "date", "message_id"]);
}

#[test]
fn chat_id_accepts_numbers_numeric_strings_and_usernames() {
    let parse = |v: Value| serde_json::from_value::<ChatId>(v).unwrap();
    assert_eq!(parse(json!(-1001234)), ChatId::Id(-1001234));
    assert_eq!(parse(json!("-1001234")), ChatId::Id(-1001234));
    assert_eq!(
        parse(json!("@channel")),
        ChatId::Username("@channel".into())
    );
}

#[test]
fn send_photo_takes_attach_references_and_file_ids() {
    let upload: methods::SendPhoto =
        serde_json::from_value(json!({ "chat_id": 1, "photo": "attach://photo" })).unwrap();
    assert_eq!(upload.photo, InputFile::Attach("photo".into()));
    let reuse: methods::SendPhoto =
        serde_json::from_value(json!({ "chat_id": 1, "photo": "AgACAgIAAxk" })).unwrap();
    assert_eq!(reuse.photo, InputFile::Reference("AgACAgIAAxk".into()));
    const { assert!(<methods::SendPhoto as Method>::MULTIPART) };
}

#[test]
fn nested_limits_report_the_full_path() {
    let request: methods::SendMessage = serde_json::from_value(json!({
        "chat_id": 1,
        "text": "hi",
        "reply_markup": { "inline_keyboard": [[{ "text": "ok", "callback_data": "x".repeat(65) }]] }
    }))
    .unwrap();
    let violations = request.validate().unwrap_err();
    assert_eq!(violations.len(), 1);
    assert_eq!(
        violations[0].path,
        "reply_markup.inline_keyboard[0][0].callback_data"
    );
    assert_eq!(
        violations[0].rule,
        Rule::Bytes {
            min: Some(1),
            max: Some(64),
            actual: 65
        }
    );
}

#[test]
fn documented_ranges_items_and_enums() {
    let updates: methods::GetUpdates = serde_json::from_value(json!({ "limit": 101 })).unwrap();
    assert!(
        matches!(updates.validate().unwrap_err()[0].rule, Rule::Range { actual, .. } if actual == 101.0)
    );

    let action: methods::SendChatAction =
        serde_json::from_value(json!({ "chat_id": 1, "action": "dancing" })).unwrap();
    assert!(matches!(
        action.validate().unwrap_err()[0].rule,
        Rule::OneOf { .. }
    ));

    let group: methods::SendMediaGroup = serde_json::from_value(json!({
        "chat_id": 1, "media": [{ "type": "photo", "media": "AgAD" }]
    }))
    .unwrap();
    assert_eq!(
        group.validate().unwrap_err()[0].rule,
        Rule::Items {
            min: Some(2),
            max: Some(10),
            actual: 1
        }
    );
}

#[test]
fn registry_describes_every_method() {
    assert_eq!(registry::METHODS.len(), 185);
    let send = registry::METHODS
        .iter()
        .find(|m| m.name == "sendMessage")
        .unwrap();
    let chat_id = send.params.iter().find(|p| p.name == "chat_id").unwrap();
    assert_eq!(
        (chat_id.kind, chat_id.required),
        (ParamKind::IntegerOrString, true)
    );
    let markup = send
        .params
        .iter()
        .find(|p| p.name == "reply_markup")
        .unwrap();
    assert_eq!(markup.kind, ParamKind::Json);
}
