//! Codec checks against fixed byte strings and JSON text. The round-trip suite can't
//! catch a mistake that the encoder and decoder share; these can.

use simula_tl::{
    Deserializable, Function, Identifiable, Serializable, enums, functions, mtproto, types,
};

fn hex(s: &str) -> Vec<u8> {
    let digits: String = s.split_whitespace().collect();
    (0..digits.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&digits[i..i + 2], 16).unwrap())
        .collect()
}

const NONCE: &str = "51 A1 14 3F C7 A3 66 6B E4 BE 54 D6 89 0A 02 DC";

/// <https://core.telegram.org/mtproto/samples-auth_key>, step 1 payload body.
#[test]
fn req_pq_multi_matches_official_sample() {
    let call = mtproto::functions::ReqPqMulti {
        nonce: hex(NONCE).try_into().unwrap(),
    };
    assert_eq!(call.to_bytes(), hex(&format!("F1 8E 7E BE {NONCE}")));
}

/// Same page, step 2: the server's resPQ body, decoded and re-encoded byte for byte.
#[test]
fn res_pq_matches_official_sample() {
    let body = hex(&format!(
        "63 24 16 05 {NONCE}
         63 24 8F 67 48 21 4E AB 8A 2F 4C C8 76 E1 19 74
         08 2E 9C DB 98 C8 0C DA 4B 00 00 00
         15 C4 B5 1C 03 00 00 00
         85 FD 64 DE 85 1D 9D D0 A5 B7 F7 09 35 5F C3 0B 21 6B E8 6C 02 2B B4 C3"
    ));
    let mtproto::enums::ResPQ::ResPQ(res) = mtproto::enums::ResPQ::from_bytes(&body).unwrap();
    assert_eq!(res.nonce.to_vec(), hex(NONCE));
    assert_eq!(res.pq, hex("2E 9C DB 98 C8 0C DA 4B"));
    assert_eq!(
        res.server_public_key_fingerprints,
        [
            i64::from_le_bytes(hex("85 FD 64 DE 85 1D 9D D0").try_into().unwrap()),
            i64::from_le_bytes(hex("A5 B7 F7 09 35 5F C3 0B").try_into().unwrap()),
            i64::from_le_bytes(hex("21 6B E8 6C 02 2B B4 C3").try_into().unwrap()),
        ]
    );
    assert_eq!(mtproto::enums::ResPQ::from(*res).to_bytes(), body);
}

fn empty_send_message() -> functions::messages::SendMessage {
    functions::messages::SendMessage {
        no_webpage: false,
        silent: false,
        background: false,
        clear_draft: false,
        noforwards: false,
        update_stickersets_order: false,
        invert_media: false,
        allow_paid_floodskip: false,
        peer: types::InputPeerSelf {}.into(),
        reply_to: None,
        message: "hi".into(),
        random_id: 42,
        reply_markup: None,
        entities: None,
        schedule_date: None,
        schedule_repeat_period: None,
        send_as: None,
        quick_reply_shortcut: None,
        effect: None,
        allow_paid_stars: None,
        suggested_post: None,
        rich_message: None,
    }
}

#[test]
fn flags_are_computed_from_fields() {
    let call = functions::messages::SendMessage {
        silent: true,
        schedule_date: Some(1_700_000_000),
        ..empty_send_message()
    };
    let bytes = call.to_bytes();
    assert_eq!(
        &bytes[..4],
        &functions::messages::SendMessage::CONSTRUCTOR_ID.to_le_bytes()
    );
    // silent = flags.5, schedule_date = flags.10
    let flags = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    assert_eq!(flags, (1 << 5) | (1 << 10));
    assert_eq!(Function::from_bytes(&bytes).unwrap(), Function::from(call));
}

#[test]
fn generic_query_nests_a_whole_function() {
    let call = functions::InvokeWithLayer {
        layer: simula_tl::LAYER,
        query: Box::new(Function::from(functions::help::GetConfig {})),
    };
    let bytes = call.to_bytes();
    assert_eq!(
        &bytes[..4],
        &functions::InvokeWithLayer::CONSTRUCTOR_ID.to_le_bytes()
    );
    assert_eq!(&bytes[4..8], &simula_tl::LAYER.to_le_bytes());
    assert_eq!(
        &bytes[8..],
        &functions::help::GetConfig::CONSTRUCTOR_ID.to_le_bytes()
    );

    let json = serde_json::to_value(Function::from(call)).unwrap();
    assert_eq!(json["_"], "invokeWithLayer");
    assert_eq!(json["query"]["_"], "help.getConfig");
}

#[test]
fn json_tl_shape() {
    let entity: enums::MessageEntity = types::MessageEntityBold {
        offset: 0,
        length: 5,
    }
    .into();
    assert_eq!(
        serde_json::to_string(&entity).unwrap(),
        r#"{"_":"messageEntityBold","offset":0,"length":5}"#
    );

    // long → string (64-bit safe for JavaScript), and numbers are accepted on input.
    let peer: enums::InputPeer = types::InputPeerUser {
        user_id: 1 << 53,
        access_hash: -1,
    }
    .into();
    let json = serde_json::to_string(&peer).unwrap();
    assert_eq!(
        json,
        r#"{"_":"inputPeerUser","user_id":"9007199254740992","access_hash":"-1"}"#
    );
    let parsed: enums::InputPeer =
        serde_json::from_str(r#"{"_":"inputPeerUser","user_id":5,"access_hash":"-1"}"#).unwrap();
    assert_eq!(
        parsed,
        types::InputPeerUser {
            user_id: 5,
            access_hash: -1
        }
        .into()
    );

    // Unset optional fields and false bit flags are omitted.
    let json = serde_json::to_value(Function::from(empty_send_message())).unwrap();
    assert!(json.get("silent").is_none() && json.get("reply_to").is_none());
    assert_eq!(json["peer"]["_"], "inputPeerSelf");
}

#[test]
fn rejects_unknown_constructors() {
    let err = enums::MessageEntity::from_bytes(&[0xef, 0xbe, 0xad, 0xde]).unwrap_err();
    assert_eq!(
        err,
        simula_tl::DecodeError::UnexpectedConstructor {
            id: 0xdead_beef,
            expected: "MessageEntity"
        }
    );
    let err = serde_json::from_str::<enums::MessageEntity>(r#"{"_":"nope"}"#).unwrap_err();
    assert!(err.to_string().contains("unknown variant"), "{err}");
}
