//! Media at the MTProto level: uploads in parts, photos (stored in Telegram's sizes) and
//! documents, sending them and again by id, captions, albums, and downloads.

use simula_core::{Account, Peer, Sandbox, UserId};
use simula_tl::{enums, functions, types};

use crate::common::{as_account, setup};

fn png(w: u32, h: u32) -> Vec<u8> {
    let image = image::DynamicImage::new_rgb8(w, h);
    let mut out = std::io::Cursor::new(Vec::new());
    image.write_to(&mut out, image::ImageFormat::Png).unwrap();
    out.into_inner()
}

/// Upload `data` as `account` in two parts; returns the `InputFile`.
async fn upload(sandbox: &Sandbox, account: UserId, data: &[u8], name: &str) -> enums::InputFile {
    let id = rand::random::<i64>();
    let half = data.len() / 2;
    for (part, bytes) in [&data[..half], &data[half..]].into_iter().enumerate() {
        let saved = sandbox
            .call(
                as_account(account),
                functions::upload::SaveFilePart {
                    file_id: id,
                    file_part: part as i32,
                    bytes: bytes.to_vec(),
                },
            )
            .await
            .unwrap();
        assert!(saved);
    }
    types::InputFile {
        id,
        parts: 2,
        name: name.into(),
        md5_checksum: String::new(),
    }
    .into()
}

async fn send_media(
    sandbox: &Sandbox,
    from: UserId,
    to: Peer,
    media: enums::InputMedia,
    caption: &str,
) -> Result<types::Message, simula_core::RpcError> {
    let updates = sandbox
        .call(
            as_account(from),
            functions::messages::SendMedia {
                peer: sandbox.input_peer(from, to),
                media,
                message: caption.into(),
                random_id: rand::random(),
                ..Default::default()
            },
        )
        .await?;
    Ok(new_messages(&updates).remove(0))
}

fn new_messages(updates: &enums::Updates) -> Vec<types::Message> {
    let enums::Updates::Updates(u) = updates else {
        panic!()
    };
    u.updates
        .iter()
        .filter_map(|u| match u {
            enums::Update::NewMessage(n) => Some(&n.message),
            enums::Update::NewChannelMessage(n) => Some(&n.message),
            _ => None,
        })
        .filter_map(|m| match m {
            enums::Message::Message(m) => Some((**m).clone()),
            _ => None,
        })
        .collect()
}

fn photo_of(message: &types::Message) -> types::Photo {
    match &message.media {
        Some(enums::MessageMedia::Photo(m)) => match &m.photo {
            Some(enums::Photo::Photo(p)) => (**p).clone(),
            _ => panic!("photo expected"),
        },
        other => panic!("photo expected, got {other:?}"),
    }
}

fn document_of(message: &types::Message) -> types::Document {
    match &message.media {
        Some(enums::MessageMedia::Document(m)) => match &m.document {
            Some(enums::Document::Document(d)) => (**d).clone(),
            _ => panic!("document expected"),
        },
        other => panic!("document expected, got {other:?}"),
    }
}

async fn download(
    sandbox: &Sandbox,
    account: UserId,
    location: enums::InputFileLocation,
) -> Result<Vec<u8>, simula_core::RpcError> {
    let enums::upload::File::File(file) = sandbox
        .call(
            as_account(account),
            functions::upload::GetFile {
                location,
                offset: 0,
                limit: 1024 * 1024,
                ..Default::default()
            },
        )
        .await?
    else {
        panic!()
    };
    Ok(file.bytes)
}

async fn talking(sandbox: &Sandbox, alice: &Account, bot: &Account) {
    sandbox
        .call(
            as_account(alice.id),
            crate::common::send(sandbox.input_peer(alice.id, bot.id.into()), "hi", 1),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn photos_documents_and_downloads() {
    let (sandbox, alice, bot) = setup().await;
    talking(&sandbox, &alice, &bot).await;
    let mut rx = sandbox.subscribe_updates();

    let file = upload(&sandbox, alice.id, &png(1000, 600), "cat.png").await;
    let sent = send_media(
        &sandbox,
        alice.id,
        bot.id.into(),
        types::InputMediaUploadedPhoto {
            file,
            ..Default::default()
        }
        .into(),
        "look",
    )
    .await
    .unwrap();
    let photo = photo_of(&sent);
    let captioned = sent.id;
    assert_eq!(sent.message, "look");
    let sizes: Vec<_> = photo
        .sizes
        .iter()
        .map(|s| match s {
            enums::PhotoSize::PhotoSize(s) => (s.r#type.clone(), s.w, s.h),
            _ => panic!(),
        })
        .collect();
    assert_eq!(
        sizes,
        vec![
            ("s".into(), 90, 54),
            ("m".into(), 320, 192),
            ("x".into(), 800, 480),
            ("y".into(), 1000, 600)
        ]
    );
    // The bot gets the same photo and downloads its sizes.
    let got = std::iter::from_fn(|| rx.try_recv().ok())
        .find(|batch| batch.account == bot.id)
        .expect("the bot's update");
    let bots_copy = new_messages(&got.updates).remove(0);
    assert_eq!(photo_of(&bots_copy).id, photo.id);
    let jpeg = download(
        &sandbox,
        bot.id,
        types::InputPhotoFileLocation {
            id: photo.id,
            access_hash: photo.access_hash,
            file_reference: photo.file_reference.clone(),
            thumb_size: "x".into(),
        }
        .into(),
    )
    .await
    .unwrap();
    let decoded = image::load_from_memory(&jpeg).unwrap();
    assert_eq!((decoded.width(), decoded.height()), (800, 480));
    let wrong = download(
        &sandbox,
        bot.id,
        types::InputPhotoFileLocation {
            id: photo.id,
            access_hash: photo.access_hash ^ 1,
            file_reference: vec![],
            thumb_size: "x".into(),
        }
        .into(),
    )
    .await
    .unwrap_err();
    assert_eq!(wrong.message, "LOCATION_INVALID");

    // The bot sends it back by id.
    let again = send_media(
        &sandbox,
        bot.id,
        alice.id.into(),
        types::InputMediaPhoto {
            id: types::InputPhoto {
                id: photo.id,
                access_hash: photo.access_hash,
                file_reference: photo.file_reference.clone(),
            }
            .into(),
            ..Default::default()
        }
        .into(),
        "",
    )
    .await
    .unwrap();
    assert_eq!(photo_of(&again).id, photo.id);
    let forged = send_media(
        &sandbox,
        bot.id,
        alice.id.into(),
        types::InputMediaPhoto {
            id: types::InputPhoto {
                id: photo.id,
                access_hash: 1,
                file_reference: vec![],
            }
            .into(),
            ..Default::default()
        }
        .into(),
        "",
    )
    .await
    .unwrap_err();
    assert_eq!(forged.message, "MEDIA_EMPTY");

    // Documents: a text file keeps its name and bytes; an image gets a thumbnail.
    let text = b"some notes\n".repeat(100);
    let file = upload(&sandbox, alice.id, &text, "notes.txt").await;
    let sent = send_media(
        &sandbox,
        alice.id,
        bot.id.into(),
        types::InputMediaUploadedDocument {
            file,
            mime_type: "text/plain".into(),
            ..Default::default()
        }
        .into(),
        "",
    )
    .await
    .unwrap();
    let document = document_of(&sent);
    assert_eq!(document.mime_type, "text/plain");
    assert_eq!(document.size, text.len() as i64);
    assert!(document.thumbs.is_none());
    assert!(
        document.attributes.contains(
            &types::DocumentAttributeFilename {
                file_name: "notes.txt".into()
            }
            .into()
        )
    );
    let bytes = download(
        &sandbox,
        bot.id,
        types::InputDocumentFileLocation {
            id: document.id,
            access_hash: document.access_hash,
            file_reference: document.file_reference,
            thumb_size: String::new(),
        }
        .into(),
    )
    .await
    .unwrap();
    assert_eq!(bytes, text);
    let file = upload(&sandbox, alice.id, &png(640, 480), "scan.png").await;
    let image_doc = document_of(
        &send_media(
            &sandbox,
            alice.id,
            bot.id.into(),
            types::InputMediaUploadedDocument {
                file,
                mime_type: "image/png".into(),
                ..Default::default()
            }
            .into(),
            "",
        )
        .await
        .unwrap(),
    );
    assert!(matches!(
        image_doc.thumbs.as_deref(),
        Some([enums::PhotoSize::PhotoSize(t)]) if t.r#type == "m" && t.w == 320 && t.h == 240
    ));
    assert!(
        image_doc
            .attributes
            .contains(&types::DocumentAttributeImageSize { w: 640, h: 480 }.into())
    );

    // The server's checks.
    let half = upload(&sandbox, alice.id, &text, "x.txt").await;
    let enums::InputFile::InputFile(mut half) = half else {
        panic!()
    };
    half.parts = 3;
    let missing = send_media(
        &sandbox,
        alice.id,
        bot.id.into(),
        types::InputMediaUploadedDocument {
            file: enums::InputFile::InputFile(half),
            mime_type: "text/plain".into(),
            ..Default::default()
        }
        .into(),
        "",
    )
    .await
    .unwrap_err();
    assert_eq!(missing.message, "FILE_PART_2_MISSING");
    let file = upload(&sandbox, alice.id, b"nope, not a picture", "x.jpg").await;
    let err = send_media(
        &sandbox,
        alice.id,
        bot.id.into(),
        types::InputMediaUploadedPhoto {
            file,
            ..Default::default()
        }
        .into(),
        "",
    )
    .await
    .unwrap_err();
    assert_eq!(err.message, "IMAGE_PROCESS_FAILED");
    let file = upload(&sandbox, alice.id, &png(50, 50), "a.png").await;
    let err = send_media(
        &sandbox,
        alice.id,
        bot.id.into(),
        types::InputMediaUploadedPhoto {
            file,
            ..Default::default()
        }
        .into(),
        &"x".repeat(1025),
    )
    .await
    .unwrap_err();
    assert_eq!(err.message, "MEDIA_CAPTION_TOO_LONG");
    let err = sandbox
        .call(
            as_account(bot.id),
            functions::upload::GetFile {
                location: types::InputPhotoFileLocation {
                    id: photo.id,
                    access_hash: photo.access_hash,
                    file_reference: vec![],
                    thumb_size: "x".into(),
                }
                .into(),
                offset: 0,
                limit: 1000,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.message, "LIMIT_INVALID");

    // A caption can be edited away; the photo stays.
    let enums::Updates::Updates(edited) = sandbox
        .call(
            as_account(alice.id),
            functions::messages::EditMessage {
                peer: sandbox.input_peer(alice.id, bot.id.into()),
                id: captioned,
                message: Some(String::new()),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    else {
        panic!()
    };
    assert!(edited.updates.iter().any(|u| matches!(
        u,
        enums::Update::EditMessage(e) if matches!(&e.message, enums::Message::Message(m) if m.message.is_empty() && m.media.is_some())
    )));
}

#[tokio::test]
async fn albums() {
    let (sandbox, alice, bot) = setup().await;
    talking(&sandbox, &alice, &bot).await;
    let single = |file: enums::InputFile, caption: &str| -> enums::InputSingleMedia {
        types::InputSingleMedia {
            media: types::InputMediaUploadedPhoto {
                file,
                ..Default::default()
            }
            .into(),
            random_id: rand::random(),
            message: caption.into(),
            entities: None,
        }
        .into()
    };
    let first = upload(&sandbox, alice.id, &png(300, 200), "1.png").await;
    let second = upload(&sandbox, alice.id, &png(200, 300), "2.png").await;
    let sent = sandbox
        .call(
            as_account(alice.id),
            functions::messages::SendMultiMedia {
                peer: sandbox.input_peer(alice.id, bot.id.into()),
                multi_media: vec![single(first, "one"), single(second, "")],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let messages = new_messages(&sent);
    assert_eq!(messages.len(), 2);
    assert!(messages[0].grouped_id.is_some());
    assert_eq!(messages[0].grouped_id, messages[1].grouped_id);
    assert_eq!(messages[0].message, "one");
    let enums::Updates::Updates(u) = &sent else {
        panic!()
    };
    assert_eq!(
        u.updates
            .iter()
            .filter(|u| matches!(u, enums::Update::MessageID(_)))
            .count(),
        2
    );

    // Photos don't mix with documents.
    let photo = upload(&sandbox, alice.id, &png(10, 10), "p.png").await;
    let doc = upload(&sandbox, alice.id, b"text", "t.txt").await;
    let err = sandbox
        .call(
            as_account(alice.id),
            functions::messages::SendMultiMedia {
                peer: sandbox.input_peer(alice.id, bot.id.into()),
                multi_media: vec![
                    single(photo, ""),
                    types::InputSingleMedia {
                        media: types::InputMediaUploadedDocument {
                            file: doc,
                            mime_type: "text/plain".into(),
                            ..Default::default()
                        }
                        .into(),
                        random_id: 9,
                        message: String::new(),
                        entities: None,
                    }
                    .into(),
                ],
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.message, "MEDIA_INVALID");
}
