//! Media as a bot sends and receives it: uploads (multipart, `attach://`), `file_id`s
//! reused, `getFile` and downloads, albums, captions, and the errors of each step.

use axum::http::StatusCode;
use serde_json::{Value, json};

use crate::support::{fails, get_raw, next_updates, ok, post_multipart, user_writes, world};

fn png(w: u32, h: u32) -> Vec<u8> {
    let image = image::DynamicImage::new_rgb8(w, h);
    let mut out = std::io::Cursor::new(Vec::new());
    image.write_to(&mut out, image::ImageFormat::Png).unwrap();
    out.into_inner()
}

fn ok_body(status: StatusCode, body: Value) -> Value {
    assert_eq!(status, StatusCode::OK, "{body}");
    body["result"].clone()
}

#[tokio::test]
async fn photos_documents_and_downloads() {
    let w = world().await;
    let mut offset = 0;
    user_writes(&w, "hi").await;
    next_updates(&w, &mut offset).await;
    let chat = w.user.id.0.to_string();

    // A photo uploaded directly: Telegram's sizes, a caption with entities.
    let (status, body) = post_multipart(
        &w,
        "sendPhoto",
        &[
            ("chat_id", &chat),
            ("caption", "<b>cat</b> photo"),
            ("parse_mode", "HTML"),
        ],
        &[("photo", "cat.png", &png(1000, 600))],
    )
    .await;
    let sent = ok_body(status, body);
    let sizes = sent["photo"].as_array().unwrap();
    let dims: Vec<(i64, i64)> = sizes
        .iter()
        .map(|s| (s["width"].as_i64().unwrap(), s["height"].as_i64().unwrap()))
        .collect();
    assert_eq!(dims, vec![(90, 54), (320, 192), (800, 480), (1000, 600)]);
    let largest = sizes.last().unwrap()["file_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(largest.starts_with("AgACAgIAAxkB"), "{largest}");
    assert_eq!(sent["caption"], "cat photo");
    assert_eq!(
        sent["caption_entities"],
        json!([{ "type": "bold", "offset": 0, "length": 3 }])
    );
    assert!(sent.get("text").is_none());

    // The same photo again by file_id, then downloaded.
    let again = ok(
        &w,
        "sendPhoto",
        json!({ "chat_id": w.user.id.0, "photo": largest }),
    )
    .await;
    assert_eq!(
        again["photo"].as_array().unwrap().last().unwrap()["file_unique_id"],
        sizes.last().unwrap()["file_unique_id"]
    );
    let file = ok(&w, "getFile", json!({ "file_id": largest })).await;
    assert_eq!(file["file_path"], "photos/file_0.jpg");
    let (status, jpeg) = get_raw(&w, &format!("/file/bot{}/photos/file_0.jpg", w.token)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(jpeg.len() as i64, file["file_size"].as_i64().unwrap());
    assert_eq!(image::load_from_memory(&jpeg).unwrap().width(), 1000);
    assert_eq!(
        ok(&w, "getFile", json!({ "file_id": largest })).await["file_path"],
        "photos/file_0.jpg"
    );
    let (status, _) = get_raw(&w, &format!("/file/bot{}/photos/file_9.jpg", w.token)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // A document through attach://, with a name, MIME type and an image thumbnail.
    let notes = b"line\n".repeat(50);
    let (status, body) = post_multipart(
        &w,
        "sendDocument",
        &[
            ("chat_id", &chat),
            ("document", "attach://file"),
            ("thumbnail", "attach://thumb"),
        ],
        &[
            ("file", "notes.txt", &notes),
            ("thumb", "t.png", &png(64, 64)),
        ],
    )
    .await;
    let document = ok_body(status, body)["document"].clone();
    assert_eq!(document["file_name"], "notes.txt");
    assert_eq!(document["mime_type"], "text/plain");
    assert_eq!(document["file_size"], notes.len());
    assert_eq!(document["thumbnail"]["width"], 64);
    let path = ok(&w, "getFile", json!({ "file_id": document["file_id"] })).await["file_path"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(path.starts_with("documents/file_0"), "{path}");
    let (_, bytes) = get_raw(&w, &format!("/file/bot{}/{path}", w.token)).await;
    assert_eq!(bytes, notes);

    // Kinds of documents.
    let (status, body) = post_multipart(
        &w,
        "sendAudio",
        &[
            ("chat_id", &chat),
            ("duration", "61"),
            ("title", "Song"),
            ("performer", "Band"),
        ],
        &[("audio", "song.mp3", b"ID3 not really audio")],
    )
    .await;
    let audio = ok_body(status, body)["audio"].clone();
    assert_eq!(
        (
            audio["duration"].clone(),
            audio["title"].clone(),
            audio["mime_type"].clone()
        ),
        (json!(61), json!("Song"), json!("audio/mpeg"))
    );
    let (status, body) = post_multipart(
        &w,
        "sendVoice",
        &[("chat_id", &chat), ("duration", "3")],
        &[("voice", "v.ogg", b"OggS")],
    )
    .await;
    assert_eq!(ok_body(status, body)["voice"]["duration"], 3);
    let (status, body) = post_multipart(
        &w,
        "sendVideo",
        &[
            ("chat_id", &chat),
            ("duration", "5"),
            ("width", "640"),
            ("height", "360"),
            ("supports_streaming", "true"),
        ],
        &[("video", "clip.mp4", b"not really mp4")],
    )
    .await;
    let video = ok_body(status, body)["video"].clone();
    assert_eq!(
        (video["width"].clone(), video["duration"].clone()),
        (json!(640), json!(5))
    );
    let (status, body) = post_multipart(
        &w,
        "sendAnimation",
        &[("chat_id", &chat), ("width", "320"), ("height", "240")],
        &[("animation", "fun.mp4", b"not really mp4 either")],
    )
    .await;
    let sent = ok_body(status, body);
    assert_eq!(sent["animation"]["width"], 320);
    assert_eq!(sent["document"]["file_id"], sent["animation"]["file_id"]);
    let (status, body) = post_multipart(
        &w,
        "sendVideoNote",
        &[("chat_id", &chat), ("duration", "7")],
        &[("video_note", "round.mp4", b"round video")],
    )
    .await;
    assert_eq!(ok_body(status, body)["video_note"]["length"], 384);
    // A video's file_id works as a document; a photo's doesn't.
    ok(
        &w,
        "sendDocument",
        json!({ "chat_id": w.user.id.0, "document": video["file_id"] }),
    )
    .await;
    assert_eq!(
        fails(
            &w,
            "sendDocument",
            json!({ "chat_id": w.user.id.0, "document": largest })
        )
        .await,
        "Bad Request: can't use file of type Photo as Document"
    );

    // Errors along the way.
    assert_eq!(
        fails(&w, "sendPhoto", json!({ "chat_id": w.user.id.0 })).await,
        "Bad Request: there is no photo in the request"
    );
    assert_eq!(
        fails(
            &w,
            "sendPhoto",
            json!({ "chat_id": w.user.id.0, "photo": "attach://nothing" })
        )
        .await,
        "Bad Request: there is no photo in the request"
    );
    assert_eq!(
        fails(
            &w,
            "sendPhoto",
            json!({ "chat_id": w.user.id.0, "photo": "aGVsbG8" })
        )
        .await,
        "Bad Request: wrong remote file identifier specified: can't unserialize it. Wrong last symbol"
    );
    let (status, body) = post_multipart(
        &w,
        "sendPhoto",
        &[("chat_id", &chat)],
        &[("photo", "x.jpg", b"not an image")],
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["description"], "Bad Request: IMAGE_PROCESS_FAILED");
    assert_eq!(
        fails(&w, "getFile", json!({ "file_id": "AgAD" })).await,
        "Bad Request: invalid file_id"
    );

    // Captions: edited, emptied, too long.
    let edited = ok(
        &w,
        "editMessageCaption",
        json!({ "chat_id": w.user.id.0, "message_id": again["message_id"], "caption": "new caption", "show_caption_above_media": true }),
    )
    .await;
    assert_eq!(
        (
            edited["caption"].clone(),
            edited["show_caption_above_media"].clone()
        ),
        (json!("new caption"), json!(true))
    );
    assert_eq!(
        fails(&w, "editMessageCaption", json!({ "chat_id": w.user.id.0, "message_id": again["message_id"], "caption": "x".repeat(1025) })).await,
        "Bad Request: message caption is too long"
    );
    assert_eq!(
        fails(
            &w,
            "editMessageText",
            json!({ "chat_id": w.user.id.0, "message_id": again["message_id"], "text": "x" })
        )
        .await,
        "Bad Request: there is no text in the message to edit"
    );
}

#[tokio::test]
async fn albums() {
    let w = world().await;
    user_writes(&w, "hi").await;
    let chat = w.user.id.0.to_string();
    let media = json!([
        { "type": "photo", "media": "attach://a", "caption": "first" },
        { "type": "photo", "media": "attach://b" },
    ])
    .to_string();
    let (status, body) = post_multipart(
        &w,
        "sendMediaGroup",
        &[("chat_id", &chat), ("media", &media)],
        &[
            ("a", "a.png", &png(200, 100)),
            ("b", "b.png", &png(100, 200)),
        ],
    )
    .await;
    let sent = ok_body(status, body);
    let sent = sent.as_array().unwrap();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0]["media_group_id"], sent[1]["media_group_id"]);
    assert!(sent[0]["media_group_id"].is_string());
    assert_eq!(sent[0]["caption"], "first");

    let mixed = json!([
        { "type": "photo", "media": "attach://a" },
        { "type": "document", "media": "attach://b" },
    ])
    .to_string();
    let (_, body) = post_multipart(
        &w,
        "sendMediaGroup",
        &[("chat_id", &chat), ("media", &mixed)],
        &[("a", "a.png", &png(20, 20)), ("b", "b.txt", b"text")],
    )
    .await;
    assert_eq!(
        body["description"],
        "Bad Request: Document can't be mixed with other media types"
    );
    let missing = json!([{ "type": "photo", "media": "attach://gone" }]).to_string();
    let (_, body) = post_multipart(
        &w,
        "sendMediaGroup",
        &[("chat_id", &chat), ("media", &missing)],
        &[],
    )
    .await;
    assert_eq!(
        body["description"],
        "Bad Request: can't parse InputMedia: media not found"
    );
}
