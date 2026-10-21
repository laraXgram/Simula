//! Media in messages: photos and documents, made from uploads (`inputMediaUploaded*`)
//! or reused by id (`inputMediaPhoto` / `inputMediaDocument`). A stored photo has its
//! sizes; a document its file, its attributes, and a thumbnail when it is an image or
//! the sender gave one. Media objects are the same for every viewer.

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::UserId;
use crate::services::{files, images};
use crate::store::{self, files::Media};

/// Captions: 1024 UTF-16 units, 4096 for Premium accounts.
pub const MAX_CAPTION_LENGTH: usize = 1024;
pub const MAX_PREMIUM_CAPTION_LENGTH: usize = 4096;

/// The media an `InputMedia` names, stored if it was just uploaded.
pub fn resolve(
    ctx: &Ctx<'_>,
    owner: UserId,
    input: &enums::InputMedia,
) -> RpcResult<enums::MessageMedia> {
    match input {
        enums::InputMedia::UploadedPhoto(m) => {
            if m.ttl_seconds.is_some() || m.live_photo || m.video.is_some() {
                return Err(RpcError::not_implemented(
                    "self-destructing and live photos",
                ));
            }
            let upload = files::take_upload(ctx, owner, &m.file)?;
            let photo = store_photo(ctx, owner, &upload.data)?;
            Ok(photo_media(photo, m.spoiler))
        }
        enums::InputMedia::Photo(m) => {
            if m.ttl_seconds.is_some() || m.live_photo || m.video.is_some() {
                return Err(RpcError::not_implemented(
                    "self-destructing and live photos",
                ));
            }
            Ok(photo_media(existing_photo(ctx, &m.id)?, m.spoiler))
        }
        enums::InputMedia::UploadedDocument(m) => {
            if m.ttl_seconds.is_some() {
                return Err(RpcError::not_implemented("self-destructing media"));
            }
            let upload = files::take_upload(ctx, owner, &m.file)?;
            let thumb = m
                .thumb
                .as_ref()
                .map(|thumb| files::take_upload(ctx, owner, thumb))
                .transpose()?;
            let document = store_document(
                ctx,
                owner,
                upload,
                &m.mime_type,
                m.attributes.clone(),
                thumb,
                m.nosound_video,
            )?;
            let cover = m
                .video_cover
                .as_ref()
                .map(|c| existing_photo(ctx, c))
                .transpose()?;
            Ok(document_media(
                document,
                m.spoiler,
                cover,
                m.video_timestamp,
            ))
        }
        enums::InputMedia::Document(m) => {
            if m.ttl_seconds.is_some() {
                return Err(RpcError::not_implemented("self-destructing media"));
            }
            let cover = m
                .video_cover
                .as_ref()
                .map(|c| existing_photo(ctx, c))
                .transpose()?;
            Ok(document_media(
                existing_document(ctx, &m.id)?,
                m.spoiler,
                cover,
                m.video_timestamp,
            ))
        }
        enums::InputMedia::PhotoExternal(_) | enums::InputMedia::DocumentExternal(_) => Err(
            RpcError::not_implemented("media fetched from URLs by the server (Simula is offline)"),
        ),
        _ => Err(RpcError::not_implemented("this kind of media")),
    }
}

/// Decode, size and store a photo.
pub fn store_photo(ctx: &Ctx<'_>, owner: UserId, data: &[u8]) -> RpcResult<types::Photo> {
    let sizes = images::photo_sizes(data)?;
    let id = files::new_id();
    let date = ctx.now();
    let photo = types::Photo {
        id,
        access_hash: rand::random(),
        file_reference: files::file_reference(),
        date,
        sizes: sizes.iter().map(photo_size).collect(),
        dc_id: files::DC_ID,
        ..Default::default()
    };
    store::files::put_media(
        ctx.db,
        &Media {
            id,
            photo: true,
            access_hash: photo.access_hash,
            owner,
            date,
            body: serde_json::to_value(enums::Photo::from(photo.clone()))
                .map_err(RpcError::internal)?,
        },
    )?;
    for size in &sizes {
        let blob = store::files::put_blob(ctx.db, &size.jpeg)?;
        store::files::put_media_file(ctx.db, id, size.kind, blob)?;
    }
    Ok(photo)
}

/// Store a document: its file, its attributes (the upload's name as its file name when
/// none was given; an image's size), and a thumbnail from `thumb` or from the file
/// itself when it is an image.
pub fn store_document(
    ctx: &Ctx<'_>,
    owner: UserId,
    upload: files::Upload,
    mime_type: &str,
    mut attributes: Vec<enums::DocumentAttribute>,
    thumb: Option<files::Upload>,
    nosound: bool,
) -> RpcResult<types::Document> {
    let id = files::new_id();
    let date = ctx.now();
    let mime_type = if mime_type.trim().is_empty() {
        "application/octet-stream".to_owned()
    } else {
        mime_type.trim().to_lowercase()
    };
    let has_name = attributes
        .iter()
        .any(|a| matches!(a, enums::DocumentAttribute::Filename(_)));
    if !has_name && !upload.name.is_empty() {
        attributes.push(
            types::DocumentAttributeFilename {
                file_name: upload.name.clone(),
            }
            .into(),
        );
    }
    let is_image = mime_type.starts_with("image/") && images::is_image(&upload.data);
    if is_image
        && !attributes
            .iter()
            .any(|a| matches!(a, enums::DocumentAttribute::ImageSize(_)))
        && let Some((w, h)) = images::dimensions(&upload.data)
    {
        attributes.push(
            types::DocumentAttributeImageSize {
                w: w as i32,
                h: h as i32,
            }
            .into(),
        );
    }
    // GIFs play as animations.
    if mime_type == "image/gif"
        && !attributes
            .iter()
            .any(|a| matches!(a, enums::DocumentAttribute::Animated))
    {
        attributes.push(enums::DocumentAttribute::Animated);
    }
    if nosound {
        for attribute in &mut attributes {
            if let enums::DocumentAttribute::Video(v) = attribute {
                v.nosound = true;
            }
        }
    }
    let thumb = match (&thumb, is_image) {
        (Some(thumb), _) => Some(images::thumbnail(&thumb.data)?),
        (None, true) => Some(images::thumbnail(&upload.data)?),
        (None, false) => None,
    };
    let document = types::Document {
        id,
        access_hash: rand::random(),
        file_reference: files::file_reference(),
        date,
        mime_type,
        size: upload.data.len() as i64,
        thumbs: thumb.as_ref().map(|t| vec![photo_size(t)]),
        dc_id: files::DC_ID,
        attributes,
        ..Default::default()
    };
    store::files::put_media(
        ctx.db,
        &Media {
            id,
            photo: false,
            access_hash: document.access_hash,
            owner,
            date,
            body: serde_json::to_value(enums::Document::from(document.clone()))
                .map_err(RpcError::internal)?,
        },
    )?;
    let blob = store::files::put_blob(ctx.db, &upload.data)?;
    store::files::put_media_file(ctx.db, id, "", blob)?;
    if let Some(thumb) = &thumb {
        let blob = store::files::put_blob(ctx.db, &thumb.jpeg)?;
        store::files::put_media_file(ctx.db, id, thumb.kind, blob)?;
    }
    Ok(document)
}

fn photo_size(size: &images::Size) -> enums::PhotoSize {
    types::PhotoSize {
        r#type: size.kind.to_owned(),
        w: size.w as i32,
        h: size.h as i32,
        size: size.jpeg.len() as i32,
    }
    .into()
}

/// A stored photo by id and access hash (`MEDIA_EMPTY` otherwise).
pub fn existing_photo(ctx: &Ctx<'_>, input: &enums::InputPhoto) -> RpcResult<types::Photo> {
    let enums::InputPhoto::InputPhoto(input) = input else {
        return Err(RpcError::bad_request("MEDIA_EMPTY"));
    };
    match stored(ctx, input.id, input.access_hash, true)? {
        Some(enums::Photo::Photo(photo)) => Ok(*photo),
        _ => Err(RpcError::bad_request("MEDIA_EMPTY")),
    }
}

/// A stored document by id and access hash (`MEDIA_EMPTY` otherwise).
pub fn existing_document(
    ctx: &Ctx<'_>,
    input: &enums::InputDocument,
) -> RpcResult<types::Document> {
    let enums::InputDocument::InputDocument(input) = input else {
        return Err(RpcError::bad_request("MEDIA_EMPTY"));
    };
    match stored::<enums::Document>(ctx, input.id, input.access_hash, false)? {
        Some(enums::Document::Document(document)) => Ok(*document),
        _ => Err(RpcError::bad_request("MEDIA_EMPTY")),
    }
}

fn stored<T: serde::de::DeserializeOwned>(
    ctx: &Ctx<'_>,
    id: i64,
    access_hash: i64,
    photo: bool,
) -> RpcResult<Option<T>> {
    Ok(store::files::media(ctx.db, id)?
        .filter(|m| m.access_hash == access_hash && m.photo == photo)
        .and_then(|m| serde_json::from_value(m.body).ok()))
}

pub fn photo_media(photo: types::Photo, spoiler: bool) -> enums::MessageMedia {
    types::MessageMediaPhoto {
        spoiler,
        photo: Some(photo.into()),
        ..Default::default()
    }
    .into()
}

/// A document as message media, marked as a video, round video or voice message by its
/// attributes.
pub fn document_media(
    document: types::Document,
    spoiler: bool,
    cover: Option<types::Photo>,
    timestamp: Option<i32>,
) -> enums::MessageMedia {
    let video = document.attributes.iter().find_map(|a| match a {
        enums::DocumentAttribute::Video(v) => Some(v.round_message),
        _ => None,
    });
    let animated = document
        .attributes
        .iter()
        .any(|a| matches!(a, enums::DocumentAttribute::Animated));
    let voice = document
        .attributes
        .iter()
        .any(|a| matches!(a, enums::DocumentAttribute::Audio(a) if a.voice));
    types::MessageMediaDocument {
        spoiler,
        video: video == Some(false) && !animated,
        round: video == Some(true),
        voice,
        document: Some(document.into()),
        video_cover: cover.map(Into::into),
        video_timestamp: timestamp,
        ..Default::default()
    }
    .into()
}

/// What a medium may be grouped with in an album.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlbumKind {
    /// Photos and videos mix.
    Visual,
    Document,
    Audio,
}

/// The album kind of a medium, or `None` when it can't be in an album.
pub fn album_kind(media: &enums::MessageMedia) -> Option<AlbumKind> {
    match media {
        enums::MessageMedia::Photo(_) => Some(AlbumKind::Visual),
        enums::MessageMedia::Document(m) => {
            let enums::Document::Document(document) = m.document.as_ref()? else {
                return None;
            };
            let has =
                |f: &dyn Fn(&enums::DocumentAttribute) -> bool| document.attributes.iter().any(f);
            if m.round
                || m.voice
                || has(&|a| {
                    matches!(
                        a,
                        enums::DocumentAttribute::Animated | enums::DocumentAttribute::Sticker(_)
                    )
                })
            {
                None
            } else if m.video {
                Some(AlbumKind::Visual)
            } else if has(&|a| matches!(a, enums::DocumentAttribute::Audio(_))) {
                Some(AlbumKind::Audio)
            } else {
                Some(AlbumKind::Document)
            }
        }
        _ => None,
    }
}
