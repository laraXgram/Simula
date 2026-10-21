//! Photos and documents in messages → the Bot API's `photo`, `document`, `audio`,
//! `video`, `animation`, `voice` and `video_note`, as tdlib classifies documents by
//! their attributes, with `file_id`s in tdlib's format.

use simula_botapi_types::types as bot;
use simula_tl::{enums, types};

use crate::files::id::{FileId, FileType};

/// A message's media fields.
#[derive(Default)]
pub struct Media {
    pub photo: Option<Vec<bot::PhotoSize>>,
    pub document: Option<Box<bot::Document>>,
    pub audio: Option<Box<bot::Audio>>,
    pub video: Option<Box<bot::Video>>,
    pub animation: Option<Box<bot::Animation>>,
    pub voice: Option<Box<bot::Voice>>,
    pub video_note: Option<Box<bot::VideoNote>>,
    pub has_media_spoiler: bool,
}

impl Media {
    pub fn is_some(&self) -> bool {
        self.photo.is_some()
            || self.document.is_some()
            || self.audio.is_some()
            || self.video.is_some()
            || self.voice.is_some()
            || self.video_note.is_some()
    }
}

pub fn convert(media: Option<&enums::MessageMedia>) -> Media {
    match media {
        Some(enums::MessageMedia::Photo(m)) => match &m.photo {
            Some(enums::Photo::Photo(photo)) => Media {
                photo: Some(photo_sizes(photo)),
                has_media_spoiler: m.spoiler,
                ..Default::default()
            },
            _ => Media::default(),
        },
        Some(enums::MessageMedia::Document(m)) => match &m.document {
            Some(enums::Document::Document(document)) => {
                let mut out = document_media(document, m.video_cover.as_ref(), m.video_timestamp);
                out.has_media_spoiler = m.spoiler;
                out
            }
            _ => Media::default(),
        },
        _ => Media::default(),
    }
}

/// A photo's sizes, smallest first (tdlib leaves out stripped and path sizes).
pub fn photo_sizes(photo: &types::Photo) -> Vec<bot::PhotoSize> {
    photo
        .sizes
        .iter()
        .filter_map(|size| match size {
            enums::PhotoSize::PhotoSize(s) => Some((s.r#type.as_str(), s.w, s.h, Some(s.size))),
            enums::PhotoSize::PhotoCachedSize(s) => {
                Some((s.r#type.as_str(), s.w, s.h, Some(s.bytes.len() as i32)))
            }
            enums::PhotoSize::Progressive(s) => {
                Some((s.r#type.as_str(), s.w, s.h, s.sizes.last().copied()))
            }
            _ => None,
        })
        .map(|(kind, w, h, size)| {
            let id = FileId {
                file_type: FileType::Photo,
                dc_id: photo.dc_id,
                file_reference: photo.file_reference.clone(),
                id: photo.id,
                access_hash: photo.access_hash,
                size: kind.chars().next(),
            };
            bot::PhotoSize {
                file_id: id.encode(),
                file_unique_id: id.unique(),
                width: w.into(),
                height: h.into(),
                file_size: size.map(Into::into),
            }
        })
        .collect()
}

/// A document's thumbnail (its largest one).
fn thumbnail(document: &types::Document) -> Option<Box<bot::PhotoSize>> {
    let (kind, w, h, size) = document
        .thumbs
        .iter()
        .flatten()
        .filter_map(|t| match t {
            enums::PhotoSize::PhotoSize(s) => Some((s.r#type.clone(), s.w, s.h, s.size)),
            _ => None,
        })
        .max_by_key(|(_, w, h, _)| w * h)?;
    let id = FileId {
        file_type: FileType::Thumbnail,
        dc_id: document.dc_id,
        file_reference: document.file_reference.clone(),
        id: document.id,
        access_hash: document.access_hash,
        size: kind.chars().next(),
    };
    Some(Box::new(bot::PhotoSize {
        file_id: id.encode(),
        file_unique_id: id.unique(),
        width: w.into(),
        height: h.into(),
        file_size: Some(size.into()),
    }))
}

fn document_media(
    document: &types::Document,
    cover: Option<&enums::Photo>,
    timestamp: Option<i32>,
) -> Media {
    let attributes = &document.attributes;
    let file_name = attributes.iter().find_map(|a| match a {
        enums::DocumentAttribute::Filename(f) => Some(f.file_name.clone()),
        _ => None,
    });
    let video = attributes.iter().find_map(|a| match a {
        enums::DocumentAttribute::Video(v) => Some(v),
        _ => None,
    });
    let audio = attributes.iter().find_map(|a| match a {
        enums::DocumentAttribute::Audio(a) => Some(a),
        _ => None,
    });
    let image = attributes.iter().find_map(|a| match a {
        enums::DocumentAttribute::ImageSize(s) => Some((s.w, s.h)),
        _ => None,
    });
    let animated = attributes
        .iter()
        .any(|a| matches!(a, enums::DocumentAttribute::Animated));
    let id = |file_type| FileId {
        file_type,
        dc_id: document.dc_id,
        file_reference: document.file_reference.clone(),
        id: document.id,
        access_hash: document.access_hash,
        size: None,
    };
    let mime_type = Some(document.mime_type.clone()).filter(|m| !m.is_empty());
    let file_size = Some(document.size);
    // tdlib rounds durations up to whole seconds.
    let seconds = |d: f64| d.max(0.0).ceil() as i64;

    let as_document = |file_type| {
        let id = id(file_type);
        Box::new(bot::Document {
            file_id: id.encode(),
            file_unique_id: id.unique(),
            thumbnail: thumbnail(document),
            file_name: file_name.clone(),
            mime_type: mime_type.clone(),
            file_size,
        })
    };

    if animated && (video.is_some() || image.is_some() || document.mime_type == "image/gif") {
        let file = id(FileType::Animation);
        let (w, h, duration) = match (video, image) {
            (Some(v), _) => (v.w, v.h, seconds(v.duration)),
            (None, Some((w, h))) => (w, h, 0),
            _ => (0, 0, 0),
        };
        return Media {
            animation: Some(Box::new(bot::Animation {
                file_id: file.encode(),
                file_unique_id: file.unique(),
                width: w.into(),
                height: h.into(),
                duration,
                thumbnail: thumbnail(document),
                file_name: file_name.clone(),
                mime_type: mime_type.clone(),
                file_size,
            })),
            // For backward compatibility the Bot API also gives animations as documents.
            document: Some(as_document(FileType::Animation)),
            ..Default::default()
        };
    }
    if let Some(audio) = audio {
        if audio.voice {
            let file = id(FileType::VoiceNote);
            return Media {
                voice: Some(Box::new(bot::Voice {
                    file_id: file.encode(),
                    file_unique_id: file.unique(),
                    duration: audio.duration.into(),
                    mime_type,
                    file_size,
                })),
                ..Default::default()
            };
        }
        let file = id(FileType::Audio);
        return Media {
            audio: Some(Box::new(bot::Audio {
                file_id: file.encode(),
                file_unique_id: file.unique(),
                duration: audio.duration.into(),
                performer: audio.performer.clone(),
                title: audio.title.clone(),
                file_name,
                mime_type,
                file_size,
                thumbnail: thumbnail(document),
            })),
            ..Default::default()
        };
    }
    if let Some(video) = video {
        if video.round_message {
            let file = id(FileType::VideoNote);
            return Media {
                video_note: Some(Box::new(bot::VideoNote {
                    file_id: file.encode(),
                    file_unique_id: file.unique(),
                    length: video.w.into(),
                    duration: seconds(video.duration),
                    thumbnail: thumbnail(document),
                    file_size,
                })),
                ..Default::default()
            };
        }
        let file = id(FileType::Video);
        let cover = match cover {
            Some(enums::Photo::Photo(photo)) => Some(photo_sizes(photo)),
            _ => None,
        };
        return Media {
            video: Some(Box::new(bot::Video {
                file_id: file.encode(),
                file_unique_id: file.unique(),
                width: video.w.into(),
                height: video.h.into(),
                duration: seconds(video.duration),
                thumbnail: thumbnail(document),
                cover,
                start_timestamp: timestamp.map(Into::into),
                file_name,
                mime_type,
                file_size,
                ..Default::default()
            })),
            ..Default::default()
        };
    }
    Media {
        document: Some(as_document(FileType::Document)),
        ..Default::default()
    }
}
