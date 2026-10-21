//! Photos, documents, audio, voice, video, animations and video notes, built the way
//! tdlib sends them: uploads go up in parts and become `inputMediaUploaded*` with the
//! attributes tdlib sets (file name, audio and video details, MIME types it falls back
//! to); `file_id`s become `inputMediaPhoto` / `inputMediaDocument`.
//!
//! One deviation: the server tells animations from videos by analysing the file;
//! Simula can't, so animations carry `documentAttributeAnimated` from the start.

use simula_tl::{enums, functions, types};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};
use crate::files::id::FileType;
use crate::files::input::{Source, TlFile};
use crate::files::mime;

/// telegram-bot-api's bounds for durations and sides.
pub const MAX_DURATION: i32 = 24 * 60 * 60;
pub const MAX_LENGTH: i32 = 10_000;

/// tdlib's default side of a video note.
const VIDEO_NOTE_LENGTH: i32 = 384;

#[derive(Debug, Clone)]
pub enum Kind {
    Photo,
    Document {
        force_file: bool,
    },
    Audio {
        duration: i32,
        title: Option<String>,
        performer: Option<String>,
    },
    Voice {
        duration: i32,
    },
    Video {
        duration: i32,
        width: i32,
        height: i32,
        supports_streaming: bool,
        cover: Option<Source>,
        start_timestamp: i32,
    },
    Animation {
        duration: i32,
        width: i32,
        height: i32,
    },
    VideoNote {
        duration: i32,
        length: i32,
    },
}

/// A medium to send, with its files still to be uploaded or decoded.
#[derive(Debug, Clone)]
pub struct Media {
    pub kind: Kind,
    pub file: Source,
    pub thumbnail: Option<Source>,
    pub spoiler: bool,
}

impl Kind {
    /// The `file_id` types this kind accepts.
    fn file_type(&self) -> FileType {
        match self {
            Kind::Photo => FileType::Photo,
            Kind::Document { .. } => FileType::Document,
            Kind::Audio { .. } => FileType::Audio,
            Kind::Voice { .. } => FileType::VoiceNote,
            Kind::Video { .. } => FileType::Video,
            Kind::Animation { .. } => FileType::Animation,
            Kind::VideoNote { .. } => FileType::VideoNote,
        }
    }

    /// The right a restricted member needs, and tdlib's words for the content.
    pub fn right(&self) -> (fn(&types::ChatBannedRights) -> bool, &'static str) {
        match self {
            Kind::Photo => (|r| r.send_photos, "photos"),
            Kind::Document { .. } => (|r| r.send_docs, "documents"),
            Kind::Audio { .. } => (|r| r.send_audios, "music"),
            Kind::Voice { .. } => (|r| r.send_voices, "voice notes"),
            Kind::Video { .. } => (|r| r.send_videos, "videos"),
            Kind::Animation { .. } => (|r| r.send_gifs, "animations"),
            Kind::VideoNote { .. } => (|r| r.send_roundvideos, "video notes"),
        }
    }

    /// tdlib's `MessageContentType` name, as in "Audio can't be mixed with other media
    /// types".
    pub fn content_type(&self) -> &'static str {
        match self {
            Kind::Photo => "Photo",
            Kind::Document { .. } => "Document",
            Kind::Audio { .. } => "Audio",
            Kind::Voice { .. } => "VoiceNote",
            Kind::Video { .. } => "Video",
            Kind::Animation { .. } => "Animation",
            Kind::VideoNote { .. } => "VideoNote",
        }
    }
}

/// Clamp an integer argument like telegram-bot-api's `get_integer_arg(…, 0, 0, max)`.
pub fn integer(value: Option<i64>, max: i32) -> i32 {
    value.unwrap_or(0).clamp(0, i64::from(max)) as i32
}

impl Media {
    /// The TL `InputMedia`.
    pub async fn into_tl(self, ctx: &BotCtx) -> BotResult<enums::InputMedia> {
        let spoiler = self.spoiler;
        let file = self.file.into_tl(ctx, self.kind.file_type()).await?;
        let (cover, timestamp) = match &self.kind {
            Kind::Video {
                cover,
                start_timestamp,
                ..
            } => {
                let cover = match cover.clone() {
                    Some(cover) => Some(cover_photo(ctx, cover).await?),
                    None => None,
                };
                (cover, (*start_timestamp > 0).then_some(*start_timestamp))
            }
            _ => (None, None),
        };
        let (file, name) = match file {
            TlFile::Remote(id) if matches!(self.kind, Kind::Photo) => {
                return Ok(types::InputMediaPhoto {
                    spoiler,
                    id: id.input_photo(),
                    ..Default::default()
                }
                .into());
            }
            TlFile::Remote(id) => {
                return Ok(types::InputMediaDocument {
                    spoiler,
                    id: id.input_document(),
                    video_cover: cover,
                    video_timestamp: timestamp,
                    ..Default::default()
                }
                .into());
            }
            TlFile::Uploaded { file, name } => (file, name),
        };
        if matches!(self.kind, Kind::Photo) {
            return Ok(types::InputMediaUploadedPhoto {
                spoiler,
                file,
                ..Default::default()
            }
            .into());
        }
        let thumb = match self.thumbnail {
            Some(thumbnail) => thumbnail.into_thumbnail(ctx).await?,
            None => None,
        };
        let filename = || -> Option<enums::DocumentAttribute> {
            (!name.is_empty()).then(|| {
                types::DocumentAttributeFilename {
                    file_name: name.clone(),
                }
                .into()
            })
        };
        let by_name = mime::from_name(&name);
        let (mime_type, attributes, force_file, nosound) = match self.kind {
            Kind::Photo => unreachable!("handled above"),
            Kind::Document { force_file } => (
                by_name.to_owned(),
                filename().into_iter().collect(),
                force_file,
                false,
            ),
            Kind::Audio {
                duration,
                title,
                performer,
            } => {
                let mut attributes: Vec<enums::DocumentAttribute> = vec![
                    types::DocumentAttributeAudio {
                        duration,
                        title,
                        performer,
                        ..Default::default()
                    }
                    .into(),
                ];
                attributes.extend(filename());
                let mime = if by_name.starts_with("audio/") {
                    by_name
                } else {
                    "audio/mpeg"
                };
                (mime.to_owned(), attributes, false, false)
            }
            Kind::Voice { duration } => {
                let attributes = vec![
                    types::DocumentAttributeAudio {
                        voice: true,
                        duration,
                        ..Default::default()
                    }
                    .into(),
                ];
                let mime = match by_name {
                    "audio/ogg" | "audio/mpeg" | "audio/mp4" => by_name,
                    _ => "audio/ogg",
                };
                (mime.to_owned(), attributes, false, false)
            }
            Kind::Video {
                duration,
                width,
                height,
                supports_streaming,
                ..
            } => {
                let mut attributes: Vec<enums::DocumentAttribute> = vec![
                    types::DocumentAttributeVideo {
                        supports_streaming,
                        duration: f64::from(duration),
                        w: width,
                        h: height,
                        ..Default::default()
                    }
                    .into(),
                ];
                attributes.extend(filename());
                let mime = if by_name.starts_with("video/") {
                    by_name
                } else {
                    "video/mp4"
                };
                (mime.to_owned(), attributes, false, false)
            }
            Kind::Animation {
                duration,
                width,
                height,
            } => {
                let mut attributes: Vec<enums::DocumentAttribute> =
                    filename().into_iter().collect();
                let mut mime = by_name.to_owned();
                if mime == "video/mp4" {
                    attributes.push(
                        types::DocumentAttributeVideo {
                            duration: f64::from(duration),
                            w: width,
                            h: height,
                            ..Default::default()
                        }
                        .into(),
                    );
                } else if width != 0 && height != 0 {
                    if !mime.starts_with("image/") {
                        mime = "image/gif".into();
                    }
                    attributes.push(
                        types::DocumentAttributeImageSize {
                            w: width,
                            h: height,
                        }
                        .into(),
                    );
                }
                attributes.push(enums::DocumentAttribute::Animated);
                (mime, attributes, false, false)
            }
            Kind::VideoNote { duration, length } => {
                let side = if length > 0 {
                    length
                } else {
                    VIDEO_NOTE_LENGTH
                };
                let attributes = vec![
                    types::DocumentAttributeVideo {
                        round_message: true,
                        duration: f64::from(duration),
                        w: side,
                        h: side,
                        ..Default::default()
                    }
                    .into(),
                ];
                ("video/mp4".to_owned(), attributes, false, true)
            }
        };
        Ok(types::InputMediaUploadedDocument {
            nosound_video: nosound,
            force_file,
            spoiler,
            file,
            thumb,
            mime_type,
            attributes,
            video_cover: cover,
            video_timestamp: timestamp,
            ..Default::default()
        }
        .into())
    }
}

/// A video's cover: a photo by `file_id`, or uploaded first (`messages.uploadMedia`), as
/// tdlib does.
async fn cover_photo(ctx: &BotCtx, cover: Source) -> BotResult<enums::InputPhoto> {
    match cover.into_tl(ctx, FileType::Photo).await? {
        TlFile::Remote(id) => Ok(id.input_photo()),
        TlFile::Uploaded { file, .. } => {
            let media = ctx
                .call(
                    functions::messages::UploadMedia {
                        peer: enums::InputPeer::InputPeerSelf,
                        media: types::InputMediaUploadedPhoto {
                            file,
                            ..Default::default()
                        }
                        .into(),
                        business_connection_id: None,
                    },
                    ErrorContext::Plain,
                )
                .await?;
            match media {
                enums::MessageMedia::Photo(m) => match m.photo {
                    Some(enums::Photo::Photo(photo)) => Ok(types::InputPhoto {
                        id: photo.id,
                        access_hash: photo.access_hash,
                        file_reference: photo.file_reference.clone(),
                    }
                    .into()),
                    _ => Err(BotError::internal("the cover wasn't stored")),
                },
                _ => Err(BotError::internal("the cover wasn't stored")),
            }
        }
    }
}
