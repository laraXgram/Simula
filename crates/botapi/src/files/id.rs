//! `file_id` and `file_unique_id`, in tdlib's format so libraries that decode them
//! (and bots that compare them) see what they see on Telegram.
//!
//! A `file_id` is tdlib's persistent id: the serialized `FullRemoteFileLocation` (file
//! type, flagged when a file reference follows; data center; file reference; then the
//! photo or document id, access hash and, for photo sizes, the size's source), with
//! runs of zero bytes compressed (`zero_encode`), followed by tdlib's version byte and
//! the persistent id version 4, in URL-safe base64 without padding. A
//! `file_unique_id` serializes only the file's class and identity, so it stays the same
//! however the file is reached.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// tdlib's `FileType` values that Simula produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum FileType {
    Thumbnail = 0,
    ProfilePhoto = 1,
    Photo = 2,
    VoiceNote = 3,
    Video = 4,
    Document = 5,
    Sticker = 8,
    Audio = 9,
    Animation = 10,
    VideoNote = 13,
}

impl FileType {
    fn from_i32(value: i32) -> Option<Self> {
        Some(match value {
            0 => Self::Thumbnail,
            1 => Self::ProfilePhoto,
            2 => Self::Photo,
            3 => Self::VoiceNote,
            4 => Self::Video,
            5 => Self::Document,
            8 => Self::Sticker,
            9 => Self::Audio,
            10 => Self::Animation,
            13 => Self::VideoNote,
            _ => return None,
        })
    }

    /// Photos and photo sizes; everything else is a document.
    pub fn is_photo(self) -> bool {
        matches!(self, Self::Thumbnail | Self::ProfilePhoto | Self::Photo)
    }

    /// tdlib's `is_document_file_type`: these may stand in for one another.
    pub fn is_document(self) -> bool {
        !self.is_photo()
    }

    /// tdlib's name for the type, as in "Can't use file of type Photo as Document".
    pub fn name(self) -> &'static str {
        match self {
            Self::Thumbnail => "Thumbnail",
            Self::ProfilePhoto => "ChatPhoto",
            Self::Photo => "Photo",
            Self::VoiceNote => "VoiceNote",
            Self::Video => "Video",
            Self::Document => "Document",
            Self::Sticker => "Sticker",
            Self::Audio => "Audio",
            Self::Animation => "Animation",
            Self::VideoNote => "VideoNote",
        }
    }

    /// The directory tdlib downloads the type into (`get_file_type_name`).
    pub fn dir(self) -> &'static str {
        match self {
            Self::Thumbnail => "thumbnails",
            Self::ProfilePhoto => "profile_photos",
            Self::Photo => "photos",
            Self::VoiceNote => "voice",
            Self::Video => "videos",
            Self::Document => "documents",
            Self::Sticker => "stickers",
            Self::Audio => "music",
            Self::Animation => "animations",
            Self::VideoNote => "video_notes",
        }
    }
}

/// Where a file lives on the server: a photo size or a document (or its thumbnail).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileId {
    pub file_type: FileType,
    pub dc_id: i32,
    pub file_reference: Vec<u8>,
    /// The photo's or document's id.
    pub id: i64,
    pub access_hash: i64,
    /// Photo sizes and thumbnails: the size's type letter ("x", "m"…).
    pub size: Option<char>,
}

/// tdlib's version bytes: `Version::Next - 1` and the persistent id version.
const TDLIB_VERSION: u8 = 61;
const PERSISTENT_ID_VERSION: u8 = 4;
const FILE_REFERENCE_FLAG: i32 = 1 << 25;
const WEB_LOCATION_FLAG: i32 = 1 << 24;
/// `PhotoSizeSource::Type::Thumbnail` in the source variant.
const THUMBNAIL_SOURCE: i32 = 1;

impl FileId {
    /// The `file_id`.
    pub fn encode(&self) -> String {
        let mut out = Vec::new();
        let mut file_type = self.file_type as i32;
        if !self.file_reference.is_empty() {
            file_type |= FILE_REFERENCE_FLAG;
        }
        out.extend(file_type.to_le_bytes());
        out.extend(self.dc_id.to_le_bytes());
        if !self.file_reference.is_empty() {
            store_bytes(&mut out, &self.file_reference);
        }
        out.extend(self.id.to_le_bytes());
        out.extend(self.access_hash.to_le_bytes());
        if self.file_type.is_photo() {
            out.extend(THUMBNAIL_SOURCE.to_le_bytes());
            out.extend(self.source_type().to_le_bytes());
            out.extend((self.size.unwrap_or('x') as i32).to_le_bytes());
        }
        let mut encoded = zero_encode(&out);
        encoded.push(TDLIB_VERSION);
        encoded.push(PERSISTENT_ID_VERSION);
        URL_SAFE_NO_PAD.encode(encoded)
    }

    /// The `file_unique_id`: the file's class and identity only.
    pub fn unique(&self) -> String {
        let mut out = Vec::new();
        // FileTypeClass + 1: photos 1, documents 2.
        let class: i32 = if self.file_type.is_photo() { 1 } else { 2 };
        out.extend(class.to_le_bytes());
        out.extend(self.id.to_le_bytes());
        if self.file_type.is_photo() {
            // `PhotoSizeSource::get_unique` of a thumbnail: its type, shifted.
            let kind = self.size.unwrap_or('x') as u8;
            out.push(match kind {
                b'a' => 0,
                b'c' => 1,
                kind => kind + 5,
            });
        }
        URL_SAFE_NO_PAD.encode(zero_encode(&out))
    }

    /// The source a photo size names: photos' sizes are photo files; thumbnails of
    /// documents are thumbnail files.
    fn source_type(&self) -> i32 {
        match self.file_type {
            FileType::Thumbnail => FileType::Thumbnail as i32,
            _ => FileType::Photo as i32,
        }
    }

    /// Parse a `file_id`, with tdlib's errors.
    pub fn decode(file_id: &str) -> Result<Self, String> {
        let wrong = || "Wrong remote file identifier specified: can't unserialize it".to_owned();
        // tdlib's `base64url_decode` errors.
        let trimmed = file_id.trim_end_matches('=');
        let binary = URL_SAFE_NO_PAD.decode(trimmed).map_err(|_| {
            let why = if trimmed.len() % 4 == 1 {
                "Wrong string length"
            } else {
                "Wrong character in the string"
            };
            format!("Wrong remote file identifier specified: {why}")
        })?;
        let Some((&last, rest)) = binary.split_last() else {
            return Err("Remote file identifier must be non-empty".into());
        };
        if last != PERSISTENT_ID_VERSION {
            return Err(
                "Wrong remote file identifier specified: can't unserialize it. Wrong last symbol"
                    .into(),
            );
        }
        let Some((_version, rest)) = rest.split_last() else {
            return Err("Invalid remote file identifier".into());
        };
        let data = zero_decode(rest);
        let mut reader = Reader(&data);
        let raw_type = reader.i32().ok_or_else(wrong)?;
        if raw_type & WEB_LOCATION_FLAG != 0 {
            return Err(wrong());
        }
        let has_reference = raw_type & FILE_REFERENCE_FLAG != 0;
        let file_type = FileType::from_i32(raw_type & !FILE_REFERENCE_FLAG).ok_or_else(wrong)?;
        let dc_id = reader.i32().ok_or_else(wrong)?;
        let file_reference = if has_reference {
            reader.bytes().ok_or_else(wrong)?
        } else {
            Vec::new()
        };
        let id = reader.i64().ok_or_else(wrong)?;
        let access_hash = reader.i64().ok_or_else(wrong)?;
        let size = if file_type.is_photo() {
            if reader.i32() != Some(THUMBNAIL_SOURCE) {
                return Err(wrong());
            }
            let _source_type = reader.i32().ok_or_else(wrong)?;
            let kind = reader.i32().ok_or_else(wrong)?;
            Some(
                char::from_u32(kind as u32)
                    .filter(char::is_ascii)
                    .ok_or_else(wrong)?,
            )
        } else {
            None
        };
        if !reader.0.is_empty() {
            return Err(wrong());
        }
        Ok(Self {
            file_type,
            dc_id,
            file_reference,
            id,
            access_hash,
            size,
        })
    }

    /// tdlib's check that a file may be used where `expected` is wanted (document
    /// kinds stand in for one another).
    pub fn check_type(&self, expected: FileType) -> Result<(), String> {
        let fits =
            self.file_type == expected || (self.file_type.is_document() && expected.is_document());
        if fits {
            Ok(())
        } else {
            Err(format!(
                "Can't use file of type {} as {}",
                self.file_type.name(),
                expected.name()
            ))
        }
    }
}

/// A TL string: a length byte (or 254 and three bytes), the data, padding to 4 bytes.
fn store_bytes(out: &mut Vec<u8>, data: &[u8]) {
    let header = if data.len() < 254 {
        out.push(data.len() as u8);
        1
    } else {
        out.push(254);
        out.extend(&(data.len() as u32).to_le_bytes()[..3]);
        4
    };
    out.extend(data);
    let total = header + data.len();
    out.extend(std::iter::repeat_n(0, (4 - total % 4) % 4));
}

/// tdlib's `zero_encode`: each run of zero bytes becomes a zero and the run's length.
fn zero_encode(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len());
    let mut i = 0;
    while i < data.len() {
        out.push(data[i]);
        if data[i] == 0 {
            let mut count = 1;
            while count < 250 && i + count < data.len() && data[i + count] == 0 {
                count += 1;
            }
            out.push(count as u8);
            i += count;
        } else {
            i += 1;
        }
    }
    out
}

fn zero_decode(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() * 2);
    let mut i = 0;
    while i < data.len() {
        if data[i] == 0 && i + 1 < data.len() {
            out.extend(std::iter::repeat_n(0, data[i + 1] as usize));
            i += 2;
        } else {
            out.push(data[i]);
            i += 1;
        }
    }
    out
}

struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Option<&[u8]> {
        if self.0.len() < n {
            return None;
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Some(head)
    }

    fn i32(&mut self) -> Option<i32> {
        Some(i32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn i64(&mut self) -> Option<i64> {
        Some(i64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    fn bytes(&mut self) -> Option<Vec<u8>> {
        let first = *self.take(1)?.first()?;
        let (len, header) = if first == 254 {
            let b = self.take(3)?;
            (u32::from_le_bytes([b[0], b[1], b[2], 0]) as usize, 4)
        } else {
            (first as usize, 1)
        };
        let data = self.take(len)?.to_vec();
        self.take((4 - (header + len) % 4) % 4)?;
        Some(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn photo() -> FileId {
        let mut reference = vec![1];
        reference.extend(std::iter::repeat_n(7, 24));
        FileId {
            file_type: FileType::Photo,
            dc_id: 2,
            file_reference: reference,
            id: 5_123_456_789_012_345_678,
            access_hash: -42,
            size: Some('y'),
        }
    }

    #[test]
    fn round_trips() {
        let photo = photo();
        assert_eq!(FileId::decode(&photo.encode()), Ok(photo.clone()));
        let document = FileId {
            file_type: FileType::Video,
            size: None,
            file_reference: Vec::new(),
            ..photo
        };
        assert_eq!(FileId::decode(&document.encode()), Ok(document));
    }

    #[test]
    fn looks_like_telegrams() {
        // Real photo ids from DC 2 with a 25-byte reference start "AgACAgIAAxkB".
        assert!(
            photo().encode().starts_with("AgACAgIAAxkB"),
            "{}",
            photo().encode()
        );
        // Unique ids differ per size, not per reference.
        let mut other = photo();
        other.file_reference = vec![1, 2, 3];
        assert_eq!(other.unique(), photo().unique());
        other.size = Some('m');
        assert_ne!(other.unique(), photo().unique());
    }

    #[test]
    fn tdlibs_errors() {
        assert_eq!(
            FileId::decode("aGVsbG8").unwrap_err(),
            "Wrong remote file identifier specified: can't unserialize it. Wrong last symbol"
        );
        assert_eq!(
            FileId::decode("hello").unwrap_err(),
            "Wrong remote file identifier specified: Wrong string length"
        );
        assert_eq!(
            photo().check_type(FileType::Document).unwrap_err(),
            "Can't use file of type Photo as Document"
        );
        let video = FileId {
            file_type: FileType::Video,
            size: None,
            ..photo()
        };
        assert!(video.check_type(FileType::Document).is_ok());
    }
}
