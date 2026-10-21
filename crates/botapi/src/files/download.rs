//! Downloads: `getFile` gives a file a path (tdlib's `<type dir>/file_<n>.<ext>`,
//! stable per file for each bot), and `/file/bot<token>/<path>` serves it, read from
//! the server in chunks as tdlib downloads it. Like telegram-bot-api without local mode,
//! only files of up to 20 MB can be downloaded.

use std::collections::HashMap;

use simula_tl::{enums, functions};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};
use crate::files::id::{FileId, FileType};

/// telegram-bot-api's `MAX_DOWNLOAD_FILE_SIZE`.
pub const MAX_DOWNLOAD_SIZE: usize = 20 << 20;
const CHUNK: i32 = 1024 * 1024;

/// A bot's downloaded files: paths by `file_unique_id`, and what each path holds.
#[derive(Debug, Default)]
pub struct Downloads {
    by_unique: HashMap<String, String>,
    by_path: HashMap<String, FileId>,
    counters: HashMap<&'static str, u32>,
}

impl Downloads {
    /// The file's path, the same one each time it is asked for.
    pub fn path_for(&mut self, id: &FileId, extension: Option<&str>) -> String {
        if let Some(path) = self.by_unique.get(&id.unique()) {
            return path.clone();
        }
        let dir = id.file_type.dir();
        let counter = self.counters.entry(dir).or_default();
        let mut path = format!("{dir}/file_{counter}");
        *counter += 1;
        if let Some(extension) = extension {
            path.push('.');
            path.push_str(extension);
        }
        self.by_unique.insert(id.unique(), path.clone());
        self.by_path.insert(path.clone(), id.clone());
        path
    }

    pub fn get(&self, path: &str) -> Option<FileId> {
        self.by_path.get(path).cloned()
    }
}

/// A whole file as the bot reads it, with its storage type; `None` when it is over
/// `max` bytes.
pub async fn read(
    ctx: &BotCtx,
    id: &FileId,
    max: usize,
) -> BotResult<Option<(Vec<u8>, enums::storage::FileType)>> {
    let mut data = Vec::new();
    loop {
        let enums::upload::File::File(chunk) = ctx
            .call(
                functions::upload::GetFile {
                    location: id.location(),
                    offset: data.len() as i64,
                    limit: CHUNK,
                    ..Default::default()
                },
                ErrorContext::Plain,
            )
            .await?
        else {
            return Err(BotError::internal("CDN redirects aren't expected"));
        };
        let storage = chunk.r#type;
        let last = chunk.bytes.len() < CHUNK as usize;
        data.extend_from_slice(&chunk.bytes);
        if data.len() > max {
            return Ok(None);
        }
        if last {
            return Ok(Some((data, storage)));
        }
    }
}

/// The extension tdlib gives a downloaded file of this kind and storage type.
pub fn extension(id: &FileId, storage: &enums::storage::FileType) -> Option<&'static str> {
    use enums::storage::FileType as S;
    if id.file_type.is_photo() {
        return Some("jpg");
    }
    Some(match storage {
        S::FileJpeg => "jpg",
        S::FilePng => "png",
        S::FileGif => "gif",
        S::FileWebp => "webp",
        S::FilePdf => "pdf",
        S::FileMp3 => "mp3",
        S::FileMov => "mov",
        S::FileMp4 => "mp4",
        _ => match id.file_type {
            FileType::VoiceNote => "oga",
            FileType::Video | FileType::VideoNote | FileType::Animation => "mp4",
            FileType::Audio => "mp3",
            _ => return None,
        },
    })
}
