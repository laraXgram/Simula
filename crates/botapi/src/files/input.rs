//! A file parameter as telegram-bot-api's `get_input_file` reads it, then as tdlib
//! sends it: a multipart part (directly, or `attach://<name>`) is uploaded with
//! `upload.saveFilePart`; a `file_id` names a stored photo or document; anything with
//! a dot is a URL.

use bytes::Bytes;
use simula_botapi_types::InputFile;
use simula_tl::{enums, functions, types};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};
use crate::errors::botapi;
use crate::files::fetch;
use crate::files::id::{FileId, FileType};

/// Upload parts are 512 KB; files over 10 MB go up as big files.
const PART_SIZE: usize = 512 * 1024;
const BIG_FILE_SIZE: usize = 10 << 20;

/// Where a file parameter's file comes from.
#[derive(Debug, Clone)]
pub enum Source {
    Upload {
        name: String,
        data: Bytes,
    },
    /// A `file_id` or a URL, read once the chat is known (as tdlib does).
    Reference(String),
}

/// What tdlib puts in the TL call for a file.
pub enum TlFile {
    Uploaded {
        file: enums::InputFile,
        name: String,
    },
    Remote(FileId),
}

/// A file parameter; a missing one is telegram-bot-api's "there is no <what> in the
/// request".
pub fn source(ctx: &BotCtx, input: Option<&InputFile>, what: &str) -> BotResult<Source> {
    let missing = || BotError::bad_request(format!("there is no {what} in the request"));
    match input {
        Some(InputFile::Attach(name)) => attached(ctx, name).ok_or_else(missing),
        Some(InputFile::Reference(value)) if !value.is_empty() => {
            Ok(Source::Reference(value.clone()))
        }
        _ => Err(missing()),
    }
}

/// A file named inside a JSON object (`InputMedia.media`), or `None` when the part
/// isn't there.
pub fn from_string(ctx: &BotCtx, value: &str) -> Option<Source> {
    match value.strip_prefix("attach://") {
        Some(name) => attached(ctx, name),
        None if value.is_empty() => None,
        None => Some(Source::Reference(value.to_owned())),
    }
}

/// An uploaded part (thumbnails must be uploads: `force_file`).
pub fn upload_only(ctx: &BotCtx, input: Option<&InputFile>) -> Option<Source> {
    match input {
        Some(InputFile::Attach(name)) => attached(ctx, name),
        _ => None,
    }
}

fn attached(ctx: &BotCtx, name: &str) -> Option<Source> {
    let upload = ctx.uploads.get(name)?;
    Some(Source::Upload {
        name: upload.file_name.clone().unwrap_or_default(),
        data: upload.bytes.clone(),
    })
}

impl Source {
    /// The TL form: uploads (and URLs, fetched) go up in parts; `file_id`s are decoded
    /// and must be of a kind usable as `expected`.
    pub async fn into_tl(self, ctx: &BotCtx, expected: FileType) -> BotResult<TlFile> {
        match self {
            Source::Upload { name, data } => {
                let file = upload(ctx, &name, &data).await?;
                Ok(TlFile::Uploaded { file, name })
            }
            Source::Reference(url) if url.contains('.') => {
                let (name, data) = fetch::get(&url)
                    .await
                    .map_err(|_| BotError::bad_request("failed to get HTTP URL content"))?;
                let file = upload(ctx, &name, &data).await?;
                Ok(TlFile::Uploaded { file, name })
            }
            Source::Reference(file_id) => {
                let id = FileId::decode(&file_id).map_err(|e| botapi::format(400, &e))?;
                id.check_type(expected)
                    .map_err(|e| botapi::format(400, &e))?;
                Ok(TlFile::Remote(id))
            }
        }
    }

    /// A thumbnail: only uploads count; anything else is ignored, as tdlib does.
    pub async fn into_thumbnail(self, ctx: &BotCtx) -> BotResult<Option<enums::InputFile>> {
        match self {
            Source::Upload { name, data } if !data.is_empty() => {
                Ok(Some(upload(ctx, &name, &data).await?))
            }
            _ => Ok(None),
        }
    }
}

impl FileId {
    pub fn input_photo(&self) -> enums::InputPhoto {
        types::InputPhoto {
            id: self.id,
            access_hash: self.access_hash,
            file_reference: self.file_reference.clone(),
        }
        .into()
    }

    pub fn input_document(&self) -> enums::InputDocument {
        types::InputDocument {
            id: self.id,
            access_hash: self.access_hash,
            file_reference: self.file_reference.clone(),
        }
        .into()
    }

    /// Where `upload.getFile` reads the file.
    pub fn location(&self) -> enums::InputFileLocation {
        if self.file_type.is_photo() && self.file_type != FileType::Thumbnail {
            return types::InputPhotoFileLocation {
                id: self.id,
                access_hash: self.access_hash,
                file_reference: self.file_reference.clone(),
                thumb_size: self.size.map(String::from).unwrap_or_default(),
            }
            .into();
        }
        types::InputDocumentFileLocation {
            id: self.id,
            access_hash: self.access_hash,
            file_reference: self.file_reference.clone(),
            thumb_size: self.size.map(String::from).unwrap_or_default(),
        }
        .into()
    }
}

/// Upload `data` as the bot, in parts.
pub async fn upload(ctx: &BotCtx, name: &str, data: &[u8]) -> BotResult<enums::InputFile> {
    if data.is_empty() {
        return Err(BotError::bad_request("file must be non-empty"));
    }
    let id = rand::random::<i64>();
    let parts = data.len().div_ceil(PART_SIZE) as i32;
    let big = data.len() > BIG_FILE_SIZE;
    for (part, bytes) in data.chunks(PART_SIZE).enumerate() {
        let part = part as i32;
        if big {
            ctx.call(
                functions::upload::SaveBigFilePart {
                    file_id: id,
                    file_part: part,
                    file_total_parts: parts,
                    bytes: bytes.to_vec(),
                },
                ErrorContext::Plain,
            )
            .await?;
        } else {
            ctx.call(
                functions::upload::SaveFilePart {
                    file_id: id,
                    file_part: part,
                    bytes: bytes.to_vec(),
                },
                ErrorContext::Plain,
            )
            .await?;
        }
    }
    Ok(if big {
        types::InputFileBig {
            id,
            parts,
            name: name.to_owned(),
        }
        .into()
    } else {
        types::InputFile {
            id,
            parts,
            name: name.to_owned(),
            md5_checksum: String::new(),
        }
        .into()
    })
}
