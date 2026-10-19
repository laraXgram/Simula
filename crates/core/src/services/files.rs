//! Uploads and downloads. Clients upload a file in parts (`upload.saveFilePart`, or
//! `saveBigFilePart` for files over 10 MB) under a random id, then name it in a call
//! with an `InputFile`; the parts become one stored file. Downloads read stored files in
//! chunks (`upload.getFile`).

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::UserId;
use crate::store;

/// Every file lives in one data center, like a small Telegram.
pub const DC_ID: i32 = 2;
const MAX_PART_SIZE: usize = 512 * 1024;
const MAX_PARTS: i32 = 4000;
/// `upload.getFile` reads at most 1 MB at a time.
const MAX_DOWNLOAD_CHUNK: i32 = 1024 * 1024;

/// An uploaded file, put together from its parts.
#[derive(Debug, Clone)]
pub struct Upload {
    pub name: String,
    pub data: Vec<u8>,
}

/// Keep one part of an upload (`total` for big files).
pub fn save_part(
    ctx: &Ctx<'_>,
    owner: UserId,
    file_id: i64,
    part: i32,
    total: Option<i32>,
    data: &[u8],
) -> RpcResult<()> {
    if data.is_empty() {
        return Err(RpcError::bad_request("FILE_PART_EMPTY"));
    }
    if data.len() > MAX_PART_SIZE {
        return Err(RpcError::bad_request("FILE_PART_TOO_BIG"));
    }
    if !(0..MAX_PARTS).contains(&part) {
        return Err(RpcError::bad_request("FILE_PART_INVALID"));
    }
    if total.is_some_and(|t| !(1..=MAX_PARTS).contains(&t) || part >= t) {
        return Err(RpcError::bad_request("FILE_PARTS_INVALID"));
    }
    store::files::save_part(ctx.db, owner, file_id, part, total, data)?;
    Ok(())
}

/// The file an `InputFile` names, from the parts `owner` uploaded (which are used up).
pub fn take_upload(ctx: &Ctx<'_>, owner: UserId, input: &enums::InputFile) -> RpcResult<Upload> {
    let (id, parts, name) = match input {
        enums::InputFile::InputFile(f) => (f.id, f.parts, f.name.clone()),
        enums::InputFile::Big(f) => (f.id, f.parts, f.name.clone()),
        _ => return Err(RpcError::not_implemented("story documents as files")),
    };
    if !(1..=MAX_PARTS).contains(&parts) {
        return Err(RpcError::bad_request("FILE_PARTS_INVALID"));
    }
    let stored = store::files::parts(ctx.db, owner, id)?;
    let mut data = Vec::new();
    for part in 0..parts {
        let Some((_, bytes)) = stored.iter().find(|(p, _)| *p == part) else {
            return Err(RpcError::bad_request(format!("FILE_PART_{part}_MISSING")));
        };
        data.extend_from_slice(bytes);
    }
    store::files::clear_parts(ctx.db, owner, id)?;
    Ok(Upload { name, data })
}

/// A new photo or document id: random and positive, like Telegram's.
pub fn new_id() -> i64 {
    rand::random::<i64>() & i64::MAX
}

/// A file reference for a new photo or document. Simula accepts any reference.
pub fn file_reference() -> Vec<u8> {
    let mut reference = vec![1];
    reference.extend_from_slice(&rand::random::<[u8; 20]>());
    reference
}

/// `limit` bytes of a file from `offset`. The server's rules: the offset is a multiple
/// of 4 KB (1 KB when `precise`), the limit too and at most 1 MB, dividing 1 MB unless
/// `precise`.
pub fn download(
    ctx: &Ctx<'_>,
    location: &enums::InputFileLocation,
    offset: i64,
    limit: i32,
    precise: bool,
) -> RpcResult<types::upload::File> {
    let (id, access_hash, thumb) = match location {
        enums::InputFileLocation::InputPhotoFileLocation(l) => {
            (l.id, l.access_hash, l.thumb_size.as_str())
        }
        enums::InputFileLocation::InputDocumentFileLocation(l) => {
            (l.id, l.access_hash, l.thumb_size.as_str())
        }
        _ => return Err(RpcError::not_implemented("downloading this kind of file")),
    };
    let unit = if precise { 1024 } else { 4096 };
    if offset < 0 || offset % unit != 0 {
        return Err(RpcError::bad_request("OFFSET_INVALID"));
    }
    let valid_limit = limit > 0
        && limit <= MAX_DOWNLOAD_CHUNK
        && i64::from(limit) % unit == 0
        && (precise || MAX_DOWNLOAD_CHUNK % limit == 0);
    if !valid_limit {
        return Err(RpcError::bad_request("LIMIT_INVALID"));
    }
    let media = store::files::media(ctx.db, id)?
        .filter(|m| m.access_hash == access_hash)
        .ok_or_else(|| RpcError::bad_request("LOCATION_INVALID"))?;
    let photo = matches!(
        location,
        enums::InputFileLocation::InputPhotoFileLocation(_)
    );
    if media.photo != photo {
        return Err(RpcError::bad_request("LOCATION_INVALID"));
    }
    let blob = store::files::media_file(ctx.db, id, thumb)?
        .ok_or_else(|| RpcError::bad_request("LOCATION_INVALID"))?;
    let bytes = store::files::blob_range(ctx.db, blob, offset, i64::from(limit))?;
    let r#type = if photo || !thumb.is_empty() {
        enums::storage::FileType::FileJpeg
    } else {
        enums::storage::FileType::FilePartial
    };
    Ok(types::upload::File {
        r#type,
        mtime: media.date,
        bytes,
    })
}
