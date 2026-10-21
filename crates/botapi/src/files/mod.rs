//! Files as the Bot API sees them: `file_id`s, the files of a request (uploads,
//! `file_id`s, URLs) turned into TL inputs, and downloads.

pub mod download;
pub mod fetch;
pub mod id;
pub mod input;
pub mod mime;
