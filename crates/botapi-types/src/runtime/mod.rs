//! Handwritten support code for the generated Bot API types.

mod chat_id;
mod input_file;
mod method;
pub mod shorthand;
mod tag;
pub mod validate;

pub use chat_id::ChatId;
pub use input_file::InputFile;
pub use method::{Method, ParamKind, ParamSpec};
pub use shorthand::StringShorthand;

/// Registry entry for one method (see `registry::METHODS`).
#[derive(Debug, Clone, Copy)]
pub struct MethodInfo {
    pub name: &'static str,
    pub params: &'static [ParamSpec],
    pub multipart: bool,
}
