//! Telegram Bot API types for Simula, generated from `spec/botapi/api.json`.
//! Regenerate with `bun run codegen rust:botapi`.
//!
//! - [`types`]: every object and union, serializing exactly as Telegram does
//! - [`methods`]: one parameter struct per method, implementing [`Method`] and [`Validate`]
//! - [`registry`]: every method's parameter metadata, for request decoding
//! - [`tags`]: the documented constants that discriminate union variants

mod generated;
mod runtime;

pub use generated::{methods, registry, tags, types};
pub use runtime::validate::{self, Rule, Validate, Violation};
pub use runtime::{ChatId, InputFile, Method, MethodInfo, ParamKind, ParamSpec};
