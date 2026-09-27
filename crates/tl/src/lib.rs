//! MTProto TL layer for Simula: types generated from `spec/tl/*.json` plus the binary and
//! JSON-TL codecs they use. Regenerate with `bun run codegen rust:tl`.
//!
//! - [`types`]: one struct per constructor (bare encoding)
//! - [`enums`]: one enum per boxed type (constructor id + payload)
//! - [`functions`]: one struct per function, implementing [`RemoteCall`]
//! - [`Function`]: every function, for decoding incoming calls
//! - [`mtproto`]: the same for the transport-level `mtproto_api.tl`

pub mod codec;
mod generated;
mod json;

pub use codec::{DecodeError, Deserializable, Identifiable, Reader, RemoteCall, Serializable};
pub use generated::api::{Function, LAYER, enums, functions, types};

/// Transport-level schema (`mtproto_api.tl`): auth key exchange, acks, salts, pings.
pub mod mtproto {
    pub use crate::generated::mtproto::{Function, enums, functions, types};
}
