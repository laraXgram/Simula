use serde::Serialize;
use serde::de::DeserializeOwned;

use super::validate::Validate;

/// A Bot API method's parameter struct.
pub trait Method: Serialize + DeserializeOwned + Validate {
    /// Name as documented, e.g. `sendMessage`.
    const NAME: &'static str;
    /// Parameter metadata, in docs order.
    const PARAMS: &'static [ParamSpec];
    /// Whether the method accepts multipart uploads.
    const MULTIPART: bool;
    type Return: Serialize + DeserializeOwned;
}

/// How a parameter is sent on the wire. Form-encoded and multipart requests carry every
/// value as a string; the gateway uses this to coerce them into typed JSON first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamKind {
    Integer,
    Float,
    String,
    Boolean,
    /// `Integer or String` (chat ids).
    IntegerOrString,
    /// `InputFile or String`.
    InputFile,
    /// Objects and arrays: sent JSON-serialized.
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParamSpec {
    pub name: &'static str,
    pub kind: ParamKind,
    pub required: bool,
}
