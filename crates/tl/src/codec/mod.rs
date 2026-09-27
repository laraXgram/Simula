//! TL binary serialization: traits plus the primitive encodings from the MTProto spec
//! (<https://core.telegram.org/mtproto/serialize>). Generated code builds on these.

mod primitives;
mod reader;

pub use primitives::{read_bare_vector, write_bare_vector};
pub use reader::Reader;

/// Constructor id of the boxed `Vector t` type.
pub const VECTOR_ID: u32 = 0x1cb5_c415;
pub const BOOL_TRUE_ID: u32 = 0x9972_75b5;
pub const BOOL_FALSE_ID: u32 = 0xbc79_9737;

/// Appends the TL encoding of a value. Structs encode bare (no constructor id);
/// enums (boxed types) prefix their constructor id.
pub trait Serializable {
    fn serialize(&self, buf: &mut Vec<u8>);

    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.serialize(&mut buf);
        buf
    }
}

pub trait Deserializable: Sized {
    fn deserialize(reader: &mut Reader<'_>) -> Result<Self, DecodeError>;

    /// Decode a complete buffer, rejecting trailing bytes.
    fn from_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut reader = Reader::new(bytes);
        let value = Self::deserialize(&mut reader)?;
        match reader.remaining() {
            0 => Ok(value),
            left => Err(DecodeError::TrailingBytes(left)),
        }
    }
}

/// A TL constructor or function with a fixed constructor id.
pub trait Identifiable {
    const CONSTRUCTOR_ID: u32;
    /// The name as written in the schema, e.g. `messages.sendMessage`.
    const NAME: &'static str;
}

/// A TL function: serializes as `id + params` and names its result type.
pub trait RemoteCall: Serializable {
    type Return: Deserializable + Serializable;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    UnexpectedEof,
    UnexpectedConstructor { id: u32, expected: &'static str },
    InvalidUtf8,
    TrailingBytes(usize),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnexpectedEof => f.write_str("unexpected end of input"),
            Self::UnexpectedConstructor { id, expected } => {
                write!(f, "unexpected constructor {id:#010x} while reading {expected}")
            }
            Self::InvalidUtf8 => f.write_str("string is not valid UTF-8"),
            Self::TrailingBytes(n) => write!(f, "{n} trailing bytes after value"),
        }
    }
}

impl std::error::Error for DecodeError {}
