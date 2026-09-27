use serde::de::{Error, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// `Integer or String` in the docs: a numeric chat id or an `@username`.
/// Numeric strings (`"123"`, as sent by form-encoded requests) are ids.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ChatId {
    Id(i64),
    Username(String),
}

impl Default for ChatId {
    fn default() -> Self {
        Self::Id(0)
    }
}

impl From<i64> for ChatId {
    fn from(id: i64) -> Self {
        Self::Id(id)
    }
}

impl Serialize for ChatId {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Id(id) => s.serialize_i64(*id),
            Self::Username(name) => s.serialize_str(name),
        }
    }
}

impl<'de> Deserialize<'de> for ChatId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct ChatIdVisitor;
        impl Visitor<'_> for ChatIdVisitor {
            type Value = ChatId;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("an integer chat id or a username")
            }
            fn visit_i64<E: Error>(self, v: i64) -> Result<ChatId, E> {
                Ok(ChatId::Id(v))
            }
            fn visit_u64<E: Error>(self, v: u64) -> Result<ChatId, E> {
                i64::try_from(v).map(ChatId::Id).map_err(E::custom)
            }
            fn visit_f64<E: Error>(self, v: f64) -> Result<ChatId, E> {
                if v.fract() == 0.0 && v.abs() < 9.0e15 {
                    Ok(ChatId::Id(v as i64))
                } else {
                    Err(E::custom("chat id is not an integer"))
                }
            }
            fn visit_str<E: Error>(self, v: &str) -> Result<ChatId, E> {
                Ok(v.trim()
                    .parse()
                    .map(ChatId::Id)
                    .unwrap_or_else(|_| ChatId::Username(v.to_owned())))
            }
        }
        d.deserialize_any(ChatIdVisitor)
    }
}
