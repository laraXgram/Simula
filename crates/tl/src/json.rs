//! Serde adapters for JSON-TL. TL `long` is written as a string (JavaScript numbers lose
//! precision past 2^53) and accepted as either a string or a number; `bytes`, `int128`
//! and `int256` are standard base64. Generated fields pick the adapter by shape.

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde::de::{Error, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A `long` in JSON: string on output, string or integer on input.
#[derive(Clone, Copy)]
struct Long(i64);

impl Serialize for Long {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Long {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct LongVisitor;
        impl Visitor<'_> for LongVisitor {
            type Value = Long;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a 64-bit integer as a string or number")
            }
            fn visit_i64<E: Error>(self, v: i64) -> Result<Long, E> {
                Ok(Long(v))
            }
            fn visit_u64<E: Error>(self, v: u64) -> Result<Long, E> {
                i64::try_from(v).map(Long).map_err(E::custom)
            }
            fn visit_str<E: Error>(self, v: &str) -> Result<Long, E> {
                v.parse().map(Long).map_err(E::custom)
            }
        }
        d.deserialize_any(LongVisitor)
    }
}

/// Raw bytes in JSON, as base64.
struct Base64(Vec<u8>);

impl Serialize for Base64 {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&STANDARD.encode(&self.0))
    }
}

impl<'de> Deserialize<'de> for Base64 {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let text = String::deserialize(d)?;
        STANDARD.decode(text).map(Base64).map_err(D::Error::custom)
    }
}

pub mod long {
    use super::*;
    pub fn serialize<S: Serializer>(v: &i64, s: S) -> Result<S::Ok, S::Error> {
        Long(*v).serialize(s)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
        Long::deserialize(d).map(|l| l.0)
    }
}

pub mod opt_long {
    use super::*;
    pub fn serialize<S: Serializer>(v: &Option<i64>, s: S) -> Result<S::Ok, S::Error> {
        v.map(Long).serialize(s)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i64>, D::Error> {
        Ok(Option::<Long>::deserialize(d)?.map(|l| l.0))
    }
}

pub mod vec_long {
    use super::*;
    pub fn serialize<S: Serializer>(v: &[i64], s: S) -> Result<S::Ok, S::Error> {
        s.collect_seq(v.iter().map(|x| Long(*x)))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<i64>, D::Error> {
        Ok(Vec::<Long>::deserialize(d)?.into_iter().map(|l| l.0).collect())
    }
}

pub mod opt_vec_long {
    use super::*;
    pub fn serialize<S: Serializer>(v: &Option<Vec<i64>>, s: S) -> Result<S::Ok, S::Error> {
        v.as_ref()
            .map(|items| items.iter().map(|x| Long(*x)).collect::<Vec<_>>())
            .serialize(s)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<i64>>, D::Error> {
        Ok(Option::<Vec<Long>>::deserialize(d)?.map(|v| v.into_iter().map(|l| l.0).collect()))
    }
}

pub mod bytes {
    use super::*;
    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&STANDARD.encode(v))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        Base64::deserialize(d).map(|b| b.0)
    }
}

pub mod opt_bytes {
    use super::*;
    pub fn serialize<S: Serializer>(v: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        v.as_ref().map(|b| STANDARD.encode(b)).serialize(s)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<u8>>, D::Error> {
        Ok(Option::<Base64>::deserialize(d)?.map(|b| b.0))
    }
}

pub mod vec_bytes {
    use super::*;
    pub fn serialize<S: Serializer>(v: &[Vec<u8>], s: S) -> Result<S::Ok, S::Error> {
        s.collect_seq(v.iter().map(|b| STANDARD.encode(b)))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Vec<u8>>, D::Error> {
        Ok(Vec::<Base64>::deserialize(d)?.into_iter().map(|b| b.0).collect())
    }
}

pub mod opt_vec_bytes {
    use super::*;
    pub fn serialize<S: Serializer>(v: &Option<Vec<Vec<u8>>>, s: S) -> Result<S::Ok, S::Error> {
        v.as_ref()
            .map(|items| items.iter().map(|b| STANDARD.encode(b)).collect::<Vec<_>>())
            .serialize(s)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<Vec<u8>>>, D::Error> {
        Ok(Option::<Vec<Base64>>::deserialize(d)?.map(|v| v.into_iter().map(|b| b.0).collect()))
    }
}

/// `int128` / `int256` as base64.
pub mod fixed {
    use super::*;
    pub fn serialize<S: Serializer, const N: usize>(v: &[u8; N], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&STANDARD.encode(v))
    }
    pub fn deserialize<'de, D: Deserializer<'de>, const N: usize>(
        d: D,
    ) -> Result<[u8; N], D::Error> {
        let bytes = Base64::deserialize(d)?.0;
        bytes
            .try_into()
            .map_err(|b: Vec<u8>| D::Error::custom(format!("expected {N} bytes, got {}", b.len())))
    }
}

pub mod opt_fixed {
    use super::*;
    pub fn serialize<S: Serializer, const N: usize>(
        v: &Option<[u8; N]>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        v.as_ref().map(|b| STANDARD.encode(b)).serialize(s)
    }
    pub fn deserialize<'de, D: Deserializer<'de>, const N: usize>(
        d: D,
    ) -> Result<Option<[u8; N]>, D::Error> {
        Option::<Base64>::deserialize(d)?
            .map(|b| {
                b.0.try_into().map_err(|b: Vec<u8>| {
                    D::Error::custom(format!("expected {N} bytes, got {}", b.len()))
                })
            })
            .transpose()
    }
}
