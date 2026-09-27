use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A file parameter (`InputFile or String` in the docs).
///
/// The gateway turns a multipart part sent directly as a parameter (e.g. `photo=<file>`)
/// into `attach://photo`, so every upload arrives here as [`InputFile::Attach`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum InputFile {
    /// `attach://<name>`: a multipart part with that field name.
    Attach(String),
    /// A `file_id` already known to the server, or an HTTP(S) URL to fetch.
    Reference(String),
}

impl InputFile {
    const ATTACH: &'static str = "attach://";
}

impl Serialize for InputFile {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Attach(name) => s.collect_str(&format_args!("{}{name}", Self::ATTACH)),
            Self::Reference(value) => s.serialize_str(value),
        }
    }
}

impl<'de> Deserialize<'de> for InputFile {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = String::deserialize(d)?;
        Ok(match value.strip_prefix(Self::ATTACH) {
            Some(name) => Self::Attach(name.to_owned()),
            None => Self::Reference(value),
        })
    }
}

impl Default for InputFile {
    fn default() -> Self {
        Self::Reference(String::new())
    }
}
