//! Macros for the generated constant types in `tags.rs`.

/// A unit type that serializes to one fixed string and rejects every other value.
#[macro_export]
macro_rules! string_tag {
    ($name:ident, $value:literal) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
        pub struct $name;

        impl $name {
            pub const VALUE: &'static str = $value;
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str($value)
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let value = <std::borrow::Cow<'de, str>>::deserialize(d)?;
                if value == $value {
                    Ok(Self)
                } else {
                    Err(serde::de::Error::custom(format_args!(
                        "expected \"{}\", got \"{value}\"",
                        $value
                    )))
                }
            }
        }
    };
}

/// A unit type that serializes to one fixed integer and rejects every other value.
#[macro_export]
macro_rules! int_tag {
    ($name:ident, $value:literal) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
        pub struct $name;

        impl $name {
            pub const VALUE: i64 = $value;
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_i64($value)
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let value = i64::deserialize(d)?;
                if value == $value {
                    Ok(Self)
                } else {
                    Err(serde::de::Error::custom(format_args!(
                        "expected {}, got {value}",
                        $value
                    )))
                }
            }
        }
    };
}
