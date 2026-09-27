//! Objects that also accept a bare string, e.g. `KeyboardButton`: "For simple text
//! buttons, String can be used instead of this object to specify the button text."

use std::fmt;
use std::marker::PhantomData;

use serde::Deserializer;
use serde::de::{MapAccess, Visitor, value::MapAccessDeserializer};

/// Implemented by generated types with a string shorthand.
pub trait StringShorthand: Sized {
    /// The object a bare string stands for.
    fn from_string(value: String) -> Self;
    /// The derived deserializer for the object form.
    fn deserialize_object<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error>;
}

pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: StringShorthand,
{
    struct StringOrObject<T>(PhantomData<T>);

    impl<'de, T: StringShorthand> Visitor<'de> for StringOrObject<T> {
        type Value = T;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a String or an Object")
        }

        fn visit_str<E>(self, value: &str) -> Result<T, E> {
            Ok(T::from_string(value.to_owned()))
        }

        fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<T, A::Error> {
            T::deserialize_object(MapAccessDeserializer::new(map))
        }
    }

    deserializer.deserialize_any(StringOrObject(PhantomData))
}
