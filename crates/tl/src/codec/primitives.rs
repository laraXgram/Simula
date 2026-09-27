use super::{BOOL_FALSE_ID, BOOL_TRUE_ID, DecodeError, Deserializable, Reader, Serializable, VECTOR_ID};

// int / long / double: little-endian, fixed width.

impl Serializable for i32 {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_le_bytes());
    }
}

impl Deserializable for i32 {
    fn deserialize(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(i32::from_le_bytes(reader.read_array()?))
    }
}

/// Flags fields and constructor ids.
impl Serializable for u32 {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_le_bytes());
    }
}

impl Deserializable for u32 {
    fn deserialize(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        reader.read_u32()
    }
}

impl Serializable for i64 {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_le_bytes());
    }
}

impl Deserializable for i64 {
    fn deserialize(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(i64::from_le_bytes(reader.read_array()?))
    }
}

impl Serializable for f64 {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_le_bytes());
    }
}

impl Deserializable for f64 {
    fn deserialize(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(f64::from_le_bytes(reader.read_array()?))
    }
}

// int128 / int256: raw bytes.

impl<const N: usize> Serializable for [u8; N] {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self);
    }
}

impl<const N: usize> Deserializable for [u8; N] {
    fn deserialize(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        reader.read_array()
    }
}

/// Boxed `Bool`: `boolTrue` / `boolFalse` constructors.
impl Serializable for bool {
    fn serialize(&self, buf: &mut Vec<u8>) {
        (if *self { BOOL_TRUE_ID } else { BOOL_FALSE_ID }).serialize(buf);
    }
}

impl Deserializable for bool {
    fn deserialize(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        match reader.read_u32()? {
            BOOL_TRUE_ID => Ok(true),
            BOOL_FALSE_ID => Ok(false),
            id => Err(DecodeError::UnexpectedConstructor { id, expected: "Bool" }),
        }
    }
}

// bytes / string: length-prefixed, padded to a multiple of 4.
// Lengths up to 253 use one byte; longer ones use 0xfe followed by 3 bytes.

impl Serializable for [u8] {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let len = self.len();
        let header = if len <= 253 {
            buf.push(len as u8);
            1
        } else {
            buf.push(254);
            buf.extend_from_slice(&(len as u32).to_le_bytes()[..3]);
            4
        };
        buf.extend_from_slice(self);
        let padding = (4 - (header + len) % 4) % 4;
        buf.extend(std::iter::repeat_n(0, padding));
    }
}

impl Serializable for Vec<u8> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.as_slice().serialize(buf);
    }
}

impl Deserializable for Vec<u8> {
    fn deserialize(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let first = reader.read_bytes(1)?[0];
        let (header, len) = if first <= 253 {
            (1, first as usize)
        } else {
            let b = reader.read_bytes(3)?;
            (4, u32::from_le_bytes([b[0], b[1], b[2], 0]) as usize)
        };
        let data = reader.read_bytes(len)?.to_vec();
        reader.read_bytes((4 - (header + len) % 4) % 4)?;
        Ok(data)
    }
}

impl Serializable for String {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.as_bytes().serialize(buf);
    }
}

impl Deserializable for String {
    fn deserialize(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        String::from_utf8(Vec::<u8>::deserialize(reader)?).map_err(|_| DecodeError::InvalidUtf8)
    }
}

// Boxed `Vector<T>`: constructor id, count, items. (Vec<u8> is `bytes`, see above.)

impl<T: Serializable> Serializable for Vec<T> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        VECTOR_ID.serialize(buf);
        write_bare_vector(buf, self);
    }
}

impl<T: Deserializable> Deserializable for Vec<T> {
    fn deserialize(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        match reader.read_u32()? {
            VECTOR_ID => read_bare_vector(reader),
            id => Err(DecodeError::UnexpectedConstructor { id, expected: "Vector" }),
        }
    }
}

/// Boxed values (generic `!X` queries, recursive payloads) encode like their contents.
impl<T: Serializable + ?Sized> Serializable for Box<T> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        (**self).serialize(buf);
    }
}

impl<T: Deserializable> Deserializable for Box<T> {
    fn deserialize(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        T::deserialize(reader).map(Box::new)
    }
}

/// Bare `vector<T>`: count and items, no constructor id.
pub fn write_bare_vector<T: Serializable>(buf: &mut Vec<u8>, items: &[T]) {
    (items.len() as i32).serialize(buf);
    for item in items {
        item.serialize(buf);
    }
}

pub fn read_bare_vector<T: Deserializable>(reader: &mut Reader<'_>) -> Result<Vec<T>, DecodeError> {
    let len = i32::deserialize(reader)?;
    let len = usize::try_from(len).map_err(|_| DecodeError::UnexpectedEof)?;
    // Every TL value takes at least 4 bytes; refuse counts the input can't hold.
    if len > reader.remaining() / 4 {
        return Err(DecodeError::UnexpectedEof);
    }
    (0..len).map(|_| T::deserialize(reader)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_string_is_padded_to_four_bytes() {
        assert_eq!("abc".to_string().to_bytes(), [3, b'a', b'b', b'c']);
        assert_eq!("abcd".to_string().to_bytes(), [4, b'a', b'b', b'c', b'd', 0, 0, 0]);
        assert_eq!(String::new().to_bytes(), [0, 0, 0, 0]);
    }

    #[test]
    fn long_bytes_use_four_byte_header() {
        let data = vec![7u8; 300];
        let encoded = data.to_bytes();
        assert_eq!(&encoded[..4], &[254, 0x2c, 0x01, 0x00]);
        assert_eq!(encoded.len(), 304);
        assert_eq!(Vec::<u8>::from_bytes(&encoded).unwrap(), data);
    }

    #[test]
    fn length_253_is_still_short_form() {
        let data = vec![1u8; 253];
        let encoded = data.to_bytes();
        assert_eq!(encoded[0], 253);
        assert_eq!(encoded.len(), 256);
        assert_eq!(Vec::<u8>::from_bytes(&encoded).unwrap(), data);
    }

    #[test]
    fn bool_uses_constructor_ids() {
        assert_eq!(true.to_bytes(), 0x9972_75b5u32.to_le_bytes());
        assert_eq!(false.to_bytes(), 0xbc79_9737u32.to_le_bytes());
    }

    #[test]
    fn boxed_vector_of_longs() {
        let encoded = vec![1i64, -1].to_bytes();
        assert_eq!(&encoded[..4], &VECTOR_ID.to_le_bytes());
        assert_eq!(&encoded[4..8], &2i32.to_le_bytes());
        assert_eq!(Vec::<i64>::from_bytes(&encoded).unwrap(), [1, -1]);
    }

    #[test]
    fn rejects_truncated_input_and_absurd_counts() {
        assert_eq!(String::from_bytes(&[5, b'a']), Err(DecodeError::UnexpectedEof));
        let mut huge = VECTOR_ID.to_le_bytes().to_vec();
        huge.extend_from_slice(&i32::MAX.to_le_bytes());
        assert_eq!(Vec::<i32>::from_bytes(&huge), Err(DecodeError::UnexpectedEof));
    }
}
