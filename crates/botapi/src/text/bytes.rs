//! Byte-level helpers shared by the parsers, mirroring tdlib's C-string reads: reading
//! past the end yields `0`, like the terminating NUL of a `std::string`.

pub fn at(text: &[u8], i: usize) -> u8 {
    text.get(i).copied().unwrap_or(0)
}

/// UTF-16 units contributed by a UTF-8 byte: 1 for a lead byte, 2 for 4-byte sequences.
pub fn utf16_units(byte: u8) -> i32 {
    if byte & 0xc0 == 0x80 {
        0
    } else {
        1 + i32::from(byte >= 0xf0)
    }
}

/// tdlib's `is_space` (NUL included, which stops scans at the end of the text).
pub fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\r' | b'\n' | 0 | 0x0b)
}

pub fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

pub fn push_char(out: &mut Vec<u8>, code: u32) {
    if let Some(c) = char::from_u32(code) {
        let mut buf = [0; 4];
        out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
    }
}
