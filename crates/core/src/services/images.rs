//! Image processing for photos and thumbnails. Like Telegram, a photo is decoded and
//! stored re-encoded as JPEG in several sizes: each box (`s` 90, `m` 320, `x` 800,
//! `y` 1280, `w` 2560 pixels) the photo is larger than gets a scaled-down copy, and the
//! first box it fits in gets the photo at its own size; smaller boxes are not made.

use image::{DynamicImage, codecs::jpeg::JpegEncoder};

use crate::error::{RpcError, RpcResult};

const BOXES: [(&str, u32); 5] = [("s", 90), ("m", 320), ("x", 800), ("y", 1280), ("w", 2560)];
const JPEG_QUALITY: u8 = 87;
/// Thumbnails of documents and videos fit this box (Telegram's `m`).
pub const THUMB_BOX: u32 = 320;

/// One stored size of a photo.
#[derive(Debug, Clone)]
pub struct Size {
    pub kind: &'static str,
    pub w: u32,
    pub h: u32,
    pub jpeg: Vec<u8>,
}

/// Whether `data` is an image this server can read.
pub fn is_image(data: &[u8]) -> bool {
    image::guess_format(data).is_ok() && image::load_from_memory(data).is_ok()
}

/// A photo's sizes, smallest first. The server's checks: the image must decode
/// (`IMAGE_PROCESS_FAILED`), and its sides must add up to at most 10000 pixels with a
/// ratio of at most 20 (`PHOTO_INVALID_DIMENSIONS`).
pub fn photo_sizes(data: &[u8]) -> RpcResult<Vec<Size>> {
    let image = decode(data)?;
    let (w, h) = (image.width(), image.height());
    if w == 0 || h == 0 || w + h > 10_000 || w.max(h) > 20 * w.min(h) {
        return Err(RpcError::bad_request("PHOTO_INVALID_DIMENSIONS"));
    }
    let mut sizes = Vec::new();
    for (kind, side) in BOXES {
        if w.max(h) <= side {
            sizes.push(encode(kind, &image)?);
            break;
        }
        sizes.push(encode(kind, &image.thumbnail(side, side))?);
    }
    Ok(sizes)
}

/// A thumbnail fitting [`THUMB_BOX`] (never enlarged), as the `m` size.
pub fn thumbnail(data: &[u8]) -> RpcResult<Size> {
    let image = decode(data)?;
    let image = if image.width().max(image.height()) > THUMB_BOX {
        image.thumbnail(THUMB_BOX, THUMB_BOX)
    } else {
        image
    };
    encode("m", &image)
}

/// The image's width and height, if it is one.
pub fn dimensions(data: &[u8]) -> Option<(u32, u32)> {
    image::load_from_memory(data)
        .ok()
        .map(|image| (image.width(), image.height()))
}

fn decode(data: &[u8]) -> RpcResult<DynamicImage> {
    image::load_from_memory(data).map_err(|_| RpcError::bad_request("IMAGE_PROCESS_FAILED"))
}

fn encode(kind: &'static str, image: &DynamicImage) -> RpcResult<Size> {
    let rgb = DynamicImage::ImageRgb8(image.to_rgb8());
    let mut jpeg = Vec::new();
    JpegEncoder::new_with_quality(&mut jpeg, JPEG_QUALITY)
        .encode_image(&rgb)
        .map_err(RpcError::internal)?;
    Ok(Size {
        kind,
        w: rgb.width(),
        h: rgb.height(),
        jpeg,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn png(w: u32, h: u32) -> Vec<u8> {
        let image = DynamicImage::new_rgb8(w, h);
        let mut out = std::io::Cursor::new(Vec::new());
        image.write_to(&mut out, image::ImageFormat::Png).unwrap();
        out.into_inner()
    }

    #[test]
    fn sizes_stop_at_the_first_box_the_photo_fits() {
        let sizes = photo_sizes(&png(1000, 500)).unwrap();
        let got: Vec<_> = sizes.iter().map(|s| (s.kind, s.w, s.h)).collect();
        assert_eq!(
            got,
            vec![
                ("s", 90, 45),
                ("m", 320, 160),
                ("x", 800, 400),
                ("y", 1000, 500)
            ]
        );
        let small = photo_sizes(&png(60, 40)).unwrap();
        assert_eq!(
            small.iter().map(|s| (s.kind, s.w)).collect::<Vec<_>>(),
            vec![("s", 60)]
        );
    }

    #[test]
    fn the_servers_checks() {
        assert_eq!(
            photo_sizes(b"not an image").unwrap_err().message,
            "IMAGE_PROCESS_FAILED"
        );
        assert_eq!(
            photo_sizes(&png(2100, 100)).unwrap_err().message,
            "PHOTO_INVALID_DIMENSIONS"
        );
        assert_eq!(thumbnail(&png(640, 480)).unwrap().w, 320);
    }
}
