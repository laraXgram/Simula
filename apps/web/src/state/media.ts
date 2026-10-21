/**
 * Reading Telegram's media objects: what kind a document is, which photo size to show,
 * where to download it from, and how to label it (chat previews, reply headers).
 */
import type { tl } from "@simula/schema";

export type DocumentKind =
  | "video"
  | "gif"
  | "round"
  | "voice"
  | "audio"
  | "sticker"
  | "image"
  | "file";

/** A document with what clients read from its attributes. */
export interface DocumentInfo {
  document: tl.document;
  kind: DocumentKind;
  name: string;
  size: number;
  mime: string;
  duration?: number;
  w?: number;
  h?: number;
  title?: string;
  performer?: string;
  waveform?: number[];
  /** The sticker's emoji. */
  emoji?: string;
}

export function documentInfo(document: tl.document, media?: tl.messageMediaDocument): DocumentInfo {
  const info: DocumentInfo = {
    document,
    kind: "file",
    name: "",
    size: Number(document.size),
    mime: document.mime_type,
  };
  let animated = false;
  let sticker = false;
  for (const attribute of document.attributes) {
    switch (attribute._) {
      case "documentAttributeFilename":
        info.name = attribute.file_name;
        break;
      case "documentAttributeImageSize":
        info.w = attribute.w;
        info.h = attribute.h;
        break;
      case "documentAttributeAnimated":
        animated = true;
        break;
      case "documentAttributeSticker":
        sticker = true;
        info.emoji = attribute.alt;
        break;
      case "documentAttributeVideo":
        info.w = attribute.w;
        info.h = attribute.h;
        info.duration = attribute.duration;
        info.kind = attribute.round_message ? "round" : "video";
        break;
      case "documentAttributeAudio":
        info.duration = attribute.duration;
        info.title = attribute.title;
        info.performer = attribute.performer;
        info.kind = attribute.voice ? "voice" : "audio";
        if (attribute.waveform) info.waveform = decodeWaveform(fromBase64(attribute.waveform));
        break;
      default:
        break;
    }
  }
  if (sticker) info.kind = "sticker";
  else if (animated) info.kind = "gif";
  else if (media?.round) info.kind = "round";
  else if (media?.voice) info.kind = "voice";
  else if (info.kind === "file" && /^image\/(jpeg|png|webp|gif)$/.test(info.mime) && info.w)
    info.kind = "image";
  return info;
}

/** What a message carries, for rendering. */
export type Content =
  | { kind: "photo"; photo: tl.photo; spoiler: boolean }
  | { kind: "document"; info: DocumentInfo; spoiler: boolean }
  | { kind: "other"; label: string };

export function contentOf(message: tl.message): Content | undefined {
  const media = message.media;
  if (!media || media._ === "messageMediaEmpty" || media._ === "messageMediaWebPage") return;
  if (media._ === "messageMediaPhoto")
    return media.photo?._ === "photo"
      ? { kind: "photo", photo: media.photo, spoiler: media.spoiler === true }
      : { kind: "other", label: "Photo expired" };
  if (media._ === "messageMediaDocument")
    return media.document?._ === "document"
      ? {
          kind: "document",
          info: documentInfo(media.document, media),
          spoiler: media.spoiler === true,
        }
      : { kind: "other", label: "File expired" };
  return { kind: "other", label: mediaLabel(message) ?? "Unsupported message" };
}

/** Photos and videos are shown inline and in the viewer; the rest as rows. */
export function isVisual(content: Content | undefined): boolean {
  return (
    content?.kind === "photo" ||
    (content?.kind === "document" && (content.info.kind === "video" || content.info.kind === "gif"))
  );
}

/** "Photo", "Voice message", a file's name …, for previews and reply headers. */
export function mediaLabel(message: tl.message): string | undefined {
  const media = message.media;
  switch (media?._) {
    case undefined:
    case "messageMediaEmpty":
    case "messageMediaWebPage":
      return undefined;
    case "messageMediaPhoto":
      return "Photo";
    case "messageMediaDocument": {
      if (media.document?._ !== "document") return "File";
      const info = documentInfo(media.document, media);
      switch (info.kind) {
        case "video":
          return "Video";
        case "gif":
          return "GIF";
        case "round":
          return "Video message";
        case "voice":
          return "Voice message";
        case "audio":
          return [info.performer, info.title].filter(Boolean).join(" – ") || info.name || "Audio";
        case "sticker":
          return `${info.emoji ?? ""} Sticker`.trim();
        default:
          return info.name || "File";
      }
    }
    case "messageMediaContact":
      return "Contact";
    case "messageMediaGeo":
    case "messageMediaGeoLive":
    case "messageMediaVenue":
      return "Location";
    case "messageMediaPoll":
      return "Poll";
    case "messageMediaDice":
      return media.emoticon;
    case "messageMediaInvoice":
      return "Invoice";
    case "messageMediaGame":
      return "Game";
    case "messageMediaStory":
      return "Story";
    default:
      return "Media";
  }
}

/** The photo size closest to `side` pixels on its longer edge (larger if none is). */
export function photoSize(photo: tl.photo, side: number): tl.photoSize | undefined {
  const sizes = photo.sizes
    .filter((s): s is tl.photoSize => s._ === "photoSize")
    .sort((a, b) => Math.max(a.w, a.h) - Math.max(b.w, b.h));
  return sizes.find((s) => Math.max(s.w, s.h) >= side) ?? sizes.at(-1);
}

export function largestPhotoSize(photo: tl.photo): tl.photoSize | undefined {
  return photoSize(photo, Number.POSITIVE_INFINITY);
}

export function photoLocation(photo: tl.photo, size: string): tl.inputPhotoFileLocation {
  return {
    _: "inputPhotoFileLocation",
    id: photo.id,
    access_hash: photo.access_hash,
    file_reference: photo.file_reference,
    thumb_size: size,
  };
}

/** A document's file, or one of its thumbnails. */
export function documentLocation(document: tl.document, thumb = ""): tl.inputDocumentFileLocation {
  return {
    _: "inputDocumentFileLocation",
    id: document.id,
    access_hash: document.access_hash,
    file_reference: document.file_reference,
    thumb_size: thumb,
  };
}

export function documentThumb(document: tl.document): tl.photoSize | undefined {
  return document.thumbs?.find((t): t is tl.photoSize => t._ === "photoSize");
}

/** "1.4 MB" */
export function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB"];
  let value = bytes / 1024;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit++;
  }
  return `${value < 10 ? value.toFixed(1) : Math.round(value)} ${units[unit]}`;
}

/** "0:07", "12:30", "1:02:03" */
export function formatDuration(seconds: number): string {
  const s = Math.max(0, Math.round(seconds));
  const two = (n: number) => String(n).padStart(2, "0");
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  return h > 0 ? `${h}:${two(m)}:${two(s % 60)}` : `${m}:${two(s % 60)}`;
}

/** Telegram's voice waveform: 5-bit values packed little-endian. */
export function decodeWaveform(bytes: Uint8Array): number[] {
  const count = Math.floor((bytes.length * 8) / 5);
  const values: number[] = [];
  for (let i = 0; i < count; i++) {
    const bit = i * 5;
    const byte = bit >> 3;
    const window = (bytes[byte] ?? 0) | ((bytes[byte + 1] ?? 0) << 8);
    values.push((window >> (bit & 7)) & 31);
  }
  return values;
}

export function encodeWaveform(values: readonly number[]): Uint8Array {
  const bytes = new Uint8Array(Math.ceil((values.length * 5) / 8));
  values.forEach((value, i) => {
    const bit = i * 5;
    const byte = bit >> 3;
    const shifted = (Math.max(0, Math.min(31, Math.round(value))) & 31) << (bit & 7);
    bytes[byte] |= shifted & 255;
    if (byte + 1 < bytes.length) bytes[byte + 1] |= shifted >> 8;
  });
  return bytes;
}

export function fromBase64(text: string): Uint8Array {
  const binary = atob(text);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

export function toBase64(bytes: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < bytes.length; i += 0x8000)
    binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  return btoa(binary);
}
