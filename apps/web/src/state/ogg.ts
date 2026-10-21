/**
 * WebM/Opus to Ogg/Opus, for voice messages: Telegram's are Ogg/Opus, and browsers that
 * can't record Ogg record WebM. The Opus packets are copied as they are into Ogg pages
 * (RFC 7845); nothing is re-encoded.
 */

const SEGMENT = 0x18538067;
const CLUSTER = 0x1f43b675;
const TRACKS = 0x1654ae6b;
const TRACK_ENTRY = 0xae;
const BLOCK_GROUP = 0xa0;
const AUDIO = 0xe1;
const CODEC_ID = 0x86;
const CODEC_PRIVATE = 0x63a2;
const CHANNELS = 0x9f;
const SIMPLE_BLOCK = 0xa3;
const BLOCK = 0xa1;
/** Elements whose children are read in place (the rest are skipped). */
const MASTERS = new Set([SEGMENT, CLUSTER, TRACKS, TRACK_ENTRY, BLOCK_GROUP, AUDIO]);

interface Opus {
  head?: Uint8Array;
  channels: number;
  packets: Uint8Array[];
}

/** The Opus stream of a WebM file, or `undefined` if it holds something else. */
function readWebm(data: Uint8Array): Opus | undefined {
  const opus: Opus = { channels: 1, packets: [] };
  let codec = "";
  let at = 0;
  // A variable-length integer; `marker` keeps the length bit (element ids do).
  const vint = (marker: boolean): number | undefined => {
    const first = data[at];
    if (first === undefined || first === 0) return undefined;
    const length = Math.clz32(first) - 23;
    let value = marker ? first : first & (0xff >> length);
    let unknown = value === 0xff >> length;
    for (let i = 1; i < length; i++) {
      const byte = data[at + i] ?? 0;
      value = value * 256 + byte;
      if (byte !== 0xff) unknown = false;
    }
    at += length;
    return !marker && unknown ? Number.POSITIVE_INFINITY : value;
  };
  while (at < data.length) {
    const id = vint(true);
    const size = vint(false);
    if (id === undefined || size === undefined) break;
    if (MASTERS.has(id)) continue;
    const end = Math.min(data.length, at + size);
    const body = data.subarray(at, end);
    if (id === CODEC_ID) codec = new TextDecoder().decode(body);
    else if (id === CODEC_PRIVATE) opus.head = body.slice();
    else if (id === CHANNELS) opus.channels = body[0] ?? 1;
    else if (id === SIMPLE_BLOCK || id === BLOCK) {
      // Track number (a vint), a 16-bit timecode, flags, then the frame (no lacing).
      const track = Math.clz32(body[0] ?? 0x80) - 23;
      opus.packets.push(body.slice(track + 3));
    }
    at = end;
  }
  return codec === "A_OPUS" && opus.packets.length > 0 ? opus : undefined;
}

/** Samples (at 48 kHz) in an Opus packet, from its TOC byte (RFC 6716 § 3.1). */
function samples(packet: Uint8Array): number {
  const toc = packet[0] ?? 0;
  const config = toc >> 3;
  const ms =
    config < 12
      ? [10, 20, 40, 60][config % 4]
      : config < 16
        ? [10, 20][config % 2]
        : [2.5, 5, 10, 20][config % 4];
  const code = toc & 3;
  const frames = code === 0 ? 1 : code < 3 ? 2 : (packet[1] ?? 0) & 0x3f;
  return frames * (ms ?? 20) * 48;
}

const CRC = new Uint32Array(256).map((_, i) => {
  let r = i << 24;
  for (let k = 0; k < 8; k++) r = r & 0x80000000 ? (r << 1) ^ 0x04c11db7 : r << 1;
  return r >>> 0;
});

function page(
  packets: readonly Uint8Array[],
  granule: number,
  serial: number,
  sequence: number,
  flags: number,
): Uint8Array {
  const lacing: number[] = [];
  for (const packet of packets) {
    for (let left = packet.length; ; left -= 255) {
      lacing.push(Math.min(left, 255));
      if (left < 255) break;
    }
  }
  const size = packets.reduce((n, p) => n + p.length, 0);
  const out = new Uint8Array(27 + lacing.length + size);
  const view = new DataView(out.buffer);
  out.set([0x4f, 0x67, 0x67, 0x53, 0, flags]);
  view.setBigUint64(6, BigInt(granule), true);
  view.setUint32(14, serial, true);
  view.setUint32(18, sequence, true);
  out[26] = lacing.length;
  out.set(lacing, 27);
  let at = 27 + lacing.length;
  for (const packet of packets) {
    out.set(packet, at);
    at += packet.length;
  }
  let crc = 0;
  for (const byte of out) crc = ((crc << 8) ^ (CRC[((crc >>> 24) ^ byte) & 0xff] ?? 0)) >>> 0;
  view.setUint32(22, crc, true);
  return out;
}

function opusHead(channels: number): Uint8Array {
  const head = new Uint8Array(19);
  head.set(new TextEncoder().encode("OpusHead"));
  const view = new DataView(head.buffer);
  head[8] = 1;
  head[9] = channels;
  view.setUint16(10, 312, true);
  view.setUint32(12, 48000, true);
  return head;
}

function opusTags(): Uint8Array {
  const vendor = new TextEncoder().encode("Simula");
  const tags = new Uint8Array(8 + 4 + vendor.length + 4);
  tags.set(new TextEncoder().encode("OpusTags"));
  new DataView(tags.buffer).setUint32(8, vendor.length, true);
  tags.set(vendor, 12);
  return tags;
}

/** `webm` as Ogg/Opus, or `undefined` when it isn't WebM with Opus in it. */
export function webmToOgg(webm: Uint8Array): Uint8Array | undefined {
  const opus = readWebm(webm);
  if (!opus) return undefined;
  const serial = crypto.getRandomValues(new Uint32Array(1))[0] ?? 1;
  const pages = [
    page([opus.head ?? opusHead(opus.channels)], 0, serial, 0, 0x02),
    page([opusTags()], 0, serial, 1, 0),
  ];
  let granule = 0;
  let batch: Uint8Array[] = [];
  let segments = 0;
  opus.packets.forEach((packet, i) => {
    const needed = Math.floor(packet.length / 255) + 1;
    if (segments + needed > 255) {
      pages.push(page(batch, granule, serial, pages.length, 0));
      batch = [];
      segments = 0;
    }
    batch.push(packet);
    segments += needed;
    granule += samples(packet);
    if (i === opus.packets.length - 1) pages.push(page(batch, granule, serial, pages.length, 0x04));
  });
  const out = new Uint8Array(pages.reduce((n, p) => n + p.length, 0));
  let at = 0;
  for (const p of pages) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}
