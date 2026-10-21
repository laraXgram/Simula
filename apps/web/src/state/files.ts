/**
 * Files over the session, the way Telegram clients move them: downloads in chunks with
 * `upload.getFile` (kept as object URLs for the session), uploads in parts with
 * `upload.saveFilePart` / `saveBigFilePart` and their progress.
 */
import type { tl } from "@simula/schema";

import type { Session } from "../api/session";
import { fromBase64, toBase64 } from "./media";

const CHUNK = 512 * 1024;
const PART = 512 * 1024;
/** Files above this go up as big files (`saveBigFilePart`), as Telegram's apps do. */
const BIG = 10 * 1024 * 1024;

export class UploadCancelled extends Error {
  constructor() {
    super("UPLOAD_CANCELLED");
  }
}

export class Files {
  private urls = new Map<string, Promise<string>>();

  constructor(private session: Session) {}

  /** An object URL with the file at `location` (downloaded once per session). */
  url(
    location: tl.inputPhotoFileLocation | tl.inputDocumentFileLocation,
    mime = "",
  ): Promise<string> {
    const key = `${location._}:${location.id}:${location.thumb_size}`;
    let url = this.urls.get(key);
    if (!url) {
      url = this.blob(location, mime).then((blob) => URL.createObjectURL(blob));
      url.catch(() => this.urls.delete(key));
      this.urls.set(key, url);
    }
    return url;
  }

  async blob(location: tl.InputFileLocation, mime = ""): Promise<Blob> {
    const parts: Uint8Array[] = [];
    for (let offset = 0; ; offset += CHUNK) {
      const chunk = await this.session.call("upload.getFile", {
        location,
        offset: String(offset),
        limit: CHUNK,
      });
      if (chunk._ !== "upload.file") throw new Error("FILE_REDIRECTED");
      const bytes = fromBase64(chunk.bytes);
      parts.push(bytes);
      if (bytes.length < CHUNK) break;
    }
    return new Blob(parts as BlobPart[], { type: mime });
  }

  /** Upload `file` in parts; `signal` stops it between parts. */
  async upload(
    file: Blob,
    name: string,
    onProgress?: (fraction: number) => void,
    signal?: AbortSignal,
  ): Promise<tl.InputFile> {
    const id = randomId();
    const big = file.size > BIG;
    const parts = Math.max(1, Math.ceil(file.size / PART));
    for (let part = 0; part < parts; part++) {
      if (signal?.aborted) throw new UploadCancelled();
      const bytes = toBase64(
        new Uint8Array(await file.slice(part * PART, (part + 1) * PART).arrayBuffer()),
      );
      if (big)
        await this.session.call("upload.saveBigFilePart", {
          file_id: id,
          file_part: part,
          file_total_parts: parts,
          bytes,
        });
      else await this.session.call("upload.saveFilePart", { file_id: id, file_part: part, bytes });
      onProgress?.((part + 1) / parts);
    }
    return big
      ? { _: "inputFileBig", id, parts, name }
      : { _: "inputFile", id, parts, name, md5_checksum: "" };
  }

  close() {
    for (const url of this.urls.values()) url.then(URL.revokeObjectURL, () => undefined);
    this.urls.clear();
  }
}

/** A random positive 63-bit id, as a decimal string (JSON-TL `long`). */
export function randomId(): string {
  const [high = 0, low = 0] = crypto.getRandomValues(new Uint32Array(2));
  return ((BigInt(high & 0x7fffffff) << 32n) | BigInt(low)).toString();
}
