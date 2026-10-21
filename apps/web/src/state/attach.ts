/**
 * Files picked to send, read the way Telegram's apps read them: photos are compressed by
 * the server, videos and audio carry their duration and size, videos a thumbnail made
 * in the browser; anything else goes as a file.
 */

export type SendAs = "photo" | "video" | "gif" | "audio" | "voice" | "file";

export interface Attachment {
  file: Blob;
  name: string;
  mime: string;
  as: SendAs;
  size: number;
  duration?: number;
  w?: number;
  h?: number;
  /** A JPEG thumbnail (videos). */
  thumb?: Blob;
  /** Voice messages: Telegram's packed 5-bit waveform. */
  waveform?: Uint8Array;
  title?: string;
  performer?: string;
  /** An object URL to show before and while it uploads. */
  preview?: string;
}

/** Photos above this are sent as files, like Telegram does. */
const MAX_PHOTO = 10 * 1024 * 1024;

/** Read `file` to send it compressed (photos, videos) or, with `compress` off, as a file. */
export async function prepare(file: File, compress: boolean): Promise<Attachment> {
  const base = {
    file,
    name: file.name || "file",
    mime: file.type || "application/octet-stream",
    size: file.size,
  };
  const image = /^image\/(jpeg|png|webp|gif)$/.test(base.mime);
  if (image) base.mime = base.mime.toLowerCase();
  if (base.mime.startsWith("audio/")) {
    const meta = await mediaMeta(file, "audio").catch(() => undefined);
    return { ...base, as: "audio", duration: meta?.duration };
  }
  if (!compress) {
    return { ...base, as: "file", preview: image ? URL.createObjectURL(file) : undefined };
  }
  if (image && base.mime === "image/gif") {
    const size = await imageSize(file).catch(() => undefined);
    return { ...base, as: "gif", ...size, preview: URL.createObjectURL(file) };
  }
  if (image && file.size <= MAX_PHOTO) {
    return { ...base, as: "photo", preview: URL.createObjectURL(file) };
  }
  if (base.mime.startsWith("video/")) {
    const meta = await mediaMeta(file, "video").catch(() => undefined);
    if (meta) {
      return {
        ...base,
        as: "video",
        duration: meta.duration,
        w: meta.w,
        h: meta.h,
        thumb: meta.thumb,
        preview: meta.thumb ? URL.createObjectURL(meta.thumb) : undefined,
      };
    }
  }
  return { ...base, as: "file" };
}

/** Whether a picked file is a photo or a video (the dialog offers to compress them). */
export function isCompressible(file: File): boolean {
  return /^(image\/(jpeg|png|webp|gif)|video\/)/.test(file.type);
}

function imageSize(file: Blob): Promise<{ w: number; h: number }> {
  return new Promise((resolve, reject) => {
    const url = URL.createObjectURL(file);
    const image = new Image();
    image.onload = () => {
      resolve({ w: image.naturalWidth, h: image.naturalHeight });
      URL.revokeObjectURL(url);
    };
    image.onerror = () => {
      reject(new Error("not an image"));
      URL.revokeObjectURL(url);
    };
    image.src = url;
  });
}

interface Meta {
  duration: number;
  w: number;
  h: number;
  thumb?: Blob;
}

/** Duration, size and (for videos) a 320 px JPEG of the first frame. */
function mediaMeta(file: Blob, kind: "audio" | "video"): Promise<Meta> {
  return new Promise((resolve, reject) => {
    const url = URL.createObjectURL(file);
    const element = document.createElement(kind);
    element.preload = "metadata";
    element.muted = true;
    const done = (meta: Meta | Error) => {
      URL.revokeObjectURL(url);
      element.removeAttribute("src");
      if (meta instanceof Error) reject(meta);
      else resolve(meta);
    };
    const timer = setTimeout(() => done(new Error("timed out")), 5000);
    element.onerror = () => {
      clearTimeout(timer);
      done(new Error("unreadable"));
    };
    element.onloadedmetadata = () => {
      const duration = Number.isFinite(element.duration) ? element.duration : 0;
      if (!(element instanceof HTMLVideoElement)) {
        clearTimeout(timer);
        done({ duration, w: 0, h: 0 });
        return;
      }
      const video = element;
      video.onseeked = () => {
        clearTimeout(timer);
        const w = video.videoWidth;
        const h = video.videoHeight;
        const scale = Math.min(1, 320 / Math.max(w, h, 1));
        const canvas = document.createElement("canvas");
        canvas.width = Math.max(1, Math.round(w * scale));
        canvas.height = Math.max(1, Math.round(h * scale));
        canvas.getContext("2d")?.drawImage(video, 0, 0, canvas.width, canvas.height);
        canvas.toBlob(
          (thumb) => done({ duration, w, h, thumb: thumb ?? undefined }),
          "image/jpeg",
          0.8,
        );
      };
      video.currentTime = Math.min(0.1, duration / 2);
    };
    element.src = url;
  });
}
