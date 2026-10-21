/**
 * The full-screen viewer: a chat's photos and videos one at a time, with who sent each
 * and when, the caption, arrows (and ← →) between them, download, Escape to close.
 */
import type { tl } from "@simula/schema";
import { useEffect } from "react";

import {
  contentOf,
  documentLocation,
  largestPhotoSize,
  photoLocation,
  photoSize,
} from "../../state/media";
import { Icon } from "../kit/Icon";
import { saveDocument, saveUrl, useFileUrl, useMedia } from "./context";

interface Props {
  items: readonly tl.message[];
  index: number;
  author: (message: tl.message) => string;
  onIndex: (index: number) => void;
  onClose: () => void;
}

export function Viewer({ items, index, author, onIndex, onClose }: Props) {
  const message = items[index];
  const prev = index > 0 ? () => onIndex(index - 1) : undefined;
  const next = index < items.length - 1 ? () => onIndex(index + 1) : undefined;
  useEffect(() => {
    const key = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
      const rtl = document.documentElement.dir === "rtl";
      if (event.key === (rtl ? "ArrowRight" : "ArrowLeft")) prev?.();
      if (event.key === (rtl ? "ArrowLeft" : "ArrowRight")) next?.();
    };
    document.addEventListener("keydown", key);
    return () => document.removeEventListener("keydown", key);
  }, [onClose, prev, next]);
  if (!message) return null;
  return (
    <div className="viewer" role="dialog" aria-modal="true" aria-label="Media viewer">
      <header className="viewer-bar">
        <div className="viewer-who">
          <span className="viewer-author" dir="auto">
            {author(message)}
          </span>
          <span className="viewer-date">
            {new Date(message.date * 1000).toLocaleString(undefined, {
              dateStyle: "medium",
              timeStyle: "short",
            })}
          </span>
        </div>
        <span className="viewer-count">
          {index + 1} of {items.length}
        </span>
        <ViewerDownload message={message} />
        <button
          type="button"
          className="icon-button"
          onClick={onClose}
          aria-label="Close"
          title="Close"
        >
          <Icon name="close" />
        </button>
      </header>
      {/* biome-ignore lint/a11y/noStaticElementInteractions: a click beside the media closes; Escape closes too */}
      <div
        className="viewer-stage"
        onMouseDown={(event) => event.target === event.currentTarget && onClose()}
      >
        {prev && (
          <button type="button" className="viewer-nav prev" onClick={prev} aria-label="Previous">
            <Icon name="back" size={28} />
          </button>
        )}
        <ViewerMedia key={message.id} message={message} />
        {next && (
          <button type="button" className="viewer-nav next" onClick={next} aria-label="Next">
            <Icon name="back" size={28} />
          </button>
        )}
      </div>
      {message.message && (
        <p className="viewer-caption" dir="auto">
          {message.message}
        </p>
      )}
    </div>
  );
}

function ViewerMedia({ message }: { message: tl.message }) {
  const content = contentOf(message);
  const photo = content?.kind === "photo" ? content.photo : undefined;
  const info = content?.kind === "document" ? content.info : undefined;
  const medium = photo && photoSize(photo, 800);
  const large = photo && largestPhotoSize(photo);
  const mediumUrl = useFileUrl(medium && photo && photoLocation(photo, medium.type));
  const largeUrl = useFileUrl(large && photo && photoLocation(photo, large.type));
  const videoUrl = useFileUrl(info && documentLocation(info.document), info?.mime);
  if (photo) {
    const url = largeUrl ?? mediumUrl;
    return url ? (
      <img className="viewer-media" src={url} alt="" />
    ) : (
      <span className="spinner large" />
    );
  }
  if (!info || !videoUrl) return <span className="spinner large" />;
  if (info.mime.startsWith("image/")) return <img className="viewer-media" src={videoUrl} alt="" />;
  return info.kind === "gif" ? (
    <video className="viewer-media" src={videoUrl} autoPlay loop muted playsInline />
  ) : (
    // biome-ignore lint/a11y/useMediaCaption: users' videos have no captions track.
    <video className="viewer-media" src={videoUrl} autoPlay controls playsInline />
  );
}

function ViewerDownload({ message }: { message: tl.message }) {
  const { files } = useMedia();
  const content = contentOf(message);
  const download = async () => {
    if (content?.kind === "document") await saveDocument(files, content.info);
    else if (content?.kind === "photo") {
      const size = largestPhotoSize(content.photo);
      if (size)
        saveUrl(
          await files.url(photoLocation(content.photo, size.type), "image/jpeg"),
          `photo_${message.id}.jpg`,
        );
    }
  };
  return (
    <button
      type="button"
      className="icon-button"
      onClick={() => void download()}
      aria-label="Download"
      title="Download"
    >
      <Icon name="download" />
    </button>
  );
}
