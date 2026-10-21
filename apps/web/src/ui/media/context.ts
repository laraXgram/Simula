/** What media in messages needs from the chat: the files, and opening the viewer. */
import type { tl } from "@simula/schema";
import { createContext, useContext, useEffect, useState } from "react";

import type { Files } from "../../state/files";
import { type DocumentInfo, documentLocation } from "../../state/media";

export interface MediaContextValue {
  files: Files;
  /** Show a message's photo or video full screen. */
  openViewer: (message: tl.message) => void;
}

export const MediaContext = createContext<MediaContextValue | null>(null);

export function useMedia(): MediaContextValue {
  const media = useContext(MediaContext);
  if (!media) throw new Error("useMedia outside a chat");
  return media;
}

type Location = tl.inputPhotoFileLocation | tl.inputDocumentFileLocation;

/** An object URL for `location`, once downloaded (`undefined` until then, or to skip). */
export function useFileUrl(location: Location | undefined, mime = ""): string | undefined {
  const { files } = useMedia();
  const [url, setUrl] = useState<string>();
  const key = location ? `${location._}:${location.id}:${location.thumb_size}` : "";
  // biome-ignore lint/correctness/useExhaustiveDependencies: `key` identifies `location`.
  useEffect(() => {
    setUrl(undefined);
    if (!location) return;
    let live = true;
    files
      .url(location, mime)
      .then((u) => live && setUrl(u))
      .catch(() => undefined);
    return () => {
      live = false;
    };
  }, [files, key, mime]);
  return url;
}

/** Save a document to the computer under its name. */
export async function saveDocument(files: Files, info: DocumentInfo) {
  saveUrl(
    await files.url(documentLocation(info.document), info.mime),
    info.name || defaultName(info),
  );
}

export function saveUrl(url: string, name: string) {
  const link = document.createElement("a");
  link.href = url;
  link.download = name;
  link.click();
}

function defaultName(info: DocumentInfo): string {
  const ext = info.mime.split("/")[1]?.split(";")[0] ?? "bin";
  return `${info.kind}_${info.document.id}.${ext}`;
}
