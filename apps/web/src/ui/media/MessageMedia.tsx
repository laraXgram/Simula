/**
 * Media inside a message, drawn the way Telegram's apps draw it: photos and videos sized
 * to their aspect, GIFs and round videos playing silently, voice messages with their
 * waveform, audio with a player, files as rows to download, stickers without a bubble.
 */
import type { tl } from "@simula/schema";
import { type CSSProperties, useEffect, useRef, useState } from "react";

import {
  type Content,
  type DocumentInfo,
  documentLocation,
  documentThumb,
  formatDuration,
  formatSize,
  photoLocation,
  photoSize,
} from "../../state/media";
import { Icon } from "../kit/Icon";
import { saveDocument, useFileUrl, useMedia } from "./context";

/** A photo's or video's box: its aspect, at most `max` wide and tall, at least 120 wide. */
export function fitBox(w = 0, h = 0, max = 400): { width: number; height: number } {
  if (!w || !h) return { width: max * 0.75, height: max * 0.56 };
  const scale = Math.min(max / w, max / h, 1);
  const width = Math.max(120, Math.round(w * scale));
  return { width, height: Math.round((h * width) / w) };
}

interface Props {
  message: tl.message;
  content: Content;
  /** In an album cell: fill it instead of sizing to the media. */
  fill?: boolean;
}

export function MessageMedia({ message, content, fill }: Props) {
  if (content.kind === "photo")
    return (
      <PhotoTile message={message} photo={content.photo} spoiler={content.spoiler} fill={fill} />
    );
  if (content.kind === "other") return <div className="media-other">{content.label}</div>;
  const { info } = content;
  switch (info.kind) {
    case "video":
      return <VideoTile message={message} info={info} spoiler={content.spoiler} fill={fill} />;
    case "gif":
      return <GifTile message={message} info={info} fill={fill} />;
    case "round":
      return <RoundVideo info={info} />;
    case "voice":
      return <AudioPlayer info={info} voice />;
    case "audio":
      return <AudioPlayer info={info} />;
    case "sticker":
      return <Sticker info={info} />;
    default:
      return <FileRow info={info} />;
  }
}

function PhotoTile({
  message,
  photo,
  spoiler,
  fill,
}: {
  message: tl.message;
  photo: tl.photo;
  spoiler: boolean;
  fill?: boolean;
}) {
  const { openViewer } = useMedia();
  const size = photoSize(photo, 800);
  const url = useFileUrl(size && photoLocation(photo, size.type));
  const [hidden, setHidden] = useState(spoiler);
  return (
    <button
      type="button"
      className={["media-tile", hidden ? "spoiler" : "", fill ? "fill" : ""].join(" ")}
      style={fill ? undefined : fitBox(size?.w, size?.h)}
      onClick={() => (hidden ? setHidden(false) : openViewer(message))}
      aria-label="Open photo"
    >
      {url ? <img src={url} alt="" draggable={false} /> : <span className="media-loading" />}
    </button>
  );
}

function VideoTile({
  message,
  info,
  spoiler,
  fill,
}: {
  message: tl.message;
  info: DocumentInfo;
  spoiler: boolean;
  fill?: boolean;
}) {
  const { openViewer } = useMedia();
  const thumb = documentThumb(info.document);
  const url = useFileUrl(thumb && documentLocation(info.document, thumb.type));
  const [hidden, setHidden] = useState(spoiler);
  return (
    <button
      type="button"
      className={["media-tile", hidden ? "spoiler" : "", fill ? "fill" : ""].join(" ")}
      style={fill ? undefined : fitBox(info.w ?? thumb?.w, info.h ?? thumb?.h)}
      onClick={() => (hidden ? setHidden(false) : openViewer(message))}
      aria-label="Play video"
    >
      {url ? <img src={url} alt="" draggable={false} /> : <span className="media-loading" />}
      <span className="media-play">
        <Icon name="play" size={30} />
      </span>
      <span className="media-badge">{formatDuration(info.duration ?? 0)}</span>
    </button>
  );
}

function GifTile({
  message,
  info,
  fill,
}: {
  message: tl.message;
  info: DocumentInfo;
  fill?: boolean;
}) {
  const { openViewer } = useMedia();
  const url = useFileUrl(documentLocation(info.document), info.mime);
  return (
    <button
      type="button"
      className={fill ? "media-tile fill" : "media-tile"}
      style={fill ? undefined : fitBox(info.w, info.h, 320)}
      onClick={() => openViewer(message)}
      aria-label="Open GIF"
    >
      {!url ? (
        <span className="media-loading" />
      ) : info.mime.startsWith("video/") ? (
        <video src={url} autoPlay loop muted playsInline />
      ) : (
        <img src={url} alt="" draggable={false} />
      )}
      <span className="media-badge">GIF</span>
    </button>
  );
}

function RoundVideo({ info }: { info: DocumentInfo }) {
  const url = useFileUrl(documentLocation(info.document), info.mime);
  const video = useRef<HTMLVideoElement>(null);
  // Playing silently in a loop until clicked; then once with sound, with its progress.
  const [sound, setSound] = useState(false);
  const [progress, setProgress] = useState(0);
  const silent = () => {
    const v = video.current;
    setSound(false);
    setProgress(0);
    if (!v) return;
    v.muted = true;
    v.loop = true;
    void v.play();
  };
  const toggle = () => {
    const v = video.current;
    if (!v) return;
    if (sound) {
      silent();
      return;
    }
    v.currentTime = 0;
    v.muted = false;
    v.loop = false;
    void v.play();
    setSound(true);
  };
  const duration = info.duration ?? 0;
  return (
    <button
      type="button"
      className={sound ? "media-round playing" : "media-round"}
      onClick={toggle}
      aria-label={sound ? "Stop video message" : "Play video message"}
    >
      {url ? (
        <video
          ref={video}
          src={url}
          autoPlay
          loop
          muted
          playsInline
          onTimeUpdate={(event) => {
            const v = event.currentTarget;
            if (sound && v.duration > 0) setProgress(v.currentTime / v.duration);
          }}
          onEnded={silent}
        />
      ) : (
        <span className="media-loading" />
      )}
      {sound && (
        <svg className="round-progress" viewBox="0 0 100 100" aria-hidden="true">
          <circle
            cx="50"
            cy="50"
            r="48"
            pathLength="100"
            strokeDasharray={`${progress * 100} 100`}
          />
        </svg>
      )}
      <span className="media-badge">
        {formatDuration(sound ? duration * (1 - progress) : duration)}
        {!sound && <Icon name="volumeOff" size={14} />}
      </span>
    </button>
  );
}

/** The one audio playing in the tab: starting another pauses it. */
let nowPlaying: HTMLAudioElement | null = null;

function useAudio(info: DocumentInfo) {
  const { files } = useMedia();
  const audio = useRef<HTMLAudioElement | null>(null);
  const [playing, setPlaying] = useState(false);
  const [loading, setLoading] = useState(false);
  const [time, setTime] = useState(0);
  useEffect(() => () => audio.current?.pause(), []);
  const load = async () => {
    if (audio.current) return audio.current;
    setLoading(true);
    const url = await files
      .url(documentLocation(info.document), info.mime)
      .finally(() => setLoading(false));
    const a = new Audio(url);
    a.ontimeupdate = () => setTime(a.currentTime);
    a.onplay = () => setPlaying(true);
    a.onpause = () => setPlaying(false);
    a.onended = () => {
      setPlaying(false);
      setTime(0);
    };
    audio.current = a;
    return a;
  };
  const toggle = async () => {
    const a = await load();
    if (!a.paused) {
      a.pause();
      return;
    }
    if (nowPlaying && nowPlaying !== a) nowPlaying.pause();
    nowPlaying = a;
    await a.play();
  };
  const duration = info.duration || audio.current?.duration || 0;
  const seek = async (fraction: number) => {
    const a = await load();
    a.currentTime = fraction * (Number.isFinite(a.duration) ? a.duration : duration);
    if (a.paused) void toggle();
  };
  return { playing, loading, time, duration, toggle, seek };
}

function AudioPlayer({ info, voice }: { info: DocumentInfo; voice?: boolean }) {
  const player = useAudio(info);
  const progress = player.duration ? player.time / player.duration : 0;
  const seekAt = (event: React.MouseEvent<HTMLElement>) => {
    const box = event.currentTarget.getBoundingClientRect();
    const x =
      getComputedStyle(event.currentTarget).direction === "rtl"
        ? box.right - event.clientX
        : event.clientX - box.left;
    void player.seek(Math.max(0, Math.min(1, x / box.width)));
  };
  return (
    <div className={voice ? "media-audio voice" : "media-audio"}>
      <button
        type="button"
        className="media-audio-button"
        onClick={() => void player.toggle()}
        aria-label={player.playing ? "Pause" : "Play"}
      >
        {player.loading ? (
          <span className="spinner" />
        ) : (
          <Icon name={player.playing ? "pause" : "play"} size={26} />
        )}
      </button>
      <div className="media-audio-body">
        {voice ? (
          <button type="button" className="waveform" onClick={seekAt} aria-label="Seek">
            {bars(info.waveform).map((value, i) => (
              <span
                // biome-ignore lint/suspicious/noArrayIndexKey: bars are positional.
                key={i}
                className={i / BARS < progress ? "bar played" : "bar"}
                style={{ "--bar": value } as CSSProperties}
              />
            ))}
          </button>
        ) : (
          <>
            <span className="media-audio-title" dir="auto">
              {info.title || info.name || "Audio"}
            </span>
            <button type="button" className="media-audio-track" onClick={seekAt} aria-label="Seek">
              <span className="track-fill" style={{ inlineSize: `${progress * 100}%` }} />
            </button>
          </>
        )}
        <span className="media-audio-meta" dir="auto">
          {player.playing || player.time > 0
            ? formatDuration(player.time)
            : formatDuration(player.duration)}
          {!voice && info.performer && ` · ${info.performer}`}
          {!voice && !player.playing && player.time === 0 && ` · ${formatSize(info.size)}`}
        </span>
      </div>
    </div>
  );
}

const BARS = 40;

/** A voice message's waveform as `BARS` heights from 0 to 1 (flat when it has none). */
function bars(waveform: readonly number[] | undefined): number[] {
  if (!waveform || waveform.length === 0) return Array(BARS).fill(0.15);
  const peak = Math.max(...waveform, 1);
  return Array.from({ length: BARS }, (_, i) => {
    const from = Math.floor((i * waveform.length) / BARS);
    const to = Math.max(from + 1, Math.floor(((i + 1) * waveform.length) / BARS));
    return Math.max(0.12, Math.max(...waveform.slice(from, to)) / peak);
  });
}

function Sticker({ info }: { info: DocumentInfo }) {
  const playable = info.mime.startsWith("image/") || info.mime.startsWith("video/");
  const thumb = documentThumb(info.document);
  const url = useFileUrl(
    playable
      ? documentLocation(info.document)
      : thumb && documentLocation(info.document, thumb.type),
    playable ? info.mime : "image/jpeg",
  );
  if (!url) return <span className="media-sticker emoji">{info.emoji ?? "🙂"}</span>;
  return info.mime.startsWith("video/") ? (
    <video className="media-sticker" src={url} autoPlay loop muted playsInline />
  ) : (
    <img className="media-sticker" src={url} alt={info.emoji ?? "Sticker"} draggable={false} />
  );
}

function FileRow({ info }: { info: DocumentInfo }) {
  const { files } = useMedia();
  const thumb = documentThumb(info.document);
  const url = useFileUrl(thumb && documentLocation(info.document, thumb.type));
  const [busy, setBusy] = useState(false);
  const ext = (info.name.split(".").pop() ?? "").slice(0, 4).toUpperCase() || "FILE";
  const download = () => {
    setBusy(true);
    void saveDocument(files, info).finally(() => setBusy(false));
  };
  return (
    <button type="button" className="media-file" onClick={download} title={`Download ${info.name}`}>
      <span className="media-file-icon" data-ext={ext.toLowerCase()}>
        {url ? (
          <img className="media-file-thumb" src={url} alt="" />
        ) : (
          <span className="media-file-ext">{ext}</span>
        )}
        <span className="media-file-action">
          {busy ? <span className="spinner" /> : <Icon name="download" size={20} />}
        </span>
      </span>
      <span className="media-file-text">
        <span className="media-file-name" dir="auto">
          {info.name || "File"}
        </span>
        <span className="media-file-size">{formatSize(info.size)}</span>
      </span>
    </button>
  );
}
