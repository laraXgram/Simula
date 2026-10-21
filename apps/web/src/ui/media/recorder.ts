/**
 * Recording voice messages (the microphone) and video messages (the camera), the way
 * Telegram's apps send them: voice as Ogg/Opus with a waveform, video messages as a
 * square video, center-cropped from the camera, up to a minute long.
 */
import { useEffect, useRef, useState } from "react";

import type { Attachment } from "../../state/attach";
import { encodeWaveform } from "../../state/media";
import { webmToOgg } from "../../state/ogg";

export type RecordMode = "voice" | "video";

/** A video message's side, in pixels (Telegram's video notes are 384 × 384). */
const SIDE = 384;
/** Video messages stop (and are sent) after a minute, like Telegram's. */
export const MAX_VIDEO_SECONDS = 60;

/** The first of `types` this browser records. */
function supported(types: readonly string[]): string {
  return types.find((type) => MediaRecorder.isTypeSupported(type)) ?? "";
}

/** Telegram's voice messages are Ogg/Opus; browsers that can't record it record WebM. */
const VOICE_TYPES = ["audio/ogg;codecs=opus", "audio/webm;codecs=opus", "audio/webm"];
/** Telegram's video messages are MP4 with H.264 (and AAC, which some browsers can't
 * encode: then Opus); other MP4, or WebM, where H.264 can't be recorded. */
const VIDEO_TYPES = [
  "video/mp4;codecs=avc1.42E01E,mp4a.40.2",
  "video/mp4;codecs=avc1.42E01E,opus",
  "video/mp4;codecs=avc1",
  "video/mp4",
  "video/webm;codecs=vp9,opus",
  "video/webm;codecs=vp8,opus",
  "video/webm",
];

interface Session {
  recorder: MediaRecorder;
  /** What to release when it stops: the devices, the drawing timer. */
  release: () => void;
}

export function useRecorder(
  onDone: (recording: Attachment) => void,
  onError: (message: string) => void,
) {
  const session = useRef<Session | null>(null);
  // Released before the device was ready: stop (sending or not) as soon as it is.
  const starting = useRef(false);
  const stopWhenStarted = useRef<boolean | null>(null);
  const [mode, setMode] = useState<RecordMode | null>(null);
  const [seconds, setSeconds] = useState(0);
  /** The camera, shown in a circle while a video message records. */
  const [camera, setCamera] = useState<MediaStream | null>(null);
  const recording = mode !== null;
  useEffect(() => {
    if (!recording) return;
    const started = Date.now();
    const timer = setInterval(() => setSeconds(Math.floor((Date.now() - started) / 1000)), 250);
    return () => clearInterval(timer);
  }, [recording]);
  useEffect(() => () => session.current?.release(), []);

  const stop = (send: boolean) => {
    if (starting.current) {
      stopWhenStarted.current = send;
      return;
    }
    const current = session.current;
    session.current = null;
    setMode(null);
    setCamera(null);
    setSeconds(0);
    if (!current) return;
    current.recorder.send = send;
    current.recorder.stop();
  };
  // A video message ends at a minute.
  useEffect(() => {
    if (mode === "video" && seconds >= MAX_VIDEO_SECONDS) stop(true);
  });

  const start = async (kind: RecordMode) => {
    if (session.current || starting.current) return;
    if (!navigator.mediaDevices?.getUserMedia || typeof MediaRecorder === "undefined") {
      onError(`This browser can't record ${kind === "voice" ? "audio" : "video"}`);
      return;
    }
    starting.current = true;
    stopWhenStarted.current = null;
    try {
      const started = Date.now();
      const next =
        kind === "voice"
          ? await voiceSession(onDone)
          : await videoSession(onDone, started, setCamera);
      session.current = next;
      starting.current = false;
      setSeconds(0);
      setMode(kind);
      if (stopWhenStarted.current !== null) stop(stopWhenStarted.current);
    } catch (error) {
      starting.current = false;
      const name = (error as Error).name;
      onError(
        name === "NotAllowedError"
          ? `${kind === "voice" ? "Microphone" : "Camera"} access was denied`
          : name === "NotFoundError"
            ? `No ${kind === "voice" ? "microphone" : "camera"} was found`
            : (error as Error).message,
      );
    }
  };
  return { recording, mode, seconds, camera, start, stop };
}

async function voiceSession(onDone: (recording: Attachment) => void): Promise<Session> {
  const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
  const type = supported(VOICE_TYPES);
  const recorder = new MediaRecorder(stream, type ? { mimeType: type } : undefined);
  const chunks: Blob[] = [];
  recorder.ondataavailable = (event) => chunks.push(event.data);
  recorder.onstop = () => {
    stopTracks(stream);
    if (!recorder.send) return;
    const recorded = new Blob(chunks, {
      type: (recorder.mimeType || type || "audio/ogg").split(";")[0],
    });
    void Promise.all([analyze(recorded), asOgg(recorded)]).then(([{ duration, waveform }, file]) =>
      onDone({
        file,
        name: file.type === "audio/ogg" ? "voice.ogg" : "voice.webm",
        mime: file.type,
        as: "voice",
        size: file.size,
        duration,
        waveform,
      }),
    );
  };
  recorder.start();
  return { recorder, release: () => stopTracks(stream) };
}

/** The camera drawn center-cropped onto a square canvas, which is what's recorded. */
async function videoSession(
  onDone: (recording: Attachment) => void,
  started: number,
  showCamera: (stream: MediaStream) => void,
): Promise<Session> {
  const camera = await navigator.mediaDevices.getUserMedia({
    audio: true,
    video: { facingMode: "user", width: { ideal: 640 }, height: { ideal: 640 } },
  });
  const source = document.createElement("video");
  source.srcObject = camera;
  source.muted = true;
  source.playsInline = true;
  await source.play();
  const canvas = document.createElement("canvas");
  canvas.width = SIDE;
  canvas.height = SIDE;
  const context = canvas.getContext("2d");
  const draw = () => {
    const w = source.videoWidth;
    const h = source.videoHeight;
    const side = Math.min(w, h);
    if (side > 0)
      context?.drawImage(source, (w - side) / 2, (h - side) / 2, side, side, 0, 0, SIDE, SIDE);
  };
  draw();
  const drawing = setInterval(draw, 1000 / 30);
  const thumb = await new Promise<Blob | null>((resolve) =>
    canvas.toBlob(resolve, "image/jpeg", 0.85),
  );
  const output = canvas.captureStream(30);
  for (const track of camera.getAudioTracks()) output.addTrack(track);
  const type = supported(VIDEO_TYPES);
  const recorder = new MediaRecorder(output, {
    ...(type ? { mimeType: type } : {}),
    videoBitsPerSecond: 1_000_000,
  });
  const chunks: Blob[] = [];
  const release = () => {
    clearInterval(drawing);
    stopTracks(camera);
    stopTracks(output);
  };
  recorder.ondataavailable = (event) => chunks.push(event.data);
  recorder.onstop = () => {
    release();
    if (!recorder.send) return;
    const mime = (recorder.mimeType || type || "video/webm").split(";")[0] ?? "video/webm";
    const file = new Blob(chunks, { type: mime });
    onDone({
      file,
      name: mime === "video/mp4" ? "video_note.mp4" : "video_note.webm",
      mime,
      as: "round",
      size: file.size,
      duration: Math.max(1, Math.round((Date.now() - started) / 1000)),
      w: SIDE,
      h: SIDE,
      thumb: thumb ?? undefined,
      preview: thumb ? URL.createObjectURL(thumb) : undefined,
    });
  };
  recorder.start(1000);
  showCamera(camera);
  return { recorder, release };
}

function stopTracks(stream: MediaStream | undefined) {
  for (const track of stream?.getTracks() ?? []) track.stop();
}

declare global {
  interface MediaRecorder {
    /** Whether stopping sends the recording (or throws it away). */
    send?: boolean;
  }
}

/** A WebM recording as Ogg/Opus, Telegram's format for voice (as it was if that fails). */
async function asOgg(recorded: Blob): Promise<Blob> {
  if (recorded.type !== "audio/webm") return recorded;
  const ogg = webmToOgg(new Uint8Array(await recorded.arrayBuffer()));
  return ogg ? new Blob([ogg as BlobPart], { type: "audio/ogg" }) : recorded;
}

/** The duration and Telegram's 100-value waveform of a recording. */
async function analyze(file: Blob): Promise<{ duration: number; waveform?: Uint8Array }> {
  const context = new AudioContext();
  try {
    const buffer = await context.decodeAudioData(await file.arrayBuffer());
    const data = buffer.getChannelData(0);
    const bins = 100;
    const peaks = Array.from({ length: bins }, (_, i) => {
      let peak = 0;
      const end = Math.floor(((i + 1) * data.length) / bins);
      for (let j = Math.floor((i * data.length) / bins); j < end; j++)
        peak = Math.max(peak, Math.abs(data[j] ?? 0));
      return peak;
    });
    const top = Math.max(...peaks, 1e-6);
    return {
      duration: Math.max(1, Math.round(buffer.duration)),
      waveform: encodeWaveform(peaks.map((p) => (p / top) * 31)),
    };
  } catch {
    return { duration: 1 };
  } finally {
    void context.close();
  }
}
