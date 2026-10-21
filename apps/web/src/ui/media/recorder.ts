/** Recording a voice message with the microphone, the way Telegram's apps send them. */
import { useEffect, useRef, useState } from "react";

import type { Attachment } from "../../state/attach";
import { encodeWaveform } from "../../state/media";
import { webmToOgg } from "../../state/ogg";

/** Telegram's voice messages are OGG/Opus; browsers that can't record it record WebM. */
function recordingType(): string {
  for (const type of ["audio/ogg;codecs=opus", "audio/webm;codecs=opus", "audio/webm"])
    if (MediaRecorder.isTypeSupported(type)) return type;
  return "";
}

export function useRecorder(
  onDone: (voice: Attachment) => void,
  onError: (message: string) => void,
) {
  const recorder = useRef<MediaRecorder | null>(null);
  const [seconds, setSeconds] = useState<number | null>(null);
  const recording = seconds !== null;
  // The timer counts while recording.
  useEffect(() => {
    if (!recording) return;
    const timer = setInterval(() => setSeconds((s) => (s === null ? s : s + 1)), 1000);
    return () => clearInterval(timer);
  }, [recording]);
  useEffect(() => () => stopTracks(recorder.current?.stream), []);

  const start = async () => {
    if (!navigator.mediaDevices?.getUserMedia || typeof MediaRecorder === "undefined") {
      onError("This browser can't record audio");
      return;
    }
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      const type = recordingType();
      const r = new MediaRecorder(stream, type ? { mimeType: type } : undefined);
      const chunks: Blob[] = [];
      r.ondataavailable = (event) => chunks.push(event.data);
      r.onstop = () => {
        stopTracks(stream);
        if (!r.send) return;
        const recorded = new Blob(chunks, {
          type: (r.mimeType || type || "audio/ogg").split(";")[0],
        });
        void Promise.all([analyze(recorded), asOgg(recorded)]).then(
          ([{ duration, waveform }, file]) =>
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
      r.start();
      recorder.current = r;
      setSeconds(0);
    } catch (error) {
      onError(
        (error as Error).name === "NotAllowedError"
          ? "Microphone access was denied"
          : (error as Error).message,
      );
    }
  };
  const stop = (send: boolean) => {
    const r = recorder.current;
    recorder.current = null;
    setSeconds(null);
    if (!r) return;
    r.send = send;
    r.stop();
  };
  return { recording, seconds: seconds ?? 0, start, stop };
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
