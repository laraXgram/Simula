/**
 * Recording controls, as in Telegram's apps: the record button (a tap switches between
 * voice and video messages; holding records, releasing sends, sliding up locks the
 * recording, sliding away cancels; Enter or Space records, locked), the bar shown while
 * recording, and the camera in a circle while a video message records.
 */
import { type PointerEvent as ReactPointerEvent, useEffect, useRef } from "react";

import { formatDuration } from "../../state/media";
import { Icon } from "../kit/Icon";
import { MAX_VIDEO_SECONDS, type RecordMode } from "./recorder";

const HOLD_MS = 250;
const LOCK_DISTANCE = 70;
const CANCEL_DISTANCE = 120;

export type Release = "send" | "cancel" | "lock";

export function RecordButton({
  mode,
  onToggle,
  onStart,
  onRelease,
}: {
  mode: RecordMode;
  onToggle: () => void;
  /** Recording starts: held down, or `locked` (started from the keyboard). */
  onStart: (locked: boolean) => void;
  onRelease: (release: Release) => void;
}) {
  const press = (event: ReactPointerEvent<HTMLButtonElement>) => {
    if (event.button !== 0) return;
    event.preventDefault();
    const x0 = event.clientX;
    const y0 = event.clientY;
    let held = false;
    let locked = false;
    const timer = setTimeout(() => {
      held = true;
      onStart(false);
    }, HOLD_MS);
    const move = (e: PointerEvent) => {
      if (held && !locked && y0 - e.clientY > LOCK_DISTANCE) {
        locked = true;
        onRelease("lock");
      }
    };
    const up = (e: PointerEvent) => {
      clearTimeout(timer);
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", up);
      window.removeEventListener("pointercancel", up);
      if (!held) {
        if (e.type === "pointerup") onToggle();
        return;
      }
      if (locked) return;
      const away = e.type === "pointercancel" || Math.abs(e.clientX - x0) > CANCEL_DISTANCE;
      onRelease(away ? "cancel" : "send");
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", up);
    window.addEventListener("pointercancel", up);
  };
  const what = mode === "voice" ? "voice message" : "video message";
  return (
    <button
      type="button"
      className="send-button round record-button"
      onPointerDown={press}
      // Keyboard "clicks" (detail 0) record, locked; pointer clicks are handled above.
      onClick={(event) => event.detail === 0 && onStart(true)}
      onContextMenu={(event) => event.preventDefault()}
      aria-label={`Record a ${what}`}
      title={`Hold to record a ${what}. Tap to switch to ${mode === "voice" ? "video" : "voice"} messages.`}
    >
      <Icon key={mode} name={mode === "voice" ? "mic" : "videocam"} />
    </button>
  );
}

export function RecordingBar({
  mode,
  seconds,
  locked,
  onCancel,
  onSend,
}: {
  mode: RecordMode;
  seconds: number;
  locked: boolean;
  onCancel: () => void;
  onSend: () => void;
}) {
  return (
    <div className="composer-row recording">
      {locked ? (
        <button
          type="button"
          className="icon-button"
          onClick={onCancel}
          aria-label="Cancel recording"
          title="Cancel"
        >
          <Icon name="delete" />
        </button>
      ) : null}
      <span className="recording-dot" />
      <span className="recording-time">{formatDuration(seconds)}</span>
      <span className="recording-hint">
        {locked
          ? `Recording a ${mode === "voice" ? "voice" : "video"} message…`
          : "Release to send · slide away to cancel · slide up to lock"}
      </span>
      <button
        type="button"
        className={locked ? "send-button round" : "send-button round holding"}
        onClick={onSend}
        aria-label={mode === "voice" ? "Send voice message" : "Send video message"}
        title="Send"
      >
        <Icon name={locked ? "send" : mode === "voice" ? "mic" : "videocam"} />
      </button>
      {!locked && (
        <span className="lock-hint" aria-hidden="true">
          <Icon name="lock" size={18} />
        </span>
      )}
    </div>
  );
}

/** The camera in a circle, with a ring filling up to a minute. */
export function RoundPreview({ stream, seconds }: { stream: MediaStream; seconds: number }) {
  const video = useRef<HTMLVideoElement>(null);
  useEffect(() => {
    if (video.current) video.current.srcObject = stream;
  }, [stream]);
  return (
    <div className="round-preview">
      <video ref={video} autoPlay muted playsInline />
      <svg viewBox="0 0 100 100" aria-hidden="true">
        <circle
          cx="50"
          cy="50"
          r="48.5"
          pathLength="100"
          strokeDasharray={`${(seconds / MAX_VIDEO_SECONDS) * 100} 100`}
        />
      </svg>
    </div>
  );
}
