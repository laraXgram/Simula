/** A file on its way: a pending message with the upload's progress and a cancel button. */
import { formatSize } from "../../state/media";
import type { PendingUpload } from "../../state/store";
import { Icon } from "../kit/Icon";

export function Pending({ upload }: { upload: PendingUpload }) {
  const visual = (upload.kind === "photo" || upload.kind === "video") && upload.preview;
  const ring = (
    <button
      type="button"
      className="progress-ring"
      onClick={upload.cancel}
      aria-label="Cancel upload"
    >
      <svg viewBox="0 0 48 48" aria-hidden="true">
        <circle
          cx="24"
          cy="24"
          r="21"
          pathLength="100"
          strokeDasharray={`${Math.max(4, upload.progress * 100)} 100`}
        />
      </svg>
      <Icon name="close" size={20} />
    </button>
  );
  if (upload.kind === "round")
    return (
      <div className="message out pending">
        <div className="bubble bare">
          <div className="media-round">
            {upload.preview && <img className="round-pending" src={upload.preview} alt="" />}
            {ring}
          </div>
        </div>
      </div>
    );
  return (
    <div className="message out pending">
      <div className={visual ? "bubble has-media media-only" : "bubble"}>
        {visual ? (
          <div className="media-tile pending-tile">
            <img className="pending-preview" src={upload.preview} alt="" />
            {ring}
          </div>
        ) : (
          <div className="media-file">
            <span className="media-file-icon">{ring}</span>
            <span className="media-file-text">
              <span className="media-file-name" dir="auto">
                {upload.kind === "voice" ? "Voice message" : upload.name}
              </span>
              <span className="media-file-size">
                {formatSize(upload.size * upload.progress)} / {formatSize(upload.size)}
              </span>
            </span>
          </div>
        )}
        <div className="meta">
          <span>sending…</span>
        </div>
      </div>
    </div>
  );
}
