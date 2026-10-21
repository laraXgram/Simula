/**
 * Sending files, like Telegram's dialog: previews of what was picked, whether to
 * compress photos and videos (or send them as files), whether to group them into
 * albums, and a caption.
 */
import { useEffect, useMemo, useState } from "react";

import { type Attachment, isCompressible, prepare } from "../../state/attach";
import { formatSize } from "../../state/media";
import { SwitchRow } from "../kit/Controls";
import { Field, Modal } from "../kit/Dialogs";
import { Icon } from "../kit/Icon";

interface Props {
  files: readonly File[];
  onClose: () => void;
  onSend: (attachments: Attachment[], caption: string, group: boolean) => void;
}

export function AttachDialog({ files, onClose, onSend }: Props) {
  const compressible = files.some(isCompressible);
  const [compress, setCompress] = useState(compressible);
  const [group, setGroup] = useState(true);
  const [caption, setCaption] = useState("");
  const [busy, setBusy] = useState(false);
  const previews = useMemo(
    () =>
      files.map((file) =>
        /^(image|video)\//.test(file.type) ? URL.createObjectURL(file) : undefined,
      ),
    [files],
  );
  useEffect(
    () => () => {
      for (const url of previews) if (url) URL.revokeObjectURL(url);
    },
    [previews],
  );
  const visual = compress && files.every(isCompressible);
  const noun = visual
    ? files.every((f) => f.type.startsWith("image/"))
      ? "photo"
      : files.every((f) => f.type.startsWith("video/"))
        ? "video"
        : "item"
    : "file";
  const title = files.length === 1 ? `Send ${noun}` : `Send ${files.length} ${noun}s`;
  const send = async () => {
    setBusy(true);
    try {
      onSend(await Promise.all(files.map((file) => prepare(file, compress))), caption, group);
    } finally {
      setBusy(false);
    }
  };
  return (
    <Modal
      title={title}
      wide
      onClose={onClose}
      onSubmit={() => void send()}
      actions={[
        { label: "Cancel", onClick: onClose },
        { label: "Send", primary: true, submit: true, disabled: busy },
      ]}
    >
      <div className={visual ? "attach-grid" : "attach-list"}>
        {files.map((file, i) =>
          visual ? (
            <div key={fileKey(file)} className="attach-thumb">
              {file.type.startsWith("video/") ? (
                <video
                  className="attach-preview"
                  src={previews[i]}
                  muted
                  playsInline
                  preload="metadata"
                />
              ) : (
                <img className="attach-preview" src={previews[i]} alt={file.name} />
              )}
            </div>
          ) : (
            <div key={fileKey(file)} className="media-file static">
              <span className="media-file-icon">
                {previews[i] && file.type.startsWith("image/") ? (
                  <img className="media-file-thumb" src={previews[i]} alt="" />
                ) : (
                  <Icon name="file" size={24} />
                )}
              </span>
              <span className="media-file-text">
                <span className="media-file-name" dir="auto">
                  {file.name}
                </span>
                <span className="media-file-size">{formatSize(file.size)}</span>
              </span>
            </div>
          ),
        )}
      </div>
      {compressible && (
        <SwitchRow
          title="Compress images"
          subtitle="Off: photos and videos are sent as files, in full quality"
          checked={compress}
          onChange={setCompress}
        />
      )}
      {files.length > 1 && (
        <SwitchRow
          title="Group items"
          subtitle="Send as albums of up to ten"
          checked={group}
          onChange={setGroup}
        />
      )}
      <Field
        label="Caption"
        value={caption}
        onChange={setCaption}
        multiline
        autoFocus
        maxLength={4096}
      />
    </Modal>
  );
}

const fileKey = (file: File) => `${file.name}:${file.size}:${file.lastModified}`;
