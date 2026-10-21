/**
 * Writing a message: text, the reply or edit it belongs to, bot commands, keyboards,
 * files to attach (picked or pasted) and voice messages.
 */
import type { tl } from "@simula/schema";
import { useEffect, useRef, useState } from "react";
import { usePreference } from "../app/preferences";
import type { Attachment } from "../state/attach";
import { toFormatting } from "../state/markdown";
import { Icon } from "./kit/Icon";
import { RecordButton, RecordingBar, RoundPreview } from "./media/Recording";
import { type RecordMode, useRecorder } from "./media/recorder";

export interface Draft {
  replyTo?: tl.message;
  editing?: tl.message;
  /** A reply to a message of another chat (named `chat`). */
  external?: { chat: string; message: tl.message };
}

interface Props {
  draft: Draft;
  placeholder?: string;
  commands: readonly tl.botCommand[];
  keyboardAvailable: boolean;
  keyboardShown: boolean;
  onToggleKeyboard: () => void;
  onCancel: () => void;
  onSend: (text: string) => void;
  /** Files picked or pasted, for the send dialog. */
  onAttach: (files: File[]) => void;
  /** A voice or video message, recorded. */
  onRecorded: (recording: Attachment) => void;
  onError: (message: string) => void;
}

export function Composer({
  draft,
  placeholder,
  commands,
  keyboardAvailable,
  keyboardShown,
  onToggleKeyboard,
  onCancel,
  onSend,
  onAttach,
  onRecorded,
  onError,
}: Props) {
  const [text, setText] = useState("");
  const [menu, setMenu] = useState(false);
  const input = useRef<HTMLTextAreaElement>(null);
  const picker = useRef<HTMLInputElement>(null);
  const recorder = useRecorder(onRecorded, onError);
  // Voice or video messages: a tap on the record button switches, as in Telegram.
  const [mode, switchMode] = usePreference<RecordMode>("record-mode", "voice");
  // Held down (recording until released) or locked (until sent or cancelled).
  const [holding, setHolding] = useState(false);
  const [locked, setLocked] = useState(false);

  // Starting a reply or an edit puts the cursor in the box (and the text, for edits).
  const editing = draft.editing;
  const replyId = draft.replyTo?.id ?? draft.external?.message.id;
  useEffect(() => {
    if (editing) setText(toFormatting(editing.message, editing.entities));
    if (editing || replyId !== undefined) input.current?.focus();
  }, [editing, replyId]);

  const send = (value = text) => {
    if (!value.trim()) return;
    onSend(value);
    setText("");
    setMenu(false);
  };
  const target = draft.editing ?? draft.replyTo ?? draft.external?.message;
  const label = draft.editing
    ? "Editing"
    : draft.external
      ? `Reply to «${draft.external.chat}»`
      : "Reply to";

  return (
    <div className="composer">
      {target && (
        <div className="composer-context">
          <span className="composer-context-label" dir="auto">
            {label}
          </span>
          <span className="composer-context-text" dir="auto">
            {target.message}
          </span>
          <button type="button" onClick={onCancel} title="Cancel">
            ✕
          </button>
        </div>
      )}
      {menu && commands.length > 0 && (
        <div className="commands-menu">
          {commands.map((command) => (
            <button key={command.command} type="button" onClick={() => send(`/${command.command}`)}>
              <span className="command-name">/{command.command}</span>
              <span className="command-description" dir="auto">
                {command.description}
              </span>
            </button>
          ))}
        </div>
      )}
      {recorder.camera && <RoundPreview stream={recorder.camera} seconds={recorder.seconds} />}
      {recorder.recording || holding ? (
        <RecordingBar
          mode={recorder.mode ?? mode}
          seconds={recorder.seconds}
          locked={locked}
          onCancel={() => recorder.stop(false)}
          onSend={() => recorder.stop(true)}
        />
      ) : (
        <div className="composer-row">
          {!draft.editing && (
            <>
              <button
                type="button"
                className="icon-button"
                onClick={() => picker.current?.click()}
                aria-label="Attach files"
                title="Attach files"
              >
                <Icon name="attach" />
              </button>
              <input
                ref={picker}
                type="file"
                multiple
                hidden
                onChange={(event) => {
                  const files = [...(event.target.files ?? [])];
                  event.target.value = "";
                  if (files.length > 0) onAttach(files);
                }}
              />
            </>
          )}
          {commands.length > 0 && (
            <button
              type="button"
              className="icon-button"
              onClick={() => setMenu(!menu)}
              title="Commands"
            >
              /
            </button>
          )}
          <textarea
            ref={input}
            dir="auto"
            rows={1}
            value={text}
            placeholder={placeholder || "Message  (**bold**, __italic__, `code`, [link](url))"}
            onChange={(event) => setText(event.target.value)}
            onPaste={(event) => {
              const files = [...event.clipboardData.files];
              if (files.length === 0 || draft.editing) return;
              event.preventDefault();
              onAttach(files);
            }}
            onKeyDown={(event) => {
              if (event.key === "Enter" && !event.shiftKey) {
                event.preventDefault();
                send();
              } else if (event.key === "Escape") {
                onCancel();
              }
            }}
          />
          {keyboardAvailable && (
            <button
              type="button"
              className={keyboardShown ? "icon-button active" : "icon-button"}
              onClick={onToggleKeyboard}
              title="Bot keyboard"
            >
              ⌨
            </button>
          )}
          {text.trim() || draft.editing ? (
            <button
              type="button"
              className="send-button round"
              onClick={() => send()}
              disabled={!text.trim()}
              aria-label={draft.editing ? "Save" : "Send"}
              title={draft.editing ? "Save" : "Send"}
            >
              <Icon name={draft.editing ? "check" : "send"} />
            </button>
          ) : (
            <RecordButton
              mode={mode}
              onToggle={() => switchMode(mode === "voice" ? "video" : "voice")}
              onStart={(lock) => {
                setLocked(lock);
                setHolding(!lock);
                void recorder.start(mode);
              }}
              onRelease={(release) => {
                setHolding(false);
                if (release === "lock") setLocked(true);
                else recorder.stop(release === "send");
              }}
            />
          )}
        </div>
      )}
    </div>
  );
}
