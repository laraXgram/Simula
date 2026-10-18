/** Writing a message: text, the reply or edit it belongs to, bot commands, keyboards. */
import type { tl } from "@simula/schema";
import { useEffect, useRef, useState } from "react";

export interface Draft {
  replyTo?: tl.message;
  editing?: tl.message;
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
}: Props) {
  const [text, setText] = useState("");
  const [menu, setMenu] = useState(false);
  const input = useRef<HTMLTextAreaElement>(null);

  // Starting a reply or an edit puts the cursor in the box (and the text, for edits).
  const editing = draft.editing;
  const replyId = draft.replyTo?.id;
  useEffect(() => {
    if (editing) setText(editing.message);
    if (editing || replyId !== undefined) input.current?.focus();
  }, [editing, replyId]);

  const send = (value = text) => {
    if (!value.trim()) return;
    onSend(value);
    setText("");
    setMenu(false);
  };
  const target = draft.editing ?? draft.replyTo;

  return (
    <div className="composer">
      {target && (
        <div className="composer-context">
          <span className="composer-context-label">{draft.editing ? "Editing" : "Reply to"}</span>
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
      <div className="composer-row">
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
          placeholder={placeholder || "Message"}
          onChange={(event) => setText(event.target.value)}
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
        <button
          type="button"
          className="send-button"
          onClick={() => send()}
          disabled={!text.trim()}
        >
          {draft.editing ? "Save" : "Send"}
        </button>
      </div>
    </div>
  );
}
