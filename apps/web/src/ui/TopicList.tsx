/** A forum's topics, the way Telegram lists them: pinned first, then by activity. */
import type { tl } from "@simula/schema";
import { useState } from "react";

import { displayName, senderId } from "../state/peers";
import type { AnyMessage } from "../state/store";
import { GENERAL, topicColor } from "../state/topics";
import { Field, Modal } from "./kit/Dialogs";
import { Icon } from "./kit/Icon";
import { describeService } from "./ServiceMessage";

interface Props {
  topics: readonly tl.forumTopic[];
  messages: ReadonlyMap<number, AnyMessage> | undefined;
  users: ReadonlyMap<string, tl.user>;
  me?: string;
  canCreate: boolean;
  onOpen: (topic: number) => void;
  onCreate: (title: string, color: number) => Promise<void>;
}

/** Telegram's topic icon colors. */
const COLORS = [0x6fb9f0, 0xffd67e, 0xcb86db, 0x8eee98, 0xff93b2, 0xfb6f5f];

function NewTopic({
  onClose,
  onCreate,
}: {
  onClose: () => void;
  onCreate: (title: string, color: number) => Promise<void>;
}) {
  const [title, setTitle] = useState("");
  const [color, setColor] = useState(COLORS[0] ?? 0x6fb9f0);
  const [error, setError] = useState("");
  const hex = `#${color.toString(16).padStart(6, "0")}`;
  return (
    <Modal
      title="New Topic"
      onClose={onClose}
      onSubmit={() =>
        onCreate(title.trim(), color)
          .then(onClose)
          .catch((e: Error) =>
            setError(e.message === "TOPIC_TITLE_EMPTY" ? "Enter a topic name" : e.message),
          )
      }
      actions={[
        { label: "Cancel", onClick: onClose },
        { label: "Create", primary: true, submit: true, disabled: !title.trim() },
      ]}
    >
      <div className="topic-preview-row">
        <span className="topic-icon big" style={{ background: hex }}>
          {[...title.trim()][0]?.toUpperCase() ?? "#"}
        </span>
        <Field
          label="Topic name"
          value={title}
          onChange={setTitle}
          maxLength={128}
          autoFocus
          error={error || undefined}
        />
      </div>
      <fieldset className="color-choices">
        <legend className="sr-only">Icon color</legend>
        {COLORS.map((c) => (
          <label
            key={c}
            className={c === color ? "color-choice on" : "color-choice"}
            style={{ background: `#${c.toString(16).padStart(6, "0")}` }}
          >
            <input
              type="radio"
              name="topic-color"
              className="sr-only"
              checked={c === color}
              onChange={() => setColor(c)}
              aria-label={`Color #${c.toString(16)}`}
            />
          </label>
        ))}
      </fieldset>
    </Modal>
  );
}

export function TopicList({ topics, messages, users, me, canCreate, onOpen, onCreate }: Props) {
  const [creating, setCreating] = useState(false);
  return (
    <div className="topics">
      {canCreate && (
        <button type="button" className="row accent" onClick={() => setCreating(true)}>
          <span className="row-icon">
            <Icon name="add" />
          </span>
          <span className="row-text">
            <span className="row-title">New Topic</span>
          </span>
        </button>
      )}
      {creating && <NewTopic onClose={() => setCreating(false)} onCreate={onCreate} />}
      {topics.map((topic) => {
        const top = messages?.get(topic.top_message);
        return (
          <button
            key={topic.id}
            type="button"
            className={topic.hidden ? "topic hidden" : "topic"}
            onClick={() => onOpen(topic.id)}
          >
            <span className="topic-icon" style={{ background: topicColor(topic) }}>
              {topic.id === GENERAL ? "#" : [...topic.title][0]?.toUpperCase()}
            </span>
            <span className="topic-body">
              <span className="topic-title">
                <span dir="auto">{topic.title}</span>
                {topic.pinned && <span title="Pinned">📌</span>}
                {topic.closed && <span title="Closed">🔒</span>}
                {topic.hidden && <span className="badge">hidden</span>}
              </span>
              <span className="topic-preview">
                <span dir="auto">{preview(top, users, me)}</span>
                {topic.unread_count > 0 && <span className="unread">{topic.unread_count}</span>}
              </span>
            </span>
          </button>
        );
      })}
    </div>
  );
}

function preview(
  message: AnyMessage | undefined,
  users: ReadonlyMap<string, tl.user>,
  me?: string,
): string {
  if (!message) return "";
  if (message._ === "messageService") return describeService(message, users, me);
  const sender = senderId(message);
  const who = message.out
    ? "You: "
    : sender
      ? `${displayName(users.get(sender)).split(" ")[0]}: `
      : "";
  return `${who}${message.message.replace(/\s+/g, " ")}`;
}
