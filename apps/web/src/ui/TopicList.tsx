/** A forum's topics, the way Telegram lists them: pinned first, then by activity. */
import type { tl } from "@simula/schema";
import { useState } from "react";

import { displayName, senderId } from "../state/peers";
import type { AnyMessage } from "../state/store";
import { GENERAL, topicColor } from "../state/topics";
import { describeService } from "./ServiceMessage";

interface Props {
  topics: readonly tl.forumTopic[];
  messages: ReadonlyMap<number, AnyMessage> | undefined;
  users: ReadonlyMap<string, tl.user>;
  me?: string;
  canCreate: boolean;
  onOpen: (topic: number) => void;
  onCreate: (title: string) => Promise<void>;
}

export function TopicList({ topics, messages, users, me, canCreate, onOpen, onCreate }: Props) {
  const [title, setTitle] = useState("");
  const [error, setError] = useState("");
  return (
    <div className="topics">
      {canCreate && (
        <form
          className="new-topic"
          onSubmit={(event) => {
            event.preventDefault();
            setError("");
            onCreate(title.trim())
              .then(() => setTitle(""))
              .catch((e: Error) => setError(e.message));
          }}
        >
          <input
            value={title}
            onChange={(event) => setTitle(event.target.value)}
            placeholder="New topic name"
            required
          />
          <button type="submit">Create topic</button>
        </form>
      )}
      {error && <p className="find-error">{error}</p>}
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
