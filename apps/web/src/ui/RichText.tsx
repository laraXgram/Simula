/**
 * Message text with its entities, as Telegram renders them. Offsets and lengths are in
 * UTF-16 code units, which is how JavaScript strings index, so slicing is direct.
 * Telegram's entities nest without partial overlaps.
 */
import type { tl } from "@simula/schema";
import { type ReactNode, useState } from "react";

export interface TextActions {
  /** A `/command` was clicked (in chats with bots). */
  onCommand?: (command: string) => void;
  /** An `@username` was clicked. */
  onMention?: (username: string) => void;
  /** A mention by id was clicked. */
  onUser?: (userId: string) => void;
}

interface Props extends TextActions {
  text: string;
  entities?: readonly tl.MessageEntity[];
}

export function RichText({ text, entities = [], ...actions }: Props) {
  const sorted = [...entities].sort((a, b) => a.offset - b.offset || b.length - a.length);
  return <>{render(text, 0, text.length, sorted, actions)}</>;
}

function render(
  text: string,
  start: number,
  end: number,
  entities: readonly tl.MessageEntity[],
  actions: TextActions,
): ReactNode[] {
  const out: ReactNode[] = [];
  let at = start;
  let i = 0;
  while (i < entities.length) {
    const entity = entities[i];
    if (entity.offset < at || entity.offset >= end) {
      i++;
      continue;
    }
    const entityEnd = Math.min(entity.offset + entity.length, end);
    if (entity.offset > at) out.push(text.slice(at, entity.offset));
    // Entities inside this one render as its children.
    const inner: tl.MessageEntity[] = [];
    let j = i + 1;
    while (j < entities.length && entities[j].offset < entityEnd) {
      inner.push(entities[j]);
      j++;
    }
    const children = render(text, entity.offset, entityEnd, inner, actions);
    out.push(
      <Entity
        key={`${entity.offset}-${entity._}`}
        entity={entity}
        text={text.slice(entity.offset, entityEnd)}
        actions={actions}
      >
        {children}
      </Entity>,
    );
    at = entityEnd;
    i = j;
  }
  if (at < end) out.push(text.slice(at, end));
  return out;
}

function Entity({
  entity,
  text,
  actions,
  children,
}: {
  entity: tl.MessageEntity;
  text: string;
  actions: TextActions;
  children: ReactNode;
}) {
  switch (entity._) {
    case "messageEntityBold":
      return <strong>{children}</strong>;
    case "messageEntityItalic":
      return <em>{children}</em>;
    case "messageEntityUnderline":
      return <u>{children}</u>;
    case "messageEntityStrike":
      return <s>{children}</s>;
    case "messageEntitySpoiler":
      return <Spoiler>{children}</Spoiler>;
    case "messageEntityCode":
      return <code className="inline-code">{children}</code>;
    case "messageEntityPre":
      return (
        <pre className="code-block" data-language={entity.language || undefined}>
          <code>{children}</code>
        </pre>
      );
    case "messageEntityBlockquote":
      return <Quote collapsed={entity.collapsed === true}>{children}</Quote>;
    case "messageEntityTextUrl":
      return (
        <a href={entity.url} target="_blank" rel="noreferrer" title={entity.url}>
          {children}
        </a>
      );
    case "messageEntityUrl":
      return (
        <a
          href={/^[a-z][a-z0-9+.-]*:/i.test(text) ? text : `http://${text}`}
          target="_blank"
          rel="noreferrer"
        >
          {children}
        </a>
      );
    case "messageEntityEmail":
      return <a href={`mailto:${text}`}>{children}</a>;
    case "messageEntityPhone":
      return <a href={`tel:${text}`}>{children}</a>;
    case "messageEntityMention":
      return (
        <button type="button" className="link" onClick={() => actions.onMention?.(text.slice(1))}>
          {children}
        </button>
      );
    case "messageEntityMentionName":
      return (
        <button type="button" className="link" onClick={() => actions.onUser?.(entity.user_id)}>
          {children}
        </button>
      );
    case "messageEntityBotCommand":
      return (
        <button type="button" className="link" onClick={() => actions.onCommand?.(text)}>
          {children}
        </button>
      );
    case "messageEntityHashtag":
    case "messageEntityCashtag":
      return <span className="tag">{children}</span>;
    case "messageEntityFormattedDate":
      return <time title={new Date(entity.date * 1000).toISOString()}>{formatDate(entity)}</time>;
    default:
      return <>{children}</>;
  }
}

function Spoiler({ children }: { children: ReactNode }) {
  const [shown, setShown] = useState(false);
  return (
    <button
      type="button"
      className={shown ? "spoiler shown" : "spoiler"}
      onClick={() => setShown(true)}
    >
      {children}
    </button>
  );
}

function Quote({ collapsed, children }: { collapsed: boolean; children: ReactNode }) {
  const [open, setOpen] = useState(!collapsed);
  return (
    <blockquote className={open ? "quote" : "quote collapsed"}>
      {children}
      {!open && (
        <button type="button" className="link quote-more" onClick={() => setOpen(true)}>
          Show more
        </button>
      )}
    </blockquote>
  );
}

/** A `date_time` entity shows the time in the reader's locale, as its flags ask. */
function formatDate(entity: tl.messageEntityFormattedDate): string {
  const date = new Date(entity.date * 1000);
  if (entity.relative) {
    const minutes = Math.round((date.getTime() - Date.now()) / 60000);
    return new Intl.RelativeTimeFormat(undefined, { numeric: "auto" }).format(minutes, "minute");
  }
  // `weekday` can't be mixed with `dateStyle`/`timeStyle`; spell such formats out.
  const options: Intl.DateTimeFormatOptions = {};
  const hasDate = entity.short_date || entity.long_date;
  const hasTime = entity.short_time || entity.long_time;
  if (entity.day_of_week && !hasDate) {
    options.weekday = "long";
    if (hasTime) Object.assign(options, { hour: "numeric", minute: "2-digit" });
    if (entity.long_time) options.second = "2-digit";
  } else {
    if (hasDate)
      options.dateStyle = entity.day_of_week ? "full" : entity.long_date ? "long" : "short";
    if (hasTime) options.timeStyle = entity.long_time ? "medium" : "short";
  }
  return date.toLocaleString(undefined, Object.keys(options).length ? options : undefined);
}
