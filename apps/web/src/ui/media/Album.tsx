/** Messages sent together: photos and videos in a mosaic, files and audio stacked. */
import type { tl } from "@simula/schema";
import type { CSSProperties } from "react";

import { type Content, contentOf, isVisual, photoSize } from "../../state/media";
import { MessageMedia } from "./MessageMedia";

/** How many items each row of a mosaic holds, for 1 to 10 items. */
const ROWS: Record<number, number[]> = {
  1: [1],
  2: [2],
  3: [1, 2],
  4: [2, 2],
  5: [2, 3],
  6: [3, 3],
  7: [3, 4],
  8: [4, 4],
  9: [3, 3, 3],
  10: [3, 3, 4],
};

const ROW_HEIGHT: Record<number, number> = { 1: 220, 2: 170, 3: 125, 4: 100 };

export function Album({ messages }: { messages: readonly tl.message[] }) {
  const items = messages.map((message) => ({ message, content: contentOf(message) }));
  if (!items.every((item) => isVisual(item.content)))
    return (
      <div className="album-list">
        {items.map(
          ({ message, content }) =>
            content && <MessageMedia key={message.id} message={message} content={content} />,
        )}
      </div>
    );
  const rows: (typeof items)[] = [];
  let next = 0;
  for (const count of ROWS[items.length] ?? [items.length]) {
    rows.push(items.slice(next, next + count));
    next += count;
  }
  return (
    <div className="album">
      {rows.map((row) => (
        <div
          key={row[0]?.message.id}
          className="album-row"
          style={{ blockSize: ROW_HEIGHT[row.length] ?? 100 }}
        >
          {row.map(
            ({ message, content }) =>
              content && (
                <div
                  key={message.id}
                  className="album-cell"
                  style={{ "--aspect": aspect(content) } as CSSProperties}
                >
                  <MessageMedia message={message} content={content} fill />
                </div>
              ),
          )}
        </div>
      ))}
    </div>
  );
}

function aspect(content: Content): number {
  if (content.kind === "photo") {
    const size = photoSize(content.photo, 800);
    return size ? size.w / size.h : 1;
  }
  if (content.kind === "document" && content.info.w && content.info.h)
    return content.info.w / content.info.h;
  return 1;
}
