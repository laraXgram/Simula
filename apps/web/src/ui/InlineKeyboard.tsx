/** An inline keyboard under a message. Each button type does what Telegram's does. */
import type { tl } from "@simula/schema";

import { type DeepLink, parseDeepLink } from "../state/links";

export interface ButtonActions {
  onCallback: (data: string) => void;
  onUser: (userId: string) => void;
  onNotice: (text: string) => void;
  onDeepLink: (link: DeepLink) => void;
}

export function InlineKeyboard({
  markup,
  actions,
}: {
  markup: tl.replyInlineMarkup;
  actions: ButtonActions;
}) {
  return (
    <div className="inline-keyboard">
      {positioned(markup.rows.map((row) => row.buttons)).map((row) => (
        <div className="inline-row" key={row.id}>
          {row.cells.map((cell) => (
            <InlineButton key={cell.id} button={cell.item} actions={actions} />
          ))}
        </div>
      ))}
    </div>
  );
}

/** Keyboards are grids that never reorder, so a cell's position is its identity. */
export function positioned<T>(rows: readonly (readonly T[])[]) {
  return rows.map((row, r) => ({
    id: `r${r}`,
    cells: row.map((item, c) => ({ id: `r${r}c${c}`, item })),
  }));
}

function InlineButton({
  button,
  actions,
}: {
  button: tl.KeyboardInlineButton;
  actions: ButtonActions;
}) {
  const style = button.style?._ === "keyboardButtonStyle" ? button.style : undefined;
  const tone = style?.bg_primary
    ? "primary"
    : style?.bg_danger
      ? "danger"
      : style?.bg_success
        ? "success"
        : "";
  const type = button.type;
  const open = (url: string) => {
    const link = parseDeepLink(url);
    if (link) actions.onDeepLink(link);
    else window.open(url, "_blank", "noopener");
  };
  let onClick: (() => void) | undefined;
  let mark = "";
  let disabled = false;
  switch (type._) {
    case "inlineButtonTypeCallback":
      onClick = () => actions.onCallback(type.data);
      break;
    case "inlineButtonTypeUrl":
    case "inlineButtonTypeUrlAuth":
      mark = "↗";
      onClick = () => open(type.url);
      break;
    case "inlineButtonTypeWebView":
      mark = "▣";
      onClick = () => {
        actions.onNotice("Mini Apps open inside Simula with P5.6; opening the page in a new tab.");
        open(type.url);
      };
      break;
    case "inlineButtonTypeUserProfile":
      onClick = () => actions.onUser(type.user_id);
      break;
    case "inlineButtonTypeCopy":
      mark = "⧉";
      onClick = () => {
        void navigator.clipboard?.writeText(type.copy_text);
        actions.onNotice("Copied to clipboard");
      };
      break;
    case "inlineButtonTypeSwitchInline":
      mark = "↪";
      onClick = () => actions.onNotice("Inline mode comes with P2.11.");
      break;
    case "inlineButtonTypeGame":
    case "inlineButtonTypeBuy":
      onClick = () => actions.onNotice("Games and payments come with P2.11 and P5.7.");
      break;
    default:
      disabled = true;
  }
  return (
    <button
      type="button"
      className={`inline-button ${tone}`}
      disabled={disabled}
      onClick={onClick}
      dir="auto"
    >
      {button.text}
      {mark && <span className="inline-mark">{mark}</span>}
    </button>
  );
}
