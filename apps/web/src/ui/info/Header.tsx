/** A screen's header: back (or close), the title, and an action on the right. */
import type { ReactNode } from "react";

import { Icon } from "../kit/Icon";

interface Props {
  title: string;
  onBack: () => void;
  /** The root screen closes the panel instead of going back. */
  closes?: boolean;
  actions?: ReactNode;
}

export function Header({ title, onBack, closes, actions }: Props) {
  return (
    <header className="panel-header">
      <button
        type="button"
        className="icon-button"
        onClick={onBack}
        aria-label={closes ? "Close" : "Back"}
        title={closes ? "Close" : "Back"}
      >
        <Icon name={closes ? "close" : "back"} />
      </button>
      <h2 className="panel-title">{title}</h2>
      {actions}
    </header>
  );
}
