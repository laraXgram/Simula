/** A popup menu opened from a button, closed by a click outside or Escape. */
import { type ReactNode, useEffect, useRef, useState } from "react";

import { Icon, type IconName } from "./Icon";

export interface MenuItem {
  icon: IconName;
  label: string;
  onSelect: () => void;
  danger?: boolean;
}

interface Props {
  items: readonly (MenuItem | false | null | undefined)[];
  /** The button's content and its accessible name. */
  trigger: ReactNode;
  label: string;
  align?: "start" | "end";
}

export function Menu({ items, trigger, label, align = "end" }: Props) {
  const [open, setOpen] = useState(false);
  const root = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open) return;
    const close = (event: Event) => {
      if (event instanceof KeyboardEvent && event.key !== "Escape") return;
      if (event instanceof MouseEvent && root.current?.contains(event.target as Node)) return;
      setOpen(false);
    };
    document.addEventListener("mousedown", close);
    document.addEventListener("keydown", close);
    return () => {
      document.removeEventListener("mousedown", close);
      document.removeEventListener("keydown", close);
    };
  }, [open]);
  const shown = items.filter((item): item is MenuItem => Boolean(item));
  if (shown.length === 0) return null;
  return (
    <div className="menu-root" ref={root}>
      <button
        type="button"
        className={open ? "icon-button active" : "icon-button"}
        aria-label={label}
        title={label}
        aria-haspopup="menu"
        aria-expanded={open}
        onClick={() => setOpen(!open)}
      >
        {trigger}
      </button>
      {open && (
        <div className={`menu ${align}`} role="menu">
          {shown.map((item) => (
            <button
              key={item.label}
              type="button"
              role="menuitem"
              className={item.danger ? "menu-item danger" : "menu-item"}
              onClick={() => {
                setOpen(false);
                item.onSelect();
              }}
            >
              <Icon name={item.icon} />
              <span>{item.label}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
