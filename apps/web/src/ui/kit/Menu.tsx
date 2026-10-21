/** Popup menus, opened from a button or at the pointer (right-click), closed by a click
 * outside or Escape. */
import {
  type ReactNode,
  type RefObject,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
} from "react";

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

/** Call `onClose` on Escape or a mouse press outside `root`, while `open`. */
function useDismiss(open: boolean, root: RefObject<HTMLElement | null>, onClose: () => void) {
  useEffect(() => {
    if (!open) return;
    const close = (event: Event) => {
      if (event instanceof KeyboardEvent && event.key !== "Escape") return;
      if (event instanceof MouseEvent && root.current?.contains(event.target as Node)) return;
      onClose();
    };
    document.addEventListener("mousedown", close);
    document.addEventListener("keydown", close);
    return () => {
      document.removeEventListener("mousedown", close);
      document.removeEventListener("keydown", close);
    };
  }, [open, root, onClose]);
}

function MenuItems({ items, onDone }: { items: readonly MenuItem[]; onDone: () => void }) {
  return items.map((item) => (
    <button
      key={item.label}
      type="button"
      role="menuitem"
      className={item.danger ? "menu-item danger" : "menu-item"}
      onClick={() => {
        onDone();
        item.onSelect();
      }}
    >
      <Icon name={item.icon} />
      <span>{item.label}</span>
    </button>
  ));
}

export function Menu({ items, trigger, label, align = "end" }: Props) {
  const [open, setOpen] = useState(false);
  const root = useRef<HTMLDivElement>(null);
  const close = useRef(() => setOpen(false)).current;
  useDismiss(open, root, close);
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
          <MenuItems items={shown} onDone={close} />
        </div>
      )}
    </div>
  );
}

/** A menu at a point of the window (where the pointer was), kept inside the window. */
export function ContextMenu({
  items,
  x,
  y,
  onClose,
}: {
  items: Props["items"];
  x: number;
  y: number;
  onClose: () => void;
}) {
  const root = useRef<HTMLDivElement>(null);
  const [place, setPlace] = useState({ left: x, top: y });
  useDismiss(true, root, onClose);
  useLayoutEffect(() => {
    const box = root.current?.getBoundingClientRect();
    if (!box) return;
    setPlace({
      left: Math.max(8, Math.min(x, window.innerWidth - box.width - 8)),
      top: Math.max(8, Math.min(y, window.innerHeight - box.height - 8)),
    });
  }, [x, y]);
  const shown = items.filter((item): item is MenuItem => Boolean(item));
  return (
    <div ref={root} className="menu context" role="menu" style={place}>
      <MenuItems items={shown} onDone={onClose} />
    </div>
  );
}
