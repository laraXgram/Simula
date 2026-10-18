/** Toasts for short notices (callback answers), and alerts that need an OK. */
import { useCallback, useState } from "react";

interface Notice {
  id: number;
  text: string;
  alert: boolean;
}

export function useNotices() {
  const [notices, setNotices] = useState<Notice[]>([]);
  const dismiss = useCallback(
    (id: number) => setNotices((list) => list.filter((n) => n.id !== id)),
    [],
  );
  const notify = useCallback(
    (text: string, options?: { alert?: boolean }) => {
      const notice = { id: Date.now() + Math.random(), text, alert: options?.alert === true };
      setNotices((list) => [...list, notice]);
      if (!notice.alert) setTimeout(() => dismiss(notice.id), 3000);
    },
    [dismiss],
  );
  const view = (
    <>
      <div className="toasts" aria-live="polite">
        {notices
          .filter((n) => !n.alert)
          .map((n) => (
            <div key={n.id} className="toast" dir="auto">
              {n.text}
            </div>
          ))}
      </div>
      {notices
        .filter((n) => n.alert)
        .slice(0, 1)
        .map((n) => (
          <div key={n.id} className="alert-backdrop">
            <div className="alert" role="alertdialog">
              <p dir="auto">{n.text}</p>
              <button type="button" onClick={() => dismiss(n.id)}>
                OK
              </button>
            </div>
          </div>
        ))}
    </>
  );
  return { notify, view };
}
