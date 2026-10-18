/** Per-browser preferences (account, theme, direction), kept in localStorage. */
import { useState } from "react";

export function usePreference<T>(name: string, fallback: T): [T, (value: T) => void] {
  const key = `simula.${name}`;
  const [value, setValue] = useState<T>(() => {
    try {
      const stored = localStorage.getItem(key);
      return stored === null ? fallback : (JSON.parse(stored) as T);
    } catch {
      return fallback;
    }
  });
  const set = (next: T) => {
    setValue(next);
    try {
      localStorage.setItem(key, JSON.stringify(next));
    } catch {
      // Storage may be unavailable (private windows); the preference lasts the session.
    }
  };
  return [value, set];
}
