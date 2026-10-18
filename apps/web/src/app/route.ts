/** The open chat lives in the URL (`#/u123`), so reloads and the back button work. */
import { useCallback, useEffect, useState } from "react";

function current(): string | undefined {
  const match = /^#\/([a-z]+\d+)$/.exec(location.hash);
  return match?.[1];
}

export function useRoute(): [string | undefined, (key: string | undefined) => void] {
  const [key, setKey] = useState(current);
  useEffect(() => {
    const changed = () => setKey(current());
    window.addEventListener("hashchange", changed);
    return () => window.removeEventListener("hashchange", changed);
  }, []);
  const navigate = useCallback((next: string | undefined) => {
    location.hash = next ? `#/${next}` : "#/";
    setKey(next);
  }, []);
  return [key, navigate];
}
