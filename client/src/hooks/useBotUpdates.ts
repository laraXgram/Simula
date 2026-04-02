import { useEffect, useRef } from 'react';
import { BotUpdate } from '../types/app';
import { buildWsUrl } from '../services/config';

interface UseBotUpdatesOptions {
  token: string;
  lastUpdateId?: number;
  onUpdate: (update: BotUpdate) => void;
}

export function useBotUpdates({ token, lastUpdateId, onUpdate }: UseBotUpdatesOptions) {
  const callbackRef = useRef(onUpdate);
  const lastUpdateIdRef = useRef(lastUpdateId);

  useEffect(() => {
    callbackRef.current = onUpdate;
  }, [onUpdate]);

  useEffect(() => {
    lastUpdateIdRef.current = lastUpdateId;
  }, [lastUpdateId]);

  useEffect(() => {
    if (!token.trim()) {
      return;
    }

    let ws: WebSocket | null = null;
    let reconnectTimer: number | null = null;
    let reconnectAttempts = 0;
    let closedByUser = false;

    const connect = () => {
      const url = buildWsUrl(token, lastUpdateIdRef.current);
      ws = new WebSocket(url);

      ws.onopen = () => {
        reconnectAttempts = 0;
      };

      ws.onmessage = (event) => {
        try {
          const parsed = JSON.parse(event.data) as BotUpdate;
          callbackRef.current(parsed);
        } catch {
          // Ignore malformed payloads from development servers.
        }
      };

      ws.onclose = () => {
        if (closedByUser) {
          return;
        }

        const backoffMs = Math.min(1500 * (reconnectAttempts + 1), 8000);
        reconnectAttempts += 1;
        reconnectTimer = window.setTimeout(connect, backoffMs);
      };

      ws.onerror = () => {
        ws?.close();
      };
    };

    connect();

    return () => {
      closedByUser = true;
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
      }
      ws?.close();
    };
  }, [token]);
}
