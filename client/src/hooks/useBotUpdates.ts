import { useEffect, useRef } from 'react';
import { BotUpdate, SimRealtimeEvent } from '../types/app';
import { buildWsUrl } from '../services/config';

interface UseBotUpdatesOptions {
  token: string;
  lastUpdateId?: number;
  onUpdate: (update: BotUpdate) => void;
  onSimEvent?: (event: SimRealtimeEvent) => void;
}

export function useBotUpdates({ token, lastUpdateId, onUpdate, onSimEvent }: UseBotUpdatesOptions) {
  const callbackRef = useRef(onUpdate);
  const simEventRef = useRef(onSimEvent);
  const lastUpdateIdRef = useRef(lastUpdateId);

  useEffect(() => {
    callbackRef.current = onUpdate;
  }, [onUpdate]);

  useEffect(() => {
    simEventRef.current = onSimEvent;
  }, [onSimEvent]);

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
          const parsed = JSON.parse(event.data) as unknown;
          if (
            parsed
            && typeof parsed === 'object'
            && 'sim_event' in (parsed as Record<string, unknown>)
          ) {
            simEventRef.current?.(parsed as SimRealtimeEvent);
            return;
          }

          callbackRef.current(parsed as BotUpdate);
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
