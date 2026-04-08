import { ChangeEvent, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import {
  ArrowLeftRight,
  Bug,
  Copy,
  Download,
  RefreshCw,
  Trash2,
  Upload,
  Wifi,
  WifiOff,
} from 'lucide-react';
import { API_BASE_URL, DEFAULT_BOT_TOKEN, buildWsUrl } from '../../services/config';
import type { SimBot } from '../../types/app';

const BOTS_KEY = 'simula-sim-bots';
const SELECTED_BOT_KEY = 'simula-selected-bot-token';
const MAX_UPDATES = 400;

type LogStatus = 'ok' | 'error';
type LogSource = 'bot' | 'webhook' | 'other';
type InspectorTab = 'request' | 'response' | 'diff';
type DataMode = 'live' | 'imported';
type WsState = 'connecting' | 'open' | 'closed';

interface RuntimeLogRecord {
  id: string;
  at: number;
  method: string;
  path: string;
  query?: string;
  statusCode: number;
  durationMs: number;
  remoteAddr?: string;
  request?: unknown;
  response?: unknown;
  status: LogStatus;
  source: LogSource;
}

interface UpdateStreamRecord {
  id: string;
  at: number;
  kind: string;
  summary: string;
  payload: unknown;
}

interface JsonDiffRow {
  path: string;
  kind: 'added' | 'removed' | 'changed';
  left?: unknown;
  right?: unknown;
}

interface TraceBundle {
  version: string;
  exported_at: string;
  api_base_url: string;
  selected_bot_token: string;
  runtime_logs: RuntimeLogRecord[];
  updates: UpdateStreamRecord[];
}

function safeJsonParse<T>(raw: string | null, fallback: T): T {
  if (!raw) {
    return fallback;
  }
  try {
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

function normalizeSimBots(value: unknown): SimBot[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return value
    .map((item) => {
      if (!item || typeof item !== 'object') {
        return null;
      }
      const raw = item as Partial<SimBot>;
      const id = Number(raw.id || 0);
      const token = String(raw.token || '').trim();
      const username = String(raw.username || '').trim();
      const firstName = String(raw.first_name || '').trim();
      if (!id || !token) {
        return null;
      }
      return {
        id,
        token,
        username: username || `bot_${id}`,
        first_name: firstName || `Bot ${id}`,
      };
    })
    .filter((item): item is SimBot => Boolean(item));
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

function getString(value: unknown): string | undefined {
  if (typeof value !== 'string') {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : undefined;
}

function toTimestamp(value: unknown): number {
  const parsed = Number(value || 0);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return Date.now();
  }

  if (parsed < 10_000_000_000) {
    return parsed * 1000;
  }
  return parsed;
}

function normalizeRuntimeLog(value: unknown, index: number): RuntimeLogRecord | null {
  const raw = asRecord(value);
  if (!raw) {
    return null;
  }

  const path = String(raw.path || '').trim();
  const source: LogSource = path.startsWith('/webhook')
    ? 'webhook'
    : path.startsWith('/bot')
      ? 'bot'
      : 'other';

  const responseObject = asRecord(raw.response);
  const inferredFailure = responseObject ? responseObject.ok === false : false;
  const statusCode = Number(raw.status || 0);

  return {
    id: String(raw.id || `runtime-log-${index}-${Date.now()}`),
    at: toTimestamp(raw.at),
    method: String(raw.method || 'UNKNOWN').toUpperCase(),
    path,
    query: getString(raw.query),
    statusCode,
    durationMs: Math.max(0, Number(raw.duration_ms || 0)),
    remoteAddr: getString(raw.remote_addr),
    request: raw.request,
    response: raw.response,
    status: inferredFailure || statusCode >= 400 ? 'error' : 'ok',
    source,
  };
}

function detectUpdateKind(payload: unknown): string {
  const raw = asRecord(payload);
  if (!raw) {
    return 'unknown';
  }

  const knownKinds = [
    'message',
    'edited_message',
    'channel_post',
    'edited_channel_post',
    'inline_query',
    'chosen_inline_result',
    'callback_query',
    'chat_member',
    'my_chat_member',
    'chat_join_request',
    'poll',
    'poll_answer',
    'message_reaction',
    'message_reaction_count',
    'business_connection',
    'business_message',
    'edited_business_message',
    'deleted_business_messages',
    'purchased_paid_media',
  ];

  const match = knownKinds.find((key) => key in raw);
  return match || 'update';
}

function detectUpdateSummary(payload: unknown): string {
  const raw = asRecord(payload);
  if (!raw) {
    return 'Malformed update payload';
  }

  const message = asRecord(raw.message)
    || asRecord(raw.edited_message)
    || asRecord(raw.channel_post)
    || asRecord(raw.edited_channel_post)
    || asRecord(raw.business_message)
    || asRecord(raw.edited_business_message);

  if (message) {
    const chat = asRecord(message.chat);
    const from = asRecord(message.from);
    const chatTitle = getString(chat?.title) || getString(chat?.username) || `chat:${chat?.id || 'unknown'}`;
    const fromName = getString(from?.first_name) || getString(from?.username) || `user:${from?.id || 'unknown'}`;
    const text = getString(message.text) || getString(message.caption);
    return text
      ? `${fromName} -> ${chatTitle}: ${text.slice(0, 80)}`
      : `${fromName} -> ${chatTitle}`;
  }

  const callback = asRecord(raw.callback_query);
  if (callback) {
    return `callback_query: ${getString(callback.data) || 'no data'}`;
  }

  const inlineQuery = asRecord(raw.inline_query);
  if (inlineQuery) {
    return `inline_query: ${getString(inlineQuery.query) || 'empty query'}`;
  }

  const joinRequest = asRecord(raw.chat_join_request);
  if (joinRequest) {
    const from = asRecord(joinRequest.from);
    const fromName = getString(from?.first_name) || getString(from?.username) || 'unknown user';
    return `join request from ${fromName}`;
  }

  return 'Update received';
}

function normalizeUpdateRecord(value: unknown, index: number): UpdateStreamRecord {
  const raw = asRecord(value);
  const updateId = raw ? Number(raw.update_id || 0) : 0;

  return {
    id: `update-${updateId || Date.now()}-${index}`,
    at: Date.now(),
    kind: detectUpdateKind(value),
    summary: detectUpdateSummary(value),
    payload: value,
  };
}

function normalizeImportedUpdate(value: unknown, index: number): UpdateStreamRecord {
  const raw = asRecord(value);
  if (raw && 'payload' in raw && 'kind' in raw && 'summary' in raw) {
    return {
      id: String(raw.id || `imported-update-${index}`),
      at: toTimestamp(raw.at),
      kind: String(raw.kind || 'update'),
      summary: String(raw.summary || 'Imported update'),
      payload: raw.payload,
    };
  }

  return normalizeUpdateRecord(value, index);
}

function formatJson(value: unknown): string {
  if (typeof value === 'undefined') {
    return 'undefined';
  }
  if (typeof value === 'string') {
    return value;
  }
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function flattenJson(value: unknown, prefix: string, output: Map<string, unknown>) {
  if (value === null || typeof value !== 'object') {
    output.set(prefix || '$', value);
    return;
  }

  if (Array.isArray(value)) {
    if (value.length === 0) {
      output.set(prefix || '$', []);
      return;
    }
    value.forEach((item, index) => {
      const nextPath = `${prefix}[${index}]`;
      flattenJson(item, nextPath, output);
    });
    return;
  }

  const entries = Object.entries(value as Record<string, unknown>);
  if (entries.length === 0) {
    output.set(prefix || '$', {});
    return;
  }

  entries
    .sort(([left], [right]) => left.localeCompare(right))
    .forEach(([key, item]) => {
      const nextPath = prefix ? `${prefix}.${key}` : key;
      flattenJson(item, nextPath, output);
    });
}

function buildJsonDiff(left: unknown, right: unknown): JsonDiffRow[] {
  const leftMap = new Map<string, unknown>();
  const rightMap = new Map<string, unknown>();

  flattenJson(left, '', leftMap);
  flattenJson(right, '', rightMap);

  const allPaths = new Set<string>([...leftMap.keys(), ...rightMap.keys()]);
  const rows: JsonDiffRow[] = [];

  Array.from(allPaths)
    .sort((a, b) => a.localeCompare(b))
    .forEach((path) => {
      const hasLeft = leftMap.has(path);
      const hasRight = rightMap.has(path);
      const leftValue = leftMap.get(path);
      const rightValue = rightMap.get(path);

      if (!hasLeft && hasRight) {
        rows.push({ path, kind: 'added', right: rightValue });
        return;
      }
      if (hasLeft && !hasRight) {
        rows.push({ path, kind: 'removed', left: leftValue });
        return;
      }

      const leftEncoded = JSON.stringify(leftValue);
      const rightEncoded = JSON.stringify(rightValue);
      if (leftEncoded !== rightEncoded) {
        rows.push({
          path,
          kind: 'changed',
          left: leftValue,
          right: rightValue,
        });
      }
    });

  return rows;
}

function formatTime(value: number): string {
  return new Date(value).toLocaleTimeString();
}

function truncateText(text: string, maxLength: number): string {
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, maxLength)}...`;
}

export default function DebugPage() {
  const [botOptions, setBotOptions] = useState<SimBot[]>(() => {
    const localBots = safeJsonParse<unknown>(localStorage.getItem(BOTS_KEY), []);
    const normalized = normalizeSimBots(localBots);
    if (normalized.length > 0) {
      return normalized;
    }
    return [{
      id: 0,
      token: DEFAULT_BOT_TOKEN,
      username: 'default_bot',
      first_name: 'Default Bot',
    }];
  });

  const [selectedBotToken, setSelectedBotToken] = useState(
    () => localStorage.getItem(SELECTED_BOT_KEY) || DEFAULT_BOT_TOKEN,
  );
  const [runtimeLogs, setRuntimeLogs] = useState<RuntimeLogRecord[]>([]);
  const [updateEvents, setUpdateEvents] = useState<UpdateStreamRecord[]>([]);
  const [dataMode, setDataMode] = useState<DataMode>('live');
  const [importedBundle, setImportedBundle] = useState<TraceBundle | null>(null);
  const [wsState, setWsState] = useState<WsState>('closed');
  const [searchText, setSearchText] = useState('');
  const [statusFilter, setStatusFilter] = useState<'all' | LogStatus>('all');
  const [sourceFilter, setSourceFilter] = useState<'all' | LogSource>('all');
  const [webhookStatusFilter, setWebhookStatusFilter] = useState<'all' | LogStatus>('all');
  const [webhookUrlFilter, setWebhookUrlFilter] = useState('');
  const [updateSearchText, setUpdateSearchText] = useState('');
  const [inspectorTab, setInspectorTab] = useState<InspectorTab>('request');
  const [selectedLogId, setSelectedLogId] = useState('');
  const [noticeText, setNoticeText] = useState('');
  const [errorText, setErrorText] = useState('');
  const importInputRef = useRef<HTMLInputElement | null>(null);

  const activeLogs = useMemo(
    () => (dataMode === 'imported' && importedBundle ? importedBundle.runtime_logs : runtimeLogs),
    [dataMode, importedBundle, runtimeLogs],
  );

  const activeUpdates = useMemo(
    () => (dataMode === 'imported' && importedBundle ? importedBundle.updates : updateEvents),
    [dataMode, importedBundle, updateEvents],
  );

  const selectedBot = useMemo(
    () => botOptions.find((bot) => bot.token === selectedBotToken) || null,
    [botOptions, selectedBotToken],
  );

  const filteredLogs = useMemo(() => {
    const keyword = searchText.trim().toLowerCase();

    return activeLogs.filter((entry) => {
      if (statusFilter !== 'all' && entry.status !== statusFilter) {
        return false;
      }
      if (sourceFilter !== 'all' && entry.source !== sourceFilter) {
        return false;
      }

      if (!keyword) {
        return true;
      }

      const requestText = formatJson(entry.request).toLowerCase();
      const responseText = formatJson(entry.response).toLowerCase();
      return entry.method.toLowerCase().includes(keyword)
        || entry.path.toLowerCase().includes(keyword)
        || (entry.query || '').toLowerCase().includes(keyword)
        || String(entry.statusCode).includes(keyword)
        || requestText.includes(keyword)
        || responseText.includes(keyword);
    });
  }, [activeLogs, searchText, sourceFilter, statusFilter]);

  const webhookLogs = useMemo(() => {
    const keyword = webhookUrlFilter.trim().toLowerCase();
    return activeLogs
      .filter((entry) => entry.source === 'webhook')
      .filter((entry) => {
        if (webhookStatusFilter !== 'all' && entry.status !== webhookStatusFilter) {
          return false;
        }

        const requestObject = asRecord(entry.request);
        const url = getString(requestObject?.url) || '';
        if (!keyword) {
          return true;
        }
        return url.toLowerCase().includes(keyword);
      });
  }, [activeLogs, webhookStatusFilter, webhookUrlFilter]);

  const filteredUpdates = useMemo(() => {
    const keyword = updateSearchText.trim().toLowerCase();
    if (!keyword) {
      return activeUpdates;
    }
    return activeUpdates.filter((entry) => (
      entry.kind.toLowerCase().includes(keyword)
      || entry.summary.toLowerCase().includes(keyword)
      || formatJson(entry.payload).toLowerCase().includes(keyword)
    ));
  }, [activeUpdates, updateSearchText]);

  const selectedLog = useMemo(
    () => filteredLogs.find((entry) => entry.id === selectedLogId) || filteredLogs[0] || null,
    [filteredLogs, selectedLogId],
  );

  const jsonDiffRows = useMemo(
    () => buildJsonDiff(selectedLog?.request, selectedLog?.response),
    [selectedLog?.request, selectedLog?.response],
  );

  const pollRuntimeLogs = useCallback(async () => {
    if (dataMode !== 'live') {
      return;
    }

    try {
      const baseUrl = API_BASE_URL.replace(/\/$/, '');
      const response = await fetch(`${baseUrl}/client-api/runtime/logs?limit=450`);
      if (!response.ok) {
        throw new Error(`runtime logs unavailable (${response.status})`);
      }

      const payload = await response.json();
      const items = Array.isArray(payload?.result?.items)
        ? payload.result.items as unknown[]
        : [];

      const mapped = items
        .map((item, index) => normalizeRuntimeLog(item, index))
        .filter((item): item is RuntimeLogRecord => Boolean(item))
        .filter((item) => item.source === 'bot' || item.source === 'webhook');

      setRuntimeLogs(mapped);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'failed to load runtime logs');
    }
  }, [dataMode]);

  const reloadBotOptions = useCallback(() => {
    const localBots = safeJsonParse<unknown>(localStorage.getItem(BOTS_KEY), []);
    const normalized = normalizeSimBots(localBots);
    if (normalized.length > 0) {
      setBotOptions(normalized);
      return;
    }

    setBotOptions([{
      id: 0,
      token: DEFAULT_BOT_TOKEN,
      username: 'default_bot',
      first_name: 'Default Bot',
    }]);
  }, []);

  const clearRuntimeLogs = async () => {
    if (dataMode !== 'live') {
      setNoticeText('Switch to live mode to clear server-side runtime logs.');
      return;
    }

    setErrorText('');
    try {
      const baseUrl = API_BASE_URL.replace(/\/$/, '');
      const response = await fetch(`${baseUrl}/client-api/runtime/logs/clear`, {
        method: 'POST',
      });
      const payload = await response.json();
      if (!payload?.ok) {
        throw new Error(payload?.description || 'clear logs failed');
      }
      setRuntimeLogs([]);
      setNoticeText('Runtime logs cleared from server.');
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'clear logs failed');
    }
  };

  const copyToClipboard = async (label: string, value: unknown) => {
    try {
      await navigator.clipboard.writeText(formatJson(value));
      setNoticeText(`${label} copied.`);
    } catch {
      setErrorText(`${label} copy failed.`);
    }
  };

  const exportTrace = () => {
    const bundle: TraceBundle = {
      version: '1.0',
      exported_at: new Date().toISOString(),
      api_base_url: API_BASE_URL,
      selected_bot_token: selectedBotToken,
      runtime_logs: activeLogs,
      updates: activeUpdates,
    };

    const blob = new Blob([JSON.stringify(bundle, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    anchor.href = url;
    anchor.download = `simula-trace-${timestamp}.json`;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    URL.revokeObjectURL(url);
    setNoticeText('Trace bundle exported.');
  };

  const importTrace = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    event.target.value = '';
    if (!file) {
      return;
    }

    setErrorText('');
    try {
      const raw = await file.text();
      const parsed = JSON.parse(raw) as Partial<TraceBundle>;
      const logs = Array.isArray(parsed.runtime_logs)
        ? parsed.runtime_logs
          .map((item, index) => normalizeRuntimeLog(item, index))
          .filter((item): item is RuntimeLogRecord => Boolean(item))
        : [];
      const updates = Array.isArray(parsed.updates)
        ? parsed.updates.map((item, index) => normalizeImportedUpdate(item, index))
        : [];

      const bundle: TraceBundle = {
        version: typeof parsed.version === 'string' ? parsed.version : '1.0',
        exported_at: typeof parsed.exported_at === 'string' ? parsed.exported_at : new Date().toISOString(),
        api_base_url: typeof parsed.api_base_url === 'string' ? parsed.api_base_url : API_BASE_URL,
        selected_bot_token: typeof parsed.selected_bot_token === 'string' ? parsed.selected_bot_token : selectedBotToken,
        runtime_logs: logs,
        updates,
      };

      setImportedBundle(bundle);
      setDataMode('imported');
      if (bundle.selected_bot_token) {
        setSelectedBotToken(bundle.selected_bot_token);
      }
      setNoticeText(`Trace imported (${logs.length} logs, ${updates.length} updates).`);
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : 'trace import failed');
    }
  };

  useEffect(() => {
    localStorage.setItem(SELECTED_BOT_KEY, selectedBotToken);
  }, [selectedBotToken]);

  useEffect(() => {
    reloadBotOptions();
  }, [reloadBotOptions]);

  useEffect(() => {
    if (dataMode !== 'live') {
      return;
    }

    void pollRuntimeLogs();
    const timer = window.setInterval(() => {
      void pollRuntimeLogs();
    }, 1200);

    return () => {
      window.clearInterval(timer);
    };
  }, [dataMode, pollRuntimeLogs]);

  useEffect(() => {
    if (dataMode !== 'live') {
      setWsState('closed');
      return;
    }
    if (!selectedBotToken.trim()) {
      return;
    }

    let ws: WebSocket | null = null;
    let reconnectTimer: number | null = null;
    let reconnectAttempts = 0;
    let closedByUser = false;

    const connect = () => {
      setWsState('connecting');
      ws = new WebSocket(buildWsUrl(selectedBotToken));

      ws.onopen = () => {
        reconnectAttempts = 0;
        setWsState('open');
      };

      ws.onmessage = (event) => {
        try {
          const parsed = JSON.parse(event.data) as unknown;
          const record = normalizeUpdateRecord(parsed, 0);
          setUpdateEvents((previous) => [
            {
              ...record,
              id: `${record.id}-${Date.now()}`,
              at: Date.now(),
            },
            ...previous,
          ].slice(0, MAX_UPDATES));
        } catch {
          // Ignore malformed WS payloads.
        }
      };

      ws.onclose = () => {
        setWsState('closed');
        if (closedByUser) {
          return;
        }

        reconnectAttempts += 1;
        const delayMs = Math.min(1000 + reconnectAttempts * 1000, 7000);
        reconnectTimer = window.setTimeout(connect, delayMs);
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
  }, [dataMode, selectedBotToken]);

  useEffect(() => {
    if (!filteredLogs.find((entry) => entry.id === selectedLogId)) {
      setSelectedLogId(filteredLogs[0]?.id || '');
    }
  }, [filteredLogs, selectedLogId]);

  const websocketBadgeClass = wsState === 'open'
    ? 'border-emerald-300/35 bg-emerald-900/20 text-emerald-100'
    : wsState === 'connecting'
      ? 'border-amber-300/35 bg-amber-900/20 text-amber-100'
      : 'border-red-300/35 bg-red-900/20 text-red-100';

  const selectedRequestText = selectedLog ? formatJson(selectedLog.request) : '';
  const selectedResponseText = selectedLog ? formatJson(selectedLog.response) : '';

  return (
    <div className="min-h-screen bg-app-pattern text-telegram-text">
      <div className="mx-auto flex min-h-screen w-full max-w-[1700px] flex-col px-3 py-3 sm:px-4 sm:py-4">
        <header className="mb-4 flex flex-wrap items-start justify-between gap-3 rounded-2xl border border-white/15 bg-[#112031]/75 px-4 py-3 backdrop-blur-sm lg:items-center">
          <div>
            <div className="flex items-center gap-2">
              <Bug className="h-5 w-5 text-[#7fc8ff]" />
              <h1 className="text-lg font-semibold text-white">Runtime Debug Console</h1>
              <span className="rounded-full border border-white/15 bg-black/20 px-2 py-0.5 text-[11px] text-[#b9d7ee]">/debug</span>
            </div>
            <p className="mt-1 text-xs text-[#9fc4dd]">Realtime request/response logs, webhook dispatch viewer, and websocket update stream.</p>
          </div>

          <div className="flex w-full flex-wrap items-center gap-2 lg:w-auto lg:justify-end">
            <select
              value={selectedBotToken}
              onChange={(event) => setSelectedBotToken(event.target.value)}
              className="w-full rounded-lg border border-white/20 bg-[#0d1a27] px-3 py-2 text-xs text-white outline-none sm:w-auto sm:min-w-[210px]"
            >
              {botOptions.map((bot) => (
                <option key={bot.token} value={bot.token}>
                  {bot.first_name} (@{bot.username})
                </option>
              ))}
            </select>

            <button
              type="button"
              onClick={() => reloadBotOptions()}
              className="rounded-lg border border-white/20 bg-black/20 px-2.5 py-2 text-xs text-white hover:bg-white/10"
              title="Reload bot options from local storage"
            >
              <RefreshCw className="h-4 w-4" />
            </button>

            <div className="flex rounded-lg border border-white/20 bg-black/20 p-1 text-xs">
              <button
                type="button"
                onClick={() => setDataMode('live')}
                className={`rounded px-2 py-1 ${dataMode === 'live' ? 'bg-[#2b5278] text-white' : 'text-[#a8c8df]'}`}
              >
                Live
              </button>
              <button
                type="button"
                onClick={() => setDataMode('imported')}
                disabled={!importedBundle}
                className={`rounded px-2 py-1 ${dataMode === 'imported' ? 'bg-[#2b5278] text-white' : 'text-[#a8c8df]'} disabled:opacity-40`}
              >
                Imported
              </button>
            </div>

            <button
              type="button"
              onClick={() => exportTrace()}
              className="inline-flex items-center gap-1 rounded-lg border border-white/20 bg-black/20 px-2.5 py-2 text-xs text-white hover:bg-white/10"
            >
              <Download className="h-4 w-4" />
              Export trace
            </button>

            <button
              type="button"
              onClick={() => importInputRef.current?.click()}
              className="inline-flex items-center gap-1 rounded-lg border border-white/20 bg-black/20 px-2.5 py-2 text-xs text-white hover:bg-white/10"
            >
              <Upload className="h-4 w-4" />
              Import trace
            </button>
            <input
              ref={importInputRef}
              type="file"
              accept="application/json"
              className="hidden"
              onChange={importTrace}
            />
          </div>
        </header>

        <div className="mb-3 flex flex-col items-start justify-between gap-2 text-xs sm:flex-row sm:items-center">
          <div className="flex items-center gap-2 text-[#b9d7ee]">
            {wsState === 'open' ? <Wifi className="h-4 w-4 text-emerald-300" /> : <WifiOff className="h-4 w-4 text-amber-300" />}
            <span className={`rounded-full border px-2 py-0.5 ${websocketBadgeClass}`}>
              websocket {wsState}
            </span>
            <span className="rounded-full border border-white/15 bg-black/20 px-2 py-0.5 text-[#9dc0da]">
              logs: {activeLogs.length}
            </span>
            <span className="rounded-full border border-white/15 bg-black/20 px-2 py-0.5 text-[#9dc0da]">
              updates: {activeUpdates.length}
            </span>
            {selectedBot ? (
              <span className="max-w-[220px] truncate rounded-full border border-white/15 bg-black/20 px-2 py-0.5 text-[#9dc0da]">
                bot: @{selectedBot.username}
              </span>
            ) : null}
          </div>

          <div className="flex w-full flex-wrap items-center gap-2 sm:w-auto">
            <Link
              to="/chat"
              className="rounded-md border border-white/20 bg-black/20 px-2.5 py-1.5 text-xs text-white hover:bg-white/10"
            >
              Open Chat
            </Link>
            <button
              type="button"
              onClick={() => void pollRuntimeLogs()}
              className="rounded-md border border-white/20 bg-black/20 px-2.5 py-1.5 text-xs text-white hover:bg-white/10"
            >
              Refresh logs
            </button>
            <button
              type="button"
              onClick={() => void clearRuntimeLogs()}
              className="inline-flex items-center gap-1 rounded-md border border-red-300/40 bg-red-900/20 px-2.5 py-1.5 text-xs text-red-100 hover:bg-red-900/30"
            >
              <Trash2 className="h-3.5 w-3.5" />
              Clear logs
            </button>
          </div>
        </div>

        {noticeText ? (
          <div className="mb-2 rounded-lg border border-emerald-300/35 bg-emerald-900/15 px-3 py-2 text-xs text-emerald-100">
            {noticeText}
          </div>
        ) : null}
        {errorText ? (
          <div className="mb-2 rounded-lg border border-red-300/35 bg-red-900/20 px-3 py-2 text-xs text-red-100">
            {errorText}
          </div>
        ) : null}

        <div className="grid flex-1 gap-4 lg:min-h-0 lg:grid-cols-[minmax(0,1.2fr)_minmax(0,1fr)]">
          <div className="grid min-h-0 gap-4 lg:grid-rows-[minmax(0,1fr)_minmax(0,0.9fr)]">
            <section className="min-h-0 rounded-2xl border border-white/10 bg-[#102234]/80 p-3">
              <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
                <p className="text-sm font-medium text-white">Runtime Request/Response Logs</p>
                <span className="rounded-full border border-white/15 bg-black/20 px-2 py-0.5 text-[11px] text-[#9fc4dd]">
                  filtered: {filteredLogs.length}
                </span>
              </div>

              <div className="mb-3 grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
                <input
                  value={searchText}
                  onChange={(event) => setSearchText(event.target.value)}
                  placeholder="Search logs"
                  className="rounded-lg border border-white/15 bg-[#0d1a27] px-2.5 py-1.5 text-xs text-white outline-none"
                />
                <select
                  value={statusFilter}
                  onChange={(event) => setStatusFilter(event.target.value as 'all' | LogStatus)}
                  className="rounded-lg border border-white/15 bg-[#0d1a27] px-2.5 py-1.5 text-xs text-white outline-none"
                >
                  <option value="all">all statuses</option>
                  <option value="ok">ok</option>
                  <option value="error">error</option>
                </select>
                <select
                  value={sourceFilter}
                  onChange={(event) => setSourceFilter(event.target.value as 'all' | LogSource)}
                  className="rounded-lg border border-white/15 bg-[#0d1a27] px-2.5 py-1.5 text-xs text-white outline-none"
                >
                  <option value="all">all sources</option>
                  <option value="bot">telegram api</option>
                  <option value="webhook">webhook</option>
                </select>
                <button
                  type="button"
                  onClick={() => {
                    setSearchText('');
                    setStatusFilter('all');
                    setSourceFilter('all');
                  }}
                  className="rounded-lg border border-white/15 bg-black/20 px-2.5 py-1.5 text-xs text-white hover:bg-white/10"
                >
                  Reset filters
                </button>
              </div>

              <div className="min-h-[240px] max-h-[50vh] overflow-y-auto pr-1 lg:h-[42vh] lg:max-h-none">
                <div className="space-y-2">
                  {filteredLogs.map((entry) => (
                    <button
                      key={entry.id}
                      type="button"
                      onClick={() => setSelectedLogId(entry.id)}
                      className={`w-full rounded-xl border px-3 py-2 text-left transition ${selectedLog?.id === entry.id
                        ? 'border-[#67b9f2] bg-[#214a6f]/60'
                        : 'border-white/10 bg-black/20 hover:bg-black/35'}`}
                    >
                      <div className="flex flex-wrap items-center gap-2 text-[11px]">
                        <span className={`rounded-full border px-1.5 py-0.5 ${entry.status === 'ok'
                          ? 'border-emerald-300/35 bg-emerald-900/20 text-emerald-100'
                          : 'border-red-300/35 bg-red-900/20 text-red-100'}`}>
                          {entry.status}
                        </span>
                        <span className="rounded border border-white/20 bg-black/20 px-1 py-0.5 text-white">
                          {entry.method}
                        </span>
                        <span className="rounded border border-sky-300/30 bg-sky-900/25 px-1 py-0.5 text-sky-100">
                          {entry.source}
                        </span>
                        <span className="text-[#a2c6de]">{formatTime(entry.at)}</span>
                        {entry.statusCode > 0 ? (
                          <span className="text-[#a2c6de]">HTTP {entry.statusCode}</span>
                        ) : null}
                        {entry.durationMs > 0 ? (
                          <span className="text-[#a2c6de]">{entry.durationMs}ms</span>
                        ) : null}
                      </div>

                      <p className="mt-1 break-all text-xs text-white">
                        {entry.path}{entry.query ? `?${entry.query}` : ''}
                      </p>
                    </button>
                  ))}
                  {filteredLogs.length === 0 ? (
                    <p className="rounded-xl border border-white/10 bg-black/20 px-3 py-6 text-center text-xs text-[#9abed7]">
                      No logs match the current filters.
                    </p>
                  ) : null}
                </div>
              </div>
            </section>

            <section className="min-h-0 rounded-2xl border border-white/10 bg-[#102234]/80 p-3">
              <div className="mb-2 flex items-center justify-between">
                <p className="text-sm font-medium text-white">Structured Webhook Viewer</p>
                <span className="rounded-full border border-white/15 bg-black/20 px-2 py-0.5 text-[11px] text-[#9fc4dd]">
                  entries: {webhookLogs.length}
                </span>
              </div>

              <div className="mb-3 grid gap-2 sm:grid-cols-3">
                <select
                  value={webhookStatusFilter}
                  onChange={(event) => setWebhookStatusFilter(event.target.value as 'all' | LogStatus)}
                  className="rounded-lg border border-white/15 bg-[#0d1a27] px-2.5 py-1.5 text-xs text-white outline-none"
                >
                  <option value="all">all statuses</option>
                  <option value="ok">ok</option>
                  <option value="error">error</option>
                </select>
                <input
                  value={webhookUrlFilter}
                  onChange={(event) => setWebhookUrlFilter(event.target.value)}
                  placeholder="Filter by webhook URL"
                  className="rounded-lg border border-white/15 bg-[#0d1a27] px-2.5 py-1.5 text-xs text-white outline-none sm:col-span-2"
                />
              </div>

              <div className="min-h-[180px] max-h-[40vh] overflow-y-auto pr-1 lg:h-[29vh] lg:max-h-none">
                <div className="space-y-2">
                  {webhookLogs.map((entry) => {
                    const request = asRecord(entry.request);
                    const response = asRecord(entry.response);
                    const targetUrl = getString(request?.url) || 'unknown url';
                    const update = asRecord(request?.update);
                    const updateId = update ? Number(update.update_id || 0) : 0;
                    const responseDescription = getString(response?.description);

                    return (
                      <div key={entry.id} className="rounded-lg border border-white/10 bg-black/20 px-3 py-2 text-xs">
                        <div className="flex flex-wrap items-center gap-2">
                          <span className={`rounded-full border px-1.5 py-0.5 ${entry.status === 'ok'
                            ? 'border-emerald-300/35 bg-emerald-900/20 text-emerald-100'
                            : 'border-red-300/35 bg-red-900/20 text-red-100'}`}>
                            {entry.status}
                          </span>
                          <span className="text-[#9fc4dd]">{formatTime(entry.at)}</span>
                          {entry.statusCode > 0 ? <span className="text-[#9fc4dd]">HTTP {entry.statusCode}</span> : null}
                          {updateId > 0 ? <span className="text-[#9fc4dd]">update_id {updateId}</span> : null}
                        </div>
                        <p className="mt-1 break-all text-[#d9edfb]">{targetUrl}</p>
                        {responseDescription ? (
                          <p className="mt-1 text-red-200">{responseDescription}</p>
                        ) : null}
                      </div>
                    );
                  })}
                  {webhookLogs.length === 0 ? (
                    <p className="rounded-lg border border-white/10 bg-black/20 px-3 py-6 text-center text-xs text-[#9abed7]">
                      No webhook dispatch log found for current filters.
                    </p>
                  ) : null}
                </div>
              </div>
            </section>
          </div>

          <div className="grid min-h-0 gap-4 lg:grid-rows-[minmax(0,0.95fr)_minmax(0,1fr)]">
            <section className="min-h-0 rounded-2xl border border-white/10 bg-[#102234]/80 p-3">
              <div className="mb-2 flex items-center justify-between">
                <p className="text-sm font-medium text-white">Realtime Updates Stream</p>
                <span className="rounded-full border border-white/15 bg-black/20 px-2 py-0.5 text-[11px] text-[#9fc4dd]">
                  filtered: {filteredUpdates.length}
                </span>
              </div>

              <div className="mb-3 flex items-center gap-2">
                <input
                  value={updateSearchText}
                  onChange={(event) => setUpdateSearchText(event.target.value)}
                  placeholder="Search updates"
                  className="min-w-0 flex-1 rounded-lg border border-white/15 bg-[#0d1a27] px-2.5 py-1.5 text-xs text-white outline-none"
                />
                <button
                  type="button"
                  onClick={() => setUpdateEvents([])}
                  className="rounded-lg border border-white/15 bg-black/20 px-2.5 py-1.5 text-xs text-white hover:bg-white/10"
                >
                  Clear
                </button>
              </div>

              <div className="min-h-[220px] max-h-[44vh] overflow-y-auto pr-1 lg:h-[35vh] lg:max-h-none">
                <div className="space-y-2">
                  {filteredUpdates.map((entry) => (
                    <details key={entry.id} className="rounded-lg border border-white/10 bg-black/20 px-2.5 py-2">
                      <summary className="cursor-pointer list-none text-xs text-white">
                        <div className="flex flex-wrap items-center gap-2">
                          <span className="rounded-full border border-sky-300/30 bg-sky-900/25 px-1.5 py-0.5 text-[10px] text-sky-100">
                            {entry.kind}
                          </span>
                          <span className="text-[#9fc4dd]">{formatTime(entry.at)}</span>
                        </div>
                        <p className="mt-1 text-[#d8ecfb]">{truncateText(entry.summary, 120)}</p>
                      </summary>
                      <div className="mt-2">
                        <div className="mb-2 flex flex-wrap items-center gap-2">
                          <button
                            type="button"
                            onClick={() => void copyToClipboard('Update payload', entry.payload)}
                            className="inline-flex items-center gap-1 rounded-md border border-white/20 bg-black/20 px-2 py-1 text-[11px] text-white hover:bg-white/10"
                          >
                            <Copy className="h-3.5 w-3.5" />
                            Copy
                          </button>
                        </div>
                        <pre className="max-h-44 overflow-auto whitespace-pre-wrap break-all rounded-lg border border-white/10 bg-black/30 p-2 text-[11px] text-[#d8ecfb]">
                          {formatJson(entry.payload)}
                        </pre>
                      </div>
                    </details>
                  ))}
                  {filteredUpdates.length === 0 ? (
                    <p className="rounded-lg border border-white/10 bg-black/20 px-3 py-6 text-center text-xs text-[#9abed7]">
                      No updates received yet.
                    </p>
                  ) : null}
                </div>
              </div>
            </section>

            <section className="min-h-0 rounded-2xl border border-white/10 bg-[#102234]/80 p-3">
              <div className="mb-2 flex items-center justify-between gap-2">
                <p className="text-sm font-medium text-white">JSON Inspector</p>
                <span className="max-w-full truncate rounded-full border border-white/15 bg-black/20 px-2 py-0.5 text-[11px] text-[#9fc4dd]">
                  {selectedLog ? `${selectedLog.method} ${selectedLog.path}` : 'No selected log'}
                </span>
              </div>

              <div className="mb-3 flex flex-wrap items-center gap-2">
                <button
                  type="button"
                  onClick={() => setInspectorTab('request')}
                  className={`rounded-md border px-2.5 py-1 text-xs ${inspectorTab === 'request'
                    ? 'border-sky-300/40 bg-sky-900/25 text-sky-100'
                    : 'border-white/15 bg-black/20 text-white hover:bg-white/10'}`}
                >
                  Request
                </button>
                <button
                  type="button"
                  onClick={() => setInspectorTab('response')}
                  className={`rounded-md border px-2.5 py-1 text-xs ${inspectorTab === 'response'
                    ? 'border-sky-300/40 bg-sky-900/25 text-sky-100'
                    : 'border-white/15 bg-black/20 text-white hover:bg-white/10'}`}
                >
                  Response
                </button>
                <button
                  type="button"
                  onClick={() => setInspectorTab('diff')}
                  className={`inline-flex items-center gap-1 rounded-md border px-2.5 py-1 text-xs ${inspectorTab === 'diff'
                    ? 'border-sky-300/40 bg-sky-900/25 text-sky-100'
                    : 'border-white/15 bg-black/20 text-white hover:bg-white/10'}`}
                >
                  <ArrowLeftRight className="h-3.5 w-3.5" />
                  Diff
                </button>

                <button
                  type="button"
                  onClick={() => void copyToClipboard('Selected request', selectedLog?.request)}
                  disabled={!selectedLog}
                  className="ml-auto rounded-md border border-white/15 bg-black/20 px-2 py-1 text-xs text-white hover:bg-white/10 disabled:opacity-40"
                >
                  Copy request
                </button>
                <button
                  type="button"
                  onClick={() => void copyToClipboard('Selected response', selectedLog?.response)}
                  disabled={!selectedLog}
                  className="rounded-md border border-white/15 bg-black/20 px-2 py-1 text-xs text-white hover:bg-white/10 disabled:opacity-40"
                >
                  Copy response
                </button>
              </div>

              {!selectedLog ? (
                <p className="rounded-lg border border-white/10 bg-black/20 px-3 py-6 text-center text-xs text-[#9abed7]">
                  Select a runtime log to inspect request, response, and diff.
                </p>
              ) : null}

              {selectedLog && inspectorTab === 'request' ? (
                <pre className="min-h-[220px] max-h-[44vh] overflow-auto whitespace-pre-wrap break-all rounded-lg border border-white/10 bg-black/30 p-3 text-xs text-[#d8ecfb] lg:h-[33vh] lg:max-h-none">
                  {selectedRequestText}
                </pre>
              ) : null}

              {selectedLog && inspectorTab === 'response' ? (
                <pre className="min-h-[220px] max-h-[44vh] overflow-auto whitespace-pre-wrap break-all rounded-lg border border-white/10 bg-black/30 p-3 text-xs text-[#d8ecfb] lg:h-[33vh] lg:max-h-none">
                  {selectedResponseText}
                </pre>
              ) : null}

              {selectedLog && inspectorTab === 'diff' ? (
                <div className="min-h-[220px] max-h-[44vh] overflow-y-auto rounded-lg border border-white/10 bg-black/30 p-2 lg:h-[33vh] lg:max-h-none">
                  <div className="space-y-2 text-xs">
                    {jsonDiffRows.map((row) => (
                      <div
                        key={`${row.kind}:${row.path}`}
                        className={`rounded-md border px-2 py-1.5 ${row.kind === 'added'
                          ? 'border-emerald-300/35 bg-emerald-900/15 text-emerald-100'
                          : row.kind === 'removed'
                            ? 'border-red-300/35 bg-red-900/15 text-red-100'
                            : 'border-amber-300/35 bg-amber-900/15 text-amber-100'}`}
                      >
                        <p className="font-medium">{row.kind.toUpperCase()} - {row.path || '$'}</p>
                        {row.kind !== 'added' ? (
                          <p className="mt-1 break-all text-[11px]">request: {formatJson(row.left)}</p>
                        ) : null}
                        {row.kind !== 'removed' ? (
                          <p className="mt-1 break-all text-[11px]">response: {formatJson(row.right)}</p>
                        ) : null}
                      </div>
                    ))}
                    {jsonDiffRows.length === 0 ? (
                      <p className="rounded-md border border-white/10 bg-black/20 px-2 py-2 text-[11px] text-[#b4d1e6]">
                        No structural diff between request and response.
                      </p>
                    ) : null}
                  </div>
                </div>
              ) : null}
            </section>
          </div>
        </div>
      </div>
    </div>
  );
}
