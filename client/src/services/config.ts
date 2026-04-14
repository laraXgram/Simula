const RUNTIME_API_BASE_URL_KEY = 'simula-runtime-api-base-url';

function readRuntimeOverride(key: string): string | null {
  if (typeof window === 'undefined') {
    return null;
  }

  const raw = window.localStorage.getItem(key);
  const normalized = raw?.trim() || '';
  return normalized.length > 0 ? normalized : null;
}

function writeRuntimeOverride(key: string, value: string | null): void {
  if (typeof window === 'undefined') {
    return;
  }

  const normalized = value?.trim() || '';
  if (!normalized) {
    window.localStorage.removeItem(key);
    return;
  }

  window.localStorage.setItem(key, normalized);
}

function buildRuntimeApiBase(values: Record<string, string>): string | null {
  const rawHost = (values.API_HOST || '').trim();
  const rawPort = (values.API_PORT || '').trim();
  if (!rawHost || !rawPort) {
    return null;
  }

  const normalizedHost = rawHost === '0.0.0.0' || rawHost === '::'
    ? (typeof window !== 'undefined' ? window.location.hostname : '127.0.0.1')
    : rawHost;

  try {
    const url = new URL(normalizedHost.includes('://') ? normalizedHost : `http://${normalizedHost}`);
    url.port = rawPort;
    url.pathname = '';
    url.search = '';
    url.hash = '';
    return url.toString().replace(/\/$/, '');
  } catch {
    return null;
  }
}

export function syncRuntimeClientEnvOverrides(values: Record<string, string>): void {
  const runtimeApiBase = buildRuntimeApiBase(values);
  writeRuntimeOverride(RUNTIME_API_BASE_URL_KEY, runtimeApiBase);

  // Cleanup legacy runtime token override from old builds.
  if (typeof window !== 'undefined') {
    window.localStorage.removeItem('simula-runtime-bot-token');
  }
}

export const API_BASE_URL =
  readRuntimeOverride(RUNTIME_API_BASE_URL_KEY)
  || 'http://127.0.0.1:8081';

export const DEFAULT_BOT_TOKEN = '123456:TESTTOKEN';

export function buildWsUrl(token: string, lastUpdateId?: number): string {
  const base = API_BASE_URL.replace(/^http/, 'ws');
  const query = typeof lastUpdateId === 'number' ? `?last_update_id=${lastUpdateId}` : '';
  return `${base}/ws/bot${token}${query}`;
}
