const DEFAULT_API_HOST = '127.0.0.1';
const DEFAULT_API_PORT = '8081';

function normalizeApiHost(rawHost: string): string {
  const trimmedHost = rawHost.trim();
  if (!trimmedHost) {
    return DEFAULT_API_HOST;
  }

  if (trimmedHost === '0.0.0.0' || trimmedHost === '::') {
    if (typeof window !== 'undefined' && window.location.hostname) {
      return window.location.hostname;
    }
    return DEFAULT_API_HOST;
  }

  return trimmedHost;
}

function normalizeApiPort(rawPort: string): string {
  const trimmedPort = rawPort.trim();
  if (!trimmedPort) {
    return DEFAULT_API_PORT;
  }

  return /^\d+$/.test(trimmedPort) ? trimmedPort : DEFAULT_API_PORT;
}

function buildApiBaseFromHostPort(rawHost: string, rawPort: string): string {
  const host = normalizeApiHost(rawHost);
  const port = normalizeApiPort(rawPort);
  return `http://${host}:${port}`;
}

function normalizeApiBaseUrl(rawBase: string): string | null {
  const trimmedBase = rawBase.trim();
  if (!trimmedBase) {
    return null;
  }

  try {
    const url = new URL(trimmedBase.includes('://') ? trimmedBase : `http://${trimmedBase}`);
    url.pathname = '';
    url.search = '';
    url.hash = '';
    return url.toString().replace(/\/$/, '');
  } catch {
    return null;
  }
}

function readLaunchQueryValue(key: string): string {
  if (typeof window === 'undefined') {
    return '';
  }

  const value = new URLSearchParams(window.location.search).get(key);
  return value?.trim() || '';
}

function readBuildEnvValue(key: string): string {
  const envValues = (import.meta as ImportMeta & { env?: Record<string, unknown> }).env;
  const rawValue = envValues?.[key];
  return typeof rawValue === 'string' ? rawValue.trim() : '';
}

function resolveApiBaseUrl(): string {
  const directBaseFromQuery = normalizeApiBaseUrl(
    readLaunchQueryValue('api_base_url') || readLaunchQueryValue('api_base'),
  );
  if (directBaseFromQuery) {
    return directBaseFromQuery;
  }

  const hostFromQuery = readLaunchQueryValue('api_host');
  const portFromQuery = readLaunchQueryValue('api_port');
  if (hostFromQuery || portFromQuery) {
    return buildApiBaseFromHostPort(hostFromQuery, portFromQuery);
  }

  const directBaseFromBuildEnv = normalizeApiBaseUrl(
    readBuildEnvValue('VITE_API_BASE_URL') || readBuildEnvValue('API_BASE_URL'),
  );
  if (directBaseFromBuildEnv) {
    return directBaseFromBuildEnv;
  }

  return buildApiBaseFromHostPort(
    readBuildEnvValue('VITE_API_HOST') || readBuildEnvValue('API_HOST'),
    readBuildEnvValue('VITE_API_PORT') || readBuildEnvValue('API_PORT'),
  );
}

export function syncRuntimeClientEnvOverrides(_values: Record<string, string>): void {
  // API base is derived from launcher/.env at page load; no client-side persistence is used.
}

export const API_BASE_URL = resolveApiBaseUrl();

export const DEFAULT_BOT_TOKEN = '123456:TESTTOKEN';

export function buildWsUrl(token: string, lastUpdateId?: number): string {
  const base = API_BASE_URL.replace(/^http/, 'ws');
  const query = typeof lastUpdateId === 'number' ? `?last_update_id=${lastUpdateId}` : '';
  return `${base}/ws/bot${token}${query}`;
}
