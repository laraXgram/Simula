export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:8080';

export const DEFAULT_BOT_TOKEN =
  import.meta.env.VITE_BOT_TOKEN || '123456:TESTTOKEN';

export function buildWsUrl(token: string, lastUpdateId?: number): string {
  const base = API_BASE_URL.replace(/^http/, 'ws');
  const query = typeof lastUpdateId === 'number' ? `?last_update_id=${lastUpdateId}` : '';
  return `${base}/ws/bot${token}${query}`;
}
