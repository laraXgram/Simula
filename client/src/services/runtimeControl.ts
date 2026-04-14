import { API_BASE_URL } from './config';

type RuntimeJson = Record<string, unknown>;

type RuntimeServiceAction = 'restart';

function normalizeApiBase(apiBaseUrl: string): string {
  return apiBaseUrl.replace(/\/$/, '');
}

async function parseJsonResponse(response: Response): Promise<RuntimeJson> {
  const payload = (await response.json()) as unknown;
  if (!payload || typeof payload !== 'object') {
    return {};
  }
  return payload as RuntimeJson;
}

async function requestRuntimeHttp(
  path: string,
  init: RequestInit = {},
  apiBaseUrl = API_BASE_URL,
): Promise<RuntimeJson> {
  const response = await fetch(`${normalizeApiBase(apiBaseUrl)}${path}`, init);
  if (!response.ok) {
    throw new Error(`runtime request failed (${response.status})`);
  }
  return parseJsonResponse(response);
}

export async function loadRuntimeInfo(apiBaseUrl = API_BASE_URL): Promise<RuntimeJson> {
  return requestRuntimeHttp('/client-api/runtime/info', undefined, apiBaseUrl);
}

export async function runRuntimeServiceAction(
  action: RuntimeServiceAction,
  apiBaseUrl = API_BASE_URL,
): Promise<RuntimeJson> {
  return requestRuntimeHttp(
    '/client-api/runtime/service',
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ action }),
    },
    apiBaseUrl,
  );
}

export async function saveRuntimeEnv(
  values: Record<string, string | null>,
  apiBaseUrl = API_BASE_URL,
): Promise<RuntimeJson> {
  return requestRuntimeHttp(
    '/client-api/runtime/env',
    {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ values }),
    },
    apiBaseUrl,
  );
}
