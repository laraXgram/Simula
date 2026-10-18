/** The few Control API calls the client needs: accounts to act as. */

export interface Account {
  id: number;
  is_bot: boolean;
  first_name: string;
  last_name?: string | null;
  username?: string | null;
  lang_code?: string | null;
  bot_token?: string | null;
}

async function request<T>(path: string, body?: unknown): Promise<T> {
  const response = await fetch(`/_simula/v1${path}`, {
    method: body === undefined ? "GET" : "POST",
    headers: { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const json = (await response.json()) as { ok: boolean; result?: T; error?: { message?: string } };
  if (!json.ok) throw new Error(json.error?.message ?? `request to ${path} failed`);
  return json.result as T;
}

export const accounts = () => request<Account[]>("/accounts");

export const createUser = (user: {
  first_name: string;
  last_name?: string;
  username?: string;
  lang_code?: string;
}) => request<Account>("/users", user);

export const createBot = (bot: { first_name: string; username: string; token?: string }) =>
  request<Account>("/bots", bot);
