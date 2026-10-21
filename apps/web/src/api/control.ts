/** The Control API calls the client needs: accounts to act as, created, edited, deleted. */

export interface Account {
  id: number;
  is_bot: boolean;
  first_name: string;
  last_name?: string | null;
  username?: string | null;
  lang_code?: string | null;
  phone?: string | null;
  is_premium?: boolean;
  bot_token?: string | null;
  extra?: {
    deleted?: boolean;
    about?: string;
    bot?: { privacy_mode?: boolean; can_join_groups?: boolean };
  };
}

/** What editing an account may change ("" clears optional fields). */
export interface AccountChange {
  first_name?: string;
  last_name?: string;
  username?: string;
  phone?: string;
  lang_code?: string;
  about?: string;
  is_premium?: boolean;
  privacy_mode?: boolean;
  can_join_groups?: boolean;
}

async function request<T>(path: string, body?: unknown, method?: string): Promise<T> {
  const response = await fetch(`/_simula/v1${path}`, {
    method: method ?? (body === undefined ? "GET" : "POST"),
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

export const createBot = (bot: {
  first_name: string;
  username: string;
  token?: string;
  privacy_mode?: boolean;
}) => request<Account>("/bots", bot);

export const account = (id: number) => request<Account>(`/accounts/${id}`);

export const updateAccount = (id: number, change: AccountChange) =>
  request<Account>(`/accounts/${id}`, change, "PATCH");

export const deleteAccount = (id: number) =>
  request<boolean>(`/accounts/${id}`, undefined, "DELETE");
