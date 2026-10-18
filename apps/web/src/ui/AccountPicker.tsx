/** Choose which simulated user to be, or create one. */
import { useEffect, useState } from "react";

import { type Account, accounts, createUser } from "../api/control";

export function AccountPicker({ onPick }: { onPick: (account: Account) => void }) {
  const [list, setList] = useState<Account[]>([]);
  const [name, setName] = useState("");
  const [username, setUsername] = useState("");
  const [language, setLanguage] = useState("en");
  const [error, setError] = useState("");

  useEffect(() => {
    accounts()
      .then(setList)
      .catch((e: Error) => setError(`Is the Simula server running? ${e.message}`));
  }, []);

  const users = list.filter((a) => !a.is_bot);
  const bots = list.filter((a) => a.is_bot);

  return (
    <main className="picker">
      <h1>Simula</h1>
      <p className="picker-lead">
        Pick the user you want to be. Bots talk to them through the Bot API.
      </p>
      {error && <p className="find-error">{error}</p>}
      <div className="picker-list">
        {users.map((account) => (
          <button key={account.id} type="button" onClick={() => onPick(account)}>
            <span dir="auto">
              {[account.first_name, account.last_name].filter(Boolean).join(" ")}
            </span>
            <span className="picker-meta">
              {account.username ? `@${account.username} · ` : ""}
              {account.id}
            </span>
          </button>
        ))}
        {users.length === 0 && <p className="empty">No users yet.</p>}
      </div>
      <form
        className="picker-form"
        onSubmit={(event) => {
          event.preventDefault();
          createUser({
            first_name: name,
            username: username || undefined,
            lang_code: language || undefined,
          })
            .then(onPick)
            .catch((e: Error) => setError(e.message));
        }}
      >
        <h2>New user</h2>
        <input
          required
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="First name"
          dir="auto"
        />
        <input
          value={username}
          onChange={(e) => setUsername(e.target.value)}
          placeholder="Username (optional)"
        />
        <input
          value={language}
          onChange={(e) => setLanguage(e.target.value)}
          placeholder="Language (en, fa, …)"
        />
        <button type="submit">Create and continue</button>
      </form>
      {bots.length > 0 && (
        <p className="picker-meta">
          Bots in this sandbox: {bots.map((b) => `@${b.username}`).join(", ")}
        </p>
      )}
    </main>
  );
}
