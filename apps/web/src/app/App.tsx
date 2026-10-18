/** The client: pick an account, then the chat list and the open chat side by side. */
import { type ReactNode, useCallback, useEffect, useState, useSyncExternalStore } from "react";

import type { Account } from "../api/control";
import { Client } from "../state/client";
import { AccountPicker } from "../ui/AccountPicker";
import { ChatView } from "../ui/ChatView";
import { useNotices } from "../ui/Notices";
import { Sidebar } from "../ui/Sidebar";
import { usePreference } from "./preferences";

export function App() {
  const [account, setAccount] = usePreference<Account | null>("account", null);
  const [theme, setTheme] = usePreference<"light" | "dark" | "auto">("theme", "auto");
  const [direction, setDirection] = usePreference<"ltr" | "rtl">("direction", "ltr");
  const [client, setClient] = useState<Client | null>(null);
  const [error, setError] = useState("");
  const { notify, view: notices } = useNotices();

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    document.documentElement.dir = direction;
  }, [theme, direction]);

  useEffect(() => {
    if (!account) return;
    let current: Client | null = null;
    setError("");
    Client.connect(String(account.id))
      .then((connected) => {
        current = connected;
        setClient(connected);
      })
      .catch((e: Error) => setError(e.message));
    return () => {
      current?.close();
      setClient(null);
    };
  }, [account]);

  if (!account) return <AccountPicker onPick={setAccount} />;
  if (error)
    return (
      <main className="picker">
        <p className="find-error">{error}</p>
        <button type="button" onClick={() => setAccount(null)}>
          Choose another account
        </button>
      </main>
    );
  if (!client) return <main className="picker">Connecting…</main>;
  return (
    <Shell
      key={client.me}
      client={client}
      account={account}
      notify={notify}
      onSwitchAccount={() => setAccount(null)}
      onToggleTheme={() =>
        setTheme(theme === "dark" ? "light" : theme === "light" ? "auto" : "dark")
      }
      onToggleDirection={() => setDirection(direction === "ltr" ? "rtl" : "ltr")}
      notices={notices}
    />
  );
}

interface ShellProps {
  client: Client;
  account: Account;
  notify: (text: string, options?: { alert?: boolean }) => void;
  onSwitchAccount: () => void;
  onToggleTheme: () => void;
  onToggleDirection: () => void;
  notices: ReactNode;
}

function Shell({
  client,
  account,
  notify,
  onSwitchAccount,
  onToggleTheme,
  onToggleDirection,
  notices,
}: ShellProps) {
  const snapshot = useSyncExternalStore(client.store.subscribe, client.store.snapshot);
  const [selected, setSelected] = useState<string | undefined>();
  const find = useCallback(
    async (username: string) => setSelected(await client.resolve(username)),
    [client],
  );

  return (
    <div className={selected ? "shell chat-open" : "shell"}>
      <Sidebar
        snapshot={snapshot}
        account={account}
        selected={selected}
        onSelect={setSelected}
        onFind={find}
        onSwitchAccount={onSwitchAccount}
        onToggleTheme={onToggleTheme}
        onToggleDirection={onToggleDirection}
      />
      {selected ? (
        <ChatView
          client={client}
          snapshot={snapshot}
          chatKey={selected}
          notify={notify}
          onOpenChat={setSelected}
          onBack={() => setSelected(undefined)}
        />
      ) : (
        <section className="chat empty-chat">
          <p>Select a chat, or open a bot by its @username.</p>
        </section>
      )}
      {notices}
    </div>
  );
}
