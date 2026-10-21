/** The client: pick an account, then the chat list and the open chat side by side. */
import type { tl } from "@simula/schema";
import { type ReactNode, useCallback, useEffect, useState, useSyncExternalStore } from "react";

import { type Account, account as fetchAccount } from "../api/control";
import { Client, type GroupKind } from "../state/client";
import type { DeepLink } from "../state/links";
import { AccountPicker } from "../ui/AccountPicker";
import { ChatChooser } from "../ui/ChatChooser";
import { ChatView } from "../ui/ChatView";
import { InfoPanel } from "../ui/info/InfoPanel";
import type { Screen } from "../ui/info/nav";
import { DialogsProvider } from "../ui/kit/Dialogs";
import { useNotices } from "../ui/Notices";
import { ProfileDialog } from "../ui/Profile";
import { Sidebar } from "../ui/Sidebar";
import { useTabAccount } from "./account";
import { usePreference } from "./preferences";
import { useRoute } from "./route";

export function App() {
  const [accountId, setAccountId] = useTabAccount();
  const [account, setAccount] = useState<Account | null>(null);
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
    setAccount(null);
    if (!accountId) return;
    setError("");
    fetchAccount(accountId)
      .then((found) => {
        if (found.extra?.deleted) throw new Error("This account was deleted");
        setAccount(found);
      })
      .catch((e: Error) => setError(e.message));
  }, [accountId]);

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

  if (!accountId)
    return (
      <DialogsProvider>
        <AccountPicker onPick={setAccountId} />
      </DialogsProvider>
    );
  if (error)
    return (
      <main className="picker">
        <p className="find-error">{error}</p>
        <button type="button" className="pill-button primary" onClick={() => setAccountId(null)}>
          Choose another account
        </button>
      </main>
    );
  if (!account) return <main className="picker">Connecting…</main>;
  if (!client) return <main className="picker">Connecting…</main>;
  return (
    <DialogsProvider>
      <Shell
        key={client.me}
        client={client}
        account={account}
        notify={notify}
        onSwitchAccount={() => setAccountId(null)}
        onToggleTheme={() =>
          setTheme(theme === "dark" ? "light" : theme === "light" ? "auto" : "dark")
        }
        onToggleDirection={() => setDirection(direction === "ltr" ? "rtl" : "ltr")}
        notices={notices}
      />
    </DialogsProvider>
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
  const [selected, setSelected] = useRoute();
  const [starts, setStarts] = useState<Record<string, string>>({});
  // The right-hand info panel, open on a screen, for the open chat.
  const [info, setInfo] = useState<Screen | null>(null);
  const [editingProfile, setEditingProfile] = useState(false);
  useEffect(() => {
    if (selected === undefined) setInfo(null);
  }, [selected]);
  const openChat = useCallback(
    (key: string | undefined, screen?: Screen) => {
      setInfo(screen ?? null);
      setSelected(key);
    },
    [setSelected],
  );
  // Replying in another chat: the message, then (once chosen) the chat to reply in.
  const [elsewhere, setElsewhere] = useState<{
    from: string;
    message: tl.message;
    to?: string;
  } | null>(null);
  // An invite link joins (or asks to join); anything else is a username.
  const find = useCallback(
    async (query: string) => {
      if (/(t|telegram)\.me\/(\+|joinchat\/)/.test(query)) {
        try {
          setSelected(await client.joinByLink(query));
        } catch (error) {
          if ((error as Error).message === "INVITE_REQUEST_SENT") {
            notify("Your request to join was sent to the admins", { alert: true });
            return;
          }
          throw error;
        }
        return;
      }
      setSelected(await client.resolve(query));
    },
    [client, notify, setSelected],
  );
  const createGroup = useCallback(
    async (kind: GroupKind, title: string, usernames: string[]) =>
      setSelected(await client.createGroup(kind, title, usernames)),
    [client, setSelected],
  );
  const openDeepLink = useCallback(
    (link: DeepLink) =>
      client
        .resolve(link.username)
        .then((key) => {
          if (link.start !== undefined) setStarts((all) => ({ ...all, [key]: link.start ?? "" }));
          setSelected(key);
        })
        .catch((e: Error) =>
          notify(e.message === "USERNAME_NOT_OCCUPIED" ? "No one has that username" : e.message),
        ),
    [client, notify, setSelected],
  );

  return (
    <div
      className={["shell", selected ? "chat-open" : "", info && selected ? "info-open" : ""].join(
        " ",
      )}
    >
      <Sidebar
        snapshot={snapshot}
        account={account}
        selected={selected}
        onSelect={openChat}
        onFind={find}
        onCreateGroup={createGroup}
        onEditProfile={() => setEditingProfile(true)}
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
          onOpenChat={openChat}
          onDeepLink={openDeepLink}
          onOpenInfo={setInfo}
          startParam={starts[selected]}
          externalReply={elsewhere?.to === selected ? elsewhere : undefined}
          onReplyElsewhere={(message) => setElsewhere({ from: selected, message })}
          onExternalReplyDone={() => setElsewhere(null)}
          onStarted={() =>
            setStarts((all) => {
              const { [selected]: _, ...rest } = all;
              return rest;
            })
          }
          onBack={() => setSelected(undefined)}
        />
      ) : (
        <section className="chat empty-chat">
          <p>Select a chat, or open a bot by its @username.</p>
        </section>
      )}
      {selected && info && (
        <InfoPanel
          client={client}
          snapshot={snapshot}
          chatKey={selected}
          start={info}
          onClose={() => setInfo(null)}
          notify={notify}
          onOpenChat={openChat}
        />
      )}
      {elsewhere && !elsewhere.to && (
        <ChatChooser
          snapshot={snapshot}
          title="Reply in…"
          exclude={elsewhere.from}
          onPick={(key) => {
            setElsewhere({ ...elsewhere, to: key });
            setSelected(key);
          }}
          onCancel={() => setElsewhere(null)}
        />
      )}
      {editingProfile && <ProfileDialog client={client} onClose={() => setEditingProfile(false)} />}
      {notices}
    </div>
  );
}
