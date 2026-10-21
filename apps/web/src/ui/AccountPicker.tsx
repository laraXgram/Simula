/**
 * The accounts of the sandbox: pick who to be (here or in a new tab), create users and
 * bots, edit them, delete them.
 */
import { useCallback, useEffect, useState } from "react";

import {
  type Account,
  type AccountChange,
  accounts,
  createBot,
  createUser,
  deleteAccount,
  updateAccount,
} from "../api/control";
import { openTab } from "../app/account";
import { Avatar } from "./kit/Avatar";
import { SwitchRow } from "./kit/Controls";
import { Field, Modal, useDialogs } from "./kit/Dialogs";
import { Icon } from "./kit/Icon";
import { Menu } from "./kit/Menu";

const fullName = (a: Account) => [a.first_name, a.last_name].filter(Boolean).join(" ");

export function AccountPicker({ onPick }: { onPick: (id: number) => void }) {
  const dialogs = useDialogs();
  const [list, setList] = useState<Account[] | null>(null);
  const [error, setError] = useState("");
  const [editing, setEditing] = useState<Account | "user" | "bot" | null>(null);
  const reload = useCallback(() => {
    accounts()
      .then((all) => setList(all.filter((a) => !a.extra?.deleted)))
      .catch((e: Error) => setError(`Is the Simula server running? ${e.message}`));
  }, []);
  useEffect(reload, [reload]);
  const users = list?.filter((a) => !a.is_bot) ?? [];
  const bots = list?.filter((a) => a.is_bot) ?? [];
  const remove = async (account: Account) => {
    const ok = await dialogs.confirm({
      title: `Delete ${fullName(account)}?`,
      text: account.is_bot
        ? "The bot's token stops working; its messages stay as from “Deleted Account”."
        : "The account becomes “Deleted Account”: its messages stay, but nobody can sign in as it or write to it.",
      ok: "Delete",
      danger: true,
    });
    if (!ok) return;
    deleteAccount(account.id)
      .then(reload)
      .catch((e: Error) => setError(e.message));
  };
  const row = (account: Account) => (
    <div key={account.id} className="account-row">
      <button
        type="button"
        className="account-open"
        onClick={() => onPick(account.id)}
        disabled={account.is_bot}
      >
        <Avatar id={String(account.id)} name={fullName(account)} size={46} />
        <span className="row-text">
          <span className="account-name" dir="auto">
            {fullName(account)}
            {account.is_premium && <span className="badge">premium</span>}
          </span>
          <span className="row-subtitle">
            {account.username ? `@${account.username} · ` : ""}
            {account.id}
          </span>
        </span>
      </button>
      <Menu
        label={`Actions for ${fullName(account)}`}
        trigger={<Icon name="more" />}
        items={[
          !account.is_bot && {
            icon: "chat",
            label: "Open here",
            onSelect: () => onPick(account.id),
          },
          !account.is_bot && {
            icon: "link",
            label: "Open in a new tab",
            onSelect: () => openTab(account.id),
          },
          { icon: "edit", label: "Edit", onSelect: () => setEditing(account) },
          account.is_bot &&
            !!account.bot_token && {
              icon: "copy",
              label: "Copy token",
              onSelect: () => void navigator.clipboard?.writeText(account.bot_token ?? ""),
            },
          { icon: "delete", label: "Delete", danger: true, onSelect: () => remove(account) },
        ]}
      />
    </div>
  );
  return (
    <main className="accounts">
      <div className="accounts-card">
        <h1>Simula</h1>
        <p className="picker-lead">Pick who to be. Each browser tab can be a different account.</p>
        {error && <p className="find-error">{error}</p>}
        <div className="accounts-head">
          <h2>People</h2>
          <button type="button" className="pill-button primary" onClick={() => setEditing("user")}>
            <Icon name="personAdd" size={18} /> New user
          </button>
        </div>
        {list === null ? <p className="empty">Loading…</p> : users.map(row)}
        {list !== null && users.length === 0 && (
          <p className="empty">No users yet. Create one to start.</p>
        )}
        <div className="accounts-head">
          <h2>Bots</h2>
          <button type="button" className="pill-button" onClick={() => setEditing("bot")}>
            <Icon name="add" size={18} /> New bot
          </button>
        </div>
        {bots.map(row)}
        {list !== null && bots.length === 0 && (
          <p className="empty">No bots yet. Bots connect to the Bot API with their token.</p>
        )}
      </div>
      {editing && (
        <AccountDialog
          account={typeof editing === "string" ? undefined : editing}
          bot={typeof editing === "string" ? editing === "bot" : editing.is_bot}
          onClose={() => setEditing(null)}
          onSaved={(saved, created) => {
            setEditing(null);
            reload();
            if (created && !saved.is_bot) onPick(saved.id);
          }}
        />
      )}
    </main>
  );
}

/** Create or edit a user or bot through the Control API. */
function AccountDialog({
  account,
  bot,
  onClose,
  onSaved,
}: {
  account?: Account;
  bot: boolean;
  onClose: () => void;
  onSaved: (account: Account, created: boolean) => void;
}) {
  const [first, setFirst] = useState(account?.first_name ?? "");
  const [last, setLast] = useState(account?.last_name ?? "");
  const [username, setUsername] = useState(account?.username ?? "");
  const [about, setAbout] = useState(account?.extra?.about ?? "");
  const [lang, setLang] = useState(account?.lang_code ?? (account ? "" : "en"));
  const [premium, setPremium] = useState(account?.is_premium === true);
  const [privacy, setPrivacy] = useState(account?.extra?.bot?.privacy_mode !== false);
  const [groups, setGroups] = useState(account?.extra?.bot?.can_join_groups !== false);
  const [error, setError] = useState("");
  const save = () => {
    setError("");
    const done = (saved: Account) => onSaved(saved, account === undefined);
    const fail = (e: Error) => setError(readable(e.message));
    if (account) {
      const change: AccountChange = bot
        ? { first_name: first, username, privacy_mode: privacy, can_join_groups: groups }
        : {
            first_name: first,
            last_name: last,
            username,
            about,
            lang_code: lang,
            is_premium: premium,
          };
      updateAccount(account.id, change).then(done).catch(fail);
    } else if (bot) {
      createBot({ first_name: first, username, privacy_mode: privacy }).then(done).catch(fail);
    } else {
      createUser({
        first_name: first,
        last_name: last || undefined,
        username: username || undefined,
        lang_code: lang || undefined,
      })
        .then(done)
        .catch(fail);
    }
  };
  const kind = bot ? "bot" : "user";
  return (
    <Modal
      title={account ? `Edit ${kind}` : `New ${kind}`}
      onClose={onClose}
      onSubmit={save}
      actions={[
        { label: "Cancel", onClick: onClose },
        {
          label: account ? "Save" : "Create",
          primary: true,
          submit: true,
          disabled: !first.trim() || (bot && !username.trim()),
        },
      ]}
    >
      <Field
        label={bot ? "Name" : "First name"}
        value={first}
        onChange={setFirst}
        maxLength={64}
        autoFocus
      />
      {!bot && (
        <Field label="Last name (optional)" value={last} onChange={setLast} maxLength={64} />
      )}
      <Field
        label={bot ? "Username (must end in “bot”)" : "Username (optional)"}
        prefix="@"
        value={username}
        onChange={(v) => setUsername(v.replace(/[^A-Za-z0-9_]/g, ""))}
        maxLength={32}
      />
      {!bot && (
        <Field label="Bio (optional)" value={about} onChange={setAbout} maxLength={70} multiline />
      )}
      {!bot && (
        <Field label="Language code (en, fa, …)" value={lang} onChange={setLang} maxLength={10} />
      )}
      {!bot && (
        <SwitchRow
          title="Telegram Premium"
          subtitle="Longer captions, premium flag for bots to see"
          checked={premium}
          onChange={setPremium}
        />
      )}
      {bot && (
        <SwitchRow
          title="Privacy mode"
          subtitle="In groups, the bot only gets commands, replies to it and service messages"
          checked={privacy}
          onChange={setPrivacy}
        />
      )}
      {bot && account && (
        <SwitchRow
          title="Allow groups"
          subtitle="The bot can be added to groups"
          checked={groups}
          onChange={setGroups}
        />
      )}
      {error && <p className="find-error">{error}</p>}
    </Modal>
  );
}

function readable(message: string): string {
  const known: Record<string, string> = {
    USERNAME_OCCUPIED: "This username is already taken",
    USERNAME_INVALID: "Usernames have 5–32 letters, digits or _, and start with a letter",
    FIRSTNAME_INVALID: "Enter a name",
    ABOUT_TOO_LONG: "The bio is too long",
  };
  return known[message] ?? message;
}
