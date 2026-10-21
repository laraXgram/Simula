/** Adding people: pick from the people the account talks to, or find by @username. */
import type { tl } from "@simula/schema";
import { useEffect, useMemo, useState } from "react";

import { abilities } from "../../state/abilities";
import { Avatar } from "../kit/Avatar";
import { Check, Row } from "../kit/Controls";
import { Icon } from "../kit/Icon";
import { Header } from "./Header";
import { attempt, fullName, readable, type ScreenProps } from "./nav";

export function AddMembers({ client, snapshot, chatKey, nav, notify }: ScreenProps) {
  const can = abilities(snapshot.chats.get(chatKey));
  const [query, setQuery] = useState("");
  const [found, setFound] = useState<tl.user[]>([]);
  const [picked, setPicked] = useState<Set<string>>(new Set());
  const [members, setMembers] = useState<Set<string>>(new Set());
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    client
      .manage(chatKey)
      .members("recent")
      .then((list) => setMembers(new Set(list.map((m) => m.user.id))))
      .catch(() => undefined);
  }, [client, chatKey]);
  const candidates = useMemo(() => {
    const known = snapshot.dialogs
      .filter((d) => d.peer._ === "peerUser")
      .map((d) => (d.peer._ === "peerUser" ? snapshot.users.get(d.peer.user_id) : undefined))
      .filter((u): u is tl.user => u !== undefined && u.id !== snapshot.me?.id);
    const all = [...found, ...known.filter((u) => !found.some((f) => f.id === u.id))];
    const q = query.toLowerCase().replace(/^@/, "");
    return all.filter(
      (u) => !q || fullName(u).toLowerCase().includes(q) || u.username?.toLowerCase().includes(q),
    );
  }, [snapshot, found, query]);
  const search = () => {
    const name = query.trim().replace(/^@/, "");
    if (name.length < 4) return;
    client
      .resolve(name)
      .then((key) => {
        const user = client.userFor(key);
        if (user) setFound((all) => [user, ...all.filter((u) => u.id !== user.id)]);
        else notify("That's not a person or a bot");
      })
      .catch((error: Error) => notify(readable(error.message)));
  };
  const toggle = (id: string) =>
    setPicked((all) => {
      const next = new Set(all);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  const add = () => {
    setBusy(true);
    attempt(notify, () =>
      client
        .manage(chatKey)
        .add([...picked])
        .then(nav.pop),
    ).finally(() => setBusy(false));
  };
  return (
    <>
      <Header title={can.broadcast ? "Add Subscribers" : "Add Members"} onBack={nav.pop} />
      <div className="panel-body">
        <form
          className="search"
          onSubmit={(event) => {
            event.preventDefault();
            search();
          }}
        >
          <Icon name="search" size={20} />
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search, or @username and Enter"
            aria-label="Search people"
          />
        </form>
        {candidates.map((user) => {
          const already = members.has(user.id);
          return (
            <Row
              key={user.id}
              leading={<Avatar id={user.id} name={fullName(user)} size={42} />}
              title={fullName(user)}
              subtitle={
                already
                  ? "already a member"
                  : user.bot
                    ? `bot${user.username ? ` · @${user.username}` : ""}`
                    : user.username
                      ? `@${user.username}`
                      : undefined
              }
              onClick={already ? undefined : () => toggle(user.id)}
              trailing={!already && <Check checked={picked.has(user.id)} />}
            />
          );
        })}
        {candidates.length === 0 && (
          <p className="empty">Type a @username and press Enter to find someone.</p>
        )}
      </div>
      {picked.size > 0 && (
        <div className="panel-footer">
          <button type="button" className="wide-button" disabled={busy} onClick={add}>
            Add {picked.size} {picked.size === 1 ? "person" : "people"}
          </button>
        </div>
      )}
    </>
  );
}
