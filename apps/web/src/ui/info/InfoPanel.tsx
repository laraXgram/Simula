/**
 * The right-hand column: a chat's info, and the screens behind it (edit, members,
 * admins, permissions, links, requests), stacked like Telegram's: each opens on top and
 * the arrow goes back.
 */
import { useEffect, useState } from "react";

import type { Client } from "../../state/client";
import type { Snapshot } from "../../state/store";
import { AddMembers } from "./AddMembers";
import { AdminRights } from "./AdminRights";
import { ChatInfo } from "./ChatInfo";
import { ChatType } from "./ChatType";
import { EditChat } from "./EditChat";
import { LinkDetails, Links } from "./Links";
import { Members } from "./Members";
import type { Screen, ScreenProps } from "./nav";
import { Permissions } from "./Permissions";
import { Requests } from "./Requests";
import { Restrict } from "./Restrict";

interface Props {
  client: Client;
  snapshot: Snapshot;
  chatKey: string;
  /** The screen it opens on. */
  start: Screen;
  onClose: () => void;
  notify: ScreenProps["notify"];
  onOpenChat: (key: string) => void;
}

export function InfoPanel({
  client,
  snapshot,
  chatKey,
  start,
  onClose,
  notify,
  onOpenChat,
}: Props) {
  const [stack, setStack] = useState<Screen[]>([start]);
  useEffect(() => setStack([start]), [start]);
  useEffect(() => {
    if (chatKey.startsWith("c")) client.loadDetails(chatKey).catch(() => undefined);
  }, [client, chatKey]);
  const nav = {
    push: (screen: Screen) => setStack((all) => [...all, screen]),
    pop: () => setStack((all) => (all.length > 1 ? all.slice(0, -1) : all)),
    close: onClose,
  };
  const props: ScreenProps = { client, snapshot, chatKey, nav, notify, onOpenChat };
  const screen = stack.at(-1) ?? start;
  const rootScreen = stack.length === 1;
  const navForScreen = rootScreen ? { ...nav, pop: onClose } : nav;
  const p = { ...props, nav: navForScreen };
  return (
    <aside className="info-panel" aria-label="Chat info">
      {screen.name === "info" && <ChatInfo {...p} />}
      {screen.name === "edit" && <EditChat {...p} />}
      {screen.name === "type" && <ChatType {...p} />}
      {screen.name === "members" && <Members {...p} filter={screen.filter} />}
      {screen.name === "admin" && <AdminRights {...p} member={screen.member} />}
      {screen.name === "restrict" && <Restrict {...p} member={screen.member} />}
      {screen.name === "permissions" && <Permissions {...p} />}
      {screen.name === "links" && <Links {...p} />}
      {screen.name === "link" && <LinkDetails {...p} link={screen.link} />}
      {screen.name === "requests" && <Requests {...p} />}
      {screen.name === "add" && <AddMembers {...p} />}
    </aside>
  );
}
