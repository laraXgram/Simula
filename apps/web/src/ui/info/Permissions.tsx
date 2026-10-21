/** What members may do: the group's default permissions, shared with the restriction
 * screen (where one member's permissions are set). */
import type { tl } from "@simula/schema";
import { useState } from "react";

import { abilities } from "../../state/abilities";
import { Section, SwitchRow } from "../kit/Controls";
import { Icon } from "../kit/Icon";
import { Header } from "./Header";
import { attempt, type ScreenProps } from "./nav";

type Flag = keyof Omit<tl.chatBannedRights, "_" | "until_date">;

/** Telegram's permission list; each may take several flags with it. */
export const PERMISSIONS: readonly {
  label: string;
  flags: Flag[];
  media?: boolean;
  forum?: boolean;
}[] = [
  { label: "Send Messages", flags: ["send_plain"] },
  { label: "Send Photos", flags: ["send_photos"], media: true },
  { label: "Send Videos", flags: ["send_videos"], media: true },
  { label: "Send Files", flags: ["send_docs"], media: true },
  { label: "Send Music", flags: ["send_audios"], media: true },
  { label: "Send Voice Messages", flags: ["send_voices"], media: true },
  { label: "Send Video Messages", flags: ["send_roundvideos"], media: true },
  {
    label: "Send Stickers & GIFs",
    flags: ["send_stickers", "send_gifs", "send_games", "send_inline"],
    media: true,
  },
  { label: "Send Polls", flags: ["send_polls"], media: true },
  { label: "Embed Links", flags: ["embed_links"], media: true },
  { label: "Add Members", flags: ["invite_users"] },
  { label: "Pin Messages", flags: ["pin_messages"] },
  { label: "Change Chat Info", flags: ["change_info"] },
  { label: "Create Topics", flags: ["manage_topics"], forum: true },
];

export function allowedCount(
  banned: tl.chatBannedRights | undefined,
  list: typeof PERMISSIONS,
): number {
  return list.filter((p) => !p.flags.some((f) => banned?.[f])).length;
}

/** `banned` with permission `p` allowed or not; "send media" follows the media ones. */
export function withPermission(
  banned: tl.chatBannedRights,
  p: (typeof PERMISSIONS)[number],
  allowed: boolean,
): tl.chatBannedRights {
  const next: tl.chatBannedRights = { ...banned };
  for (const flag of p.flags) {
    if (allowed) delete next[flag];
    else next[flag] = true;
  }
  const media = PERMISSIONS.filter((x) => x.media).every((x) => x.flags.some((f) => next[f]));
  if (media) next.send_media = true;
  else delete next.send_media;
  if (allowed) delete next.send_messages;
  return next;
}

export function PermissionList({
  banned,
  onChange,
  forum,
  disabled,
}: {
  banned: tl.chatBannedRights;
  onChange: (banned: tl.chatBannedRights) => void;
  forum: boolean;
  /** Permissions the group forbids everyone (can't be granted to one member). */
  disabled?: tl.chatBannedRights;
}) {
  return (
    <>
      {PERMISSIONS.filter((p) => !p.forum || forum).map((p) => {
        const locked = p.flags.some((f) => disabled?.[f]);
        return (
          <SwitchRow
            key={p.label}
            title={p.label}
            subtitle={locked ? "Not allowed for all members" : undefined}
            checked={!locked && !p.flags.some((f) => banned[f])}
            disabled={locked}
            onChange={(allowed) => onChange(withPermission(banned, p, allowed))}
          />
        );
      })}
    </>
  );
}

export function Permissions({ client, snapshot, chatKey, nav, notify }: ScreenProps) {
  const chat = snapshot.chats.get(chatKey);
  const can = abilities(chat);
  const initial: tl.chatBannedRights =
    chat &&
    (chat._ === "chat" || chat._ === "channel") &&
    chat.default_banned_rights?._ === "chatBannedRights"
      ? chat.default_banned_rights
      : { _: "chatBannedRights", until_date: 0 };
  const [banned, setBanned] = useState(initial);
  const changed = JSON.stringify(banned) !== JSON.stringify(initial);
  const save = () =>
    attempt(notify, () =>
      client
        .manage(chatKey)
        .setDefaultPermissions({ ...banned, until_date: 0 })
        .then(nav.pop),
    );
  return (
    <>
      <Header
        title="Permissions"
        onBack={nav.pop}
        actions={
          changed && (
            <button
              type="button"
              className="icon-button accent"
              title="Save"
              aria-label="Save"
              onClick={save}
            >
              <Icon name="check" />
            </button>
          )
        }
      />
      <div className="panel-body">
        <Section
          title="What can members of this group do?"
          note="Admins can always do everything their rights allow."
        >
          <PermissionList banned={banned} onChange={setBanned} forum={can.forum} />
        </Section>
        {!can.basic && can.bans && (
          <Section>
            <SwitchRowLink
              label="Restricted Users"
              onClick={() => nav.push({ name: "members", filter: "banned" })}
            />
            <SwitchRowLink
              label="Removed Users"
              onClick={() => nav.push({ name: "members", filter: "kicked" })}
            />
          </Section>
        )}
      </div>
    </>
  );
}

function SwitchRowLink({ label, onClick }: { label: string; onClick: () => void }) {
  return (
    <button type="button" className="row" onClick={onClick}>
      <span className="row-text">
        <span className="row-title">{label}</span>
      </span>
      <span className="row-chevron">
        <Icon name="chevron" size={20} />
      </span>
    </button>
  );
}
