/** One member's permissions in a supergroup, for a while or for good. */
import type { tl } from "@simula/schema";
import { useState } from "react";

import { abilities } from "../../state/abilities";
import type { Member } from "../../state/manage";
import { Avatar } from "../kit/Avatar";
import { Check, Row, Section } from "../kit/Controls";
import { useDialogs } from "../kit/Dialogs";
import { Icon } from "../kit/Icon";
import { Header } from "./Header";
import { attempt, fullName, type ScreenProps } from "./nav";
import { PermissionList } from "./Permissions";

const DURATIONS: readonly [string, number][] = [
  ["Forever", 0],
  ["1 day", 86400],
  ["1 week", 7 * 86400],
  ["1 month", 30 * 86400],
];

export function Restrict(props: ScreenProps & { member: Member }) {
  const { client, snapshot, chatKey, nav, notify, member } = props;
  const dialogs = useDialogs();
  const chat = snapshot.chats.get(chatKey);
  const can = abilities(chat);
  const defaults =
    chat?._ === "channel" && chat.default_banned_rights?._ === "chatBannedRights"
      ? chat.default_banned_rights
      : undefined;
  const [banned, setBanned] = useState<tl.chatBannedRights>(
    member.bannedRights ?? { _: "chatBannedRights", until_date: 0 },
  );
  const [duration, setDuration] = useState(0);
  const manager = client.manage(chatKey);
  const name = fullName(member.user);
  const save = () =>
    attempt(notify, () =>
      manager
        .restrict(member.user.id, {
          ...banned,
          until_date: duration ? Math.floor(Date.now() / 1000) + duration : 0,
        })
        .then(nav.pop),
    );
  const ban = async () => {
    const ok = await dialogs.confirm({
      title: `Ban ${name}?`,
      text: `${name} will be removed and won't be able to return until unbanned.`,
      ok: "Ban",
      danger: true,
    });
    if (ok) attempt(notify, () => manager.ban(member.user.id).then(nav.pop));
  };
  return (
    <>
      <Header
        title="User Permissions"
        onBack={nav.pop}
        actions={
          <button
            type="button"
            className="icon-button accent"
            title="Save"
            aria-label="Save"
            onClick={save}
          >
            <Icon name="check" />
          </button>
        }
      />
      <div className="panel-body">
        <Row
          leading={<Avatar id={member.user.id} name={name} size={46} />}
          title={name}
          subtitle={member.user.username ? `@${member.user.username}` : undefined}
        />
        <Section title="What can this user do?">
          <PermissionList
            banned={banned}
            onChange={setBanned}
            forum={can.forum}
            disabled={defaults}
          />
        </Section>
        <Section title="Duration" note="Restrictions end by themselves after this time.">
          {DURATIONS.map(([label, seconds]) => (
            <Row
              key={label}
              leading={<Check checked={duration === seconds} />}
              title={label}
              onClick={() => setDuration(seconds)}
            />
          ))}
        </Section>
        <Section>
          <Row icon="block" title="Ban and Remove from Group" danger onClick={ban} />
        </Section>
      </div>
    </>
  );
}
