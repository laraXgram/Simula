/** Promote a member or change an admin's rights and title (supergroups and channels);
 * in basic groups admins hold every right. */
import type { tl } from "@simula/schema";
import { useState } from "react";

import { abilities } from "../../state/abilities";
import { fullAdminRights, type Member } from "../../state/manage";
import { Avatar } from "../kit/Avatar";
import { Row, Section, SwitchRow } from "../kit/Controls";
import { Field, useDialogs } from "../kit/Dialogs";
import { Icon } from "../kit/Icon";
import { Header } from "./Header";
import { attempt, fullName, type ScreenProps } from "./nav";

type Right = keyof Omit<tl.chatAdminRights, "_">;

const GROUP_RIGHTS: readonly [Right, string, boolean?][] = [
  ["change_info", "Change Group Info"],
  ["delete_messages", "Delete Messages"],
  ["ban_users", "Ban Users"],
  ["invite_users", "Invite Users via Link"],
  ["pin_messages", "Pin Messages"],
  ["manage_topics", "Manage Topics", true],
  ["manage_call", "Manage Video Chats"],
  ["anonymous", "Remain Anonymous"],
  ["add_admins", "Add New Admins"],
];

const CHANNEL_RIGHTS: readonly [Right, string][] = [
  ["change_info", "Change Channel Info"],
  ["post_messages", "Post Messages"],
  ["edit_messages", "Edit Messages of Others"],
  ["delete_messages", "Delete Messages of Others"],
  ["invite_users", "Add Subscribers"],
  ["manage_call", "Manage Live Streams"],
  ["add_admins", "Add New Admins"],
];

export function AdminRights(props: ScreenProps & { member: Member }) {
  const { client, snapshot, chatKey, nav, notify, member } = props;
  const dialogs = useDialogs();
  const chat = snapshot.chats.get(chatKey);
  const can = abilities(chat);
  const isAdmin = member.role === "admin";
  const mine = can.adminRights;
  const [rights, setRights] = useState<tl.chatAdminRights>(() => {
    if (member.adminRights) return member.adminRights;
    // New admins get what Telegram proposes, limited to the promoter's own rights.
    const proposed = fullAdminRights(can.broadcast);
    const out: tl.chatAdminRights = { _: "chatAdminRights", other: true };
    for (const key of Object.keys(proposed) as (Right | "_")[]) {
      if (key === "_" || key === "add_admins" || key === "anonymous") continue;
      if (proposed[key] && (can.creator || mine?.[key])) out[key] = true;
    }
    return out;
  });
  const [rank, setRank] = useState(member.rank ?? "");
  const manager = client.manage(chatKey);
  const name = fullName(member.user);
  const list = can.broadcast
    ? CHANNEL_RIGHTS
    : GROUP_RIGHTS.filter(([, , forum]) => !forum || can.forum);
  const save = () =>
    attempt(notify, () =>
      manager.promote(member.user.id, { ...rights, other: true }, rank.trim()).then(nav.pop),
    );
  const dismiss = async () => {
    const ok = await dialogs.confirm({
      title: "Dismiss admin?",
      text: `${name} will lose all admin rights.`,
      ok: "Dismiss",
      danger: true,
    });
    if (ok) attempt(notify, () => manager.dismiss(member.user.id).then(nav.pop));
  };
  return (
    <>
      <Header
        title={isAdmin ? "Admin Rights" : "Promote to Admin"}
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
          subtitle={
            member.user.bot ? "bot" : member.user.username ? `@${member.user.username}` : undefined
          }
        />
        {can.basic ? (
          <Section note="In basic groups admins can do everything except add new admins. Upgrade to a supergroup to choose rights one by one.">
            <Row icon="admin" title="All admin rights" />
          </Section>
        ) : (
          <Section
            title="What can this admin do?"
            note="You can only grant rights you have yourself."
          >
            {list.map(([key, label]) => (
              <SwitchRow
                key={key}
                title={label}
                checked={rights[key] === true}
                disabled={!can.creator && !mine?.[key]}
                onChange={(on) => setRights((r) => ({ ...r, [key]: on || undefined }))}
              />
            ))}
          </Section>
        )}
        {!can.basic && (
          <div className="fields">
            <Field
              label="Custom title"
              value={rank}
              onChange={setRank}
              maxLength={16}
              hint={`A title shown instead of “admin”, like “${can.broadcast ? "editor" : "moderator"}”.`}
            />
          </div>
        )}
        {isAdmin && member.canEdit && (
          <Section>
            <Row icon="admin" title="Dismiss Admin" danger onClick={dismiss} />
          </Section>
        )}
      </div>
    </>
  );
}
