/**
 * Member lists (everyone, admins, restricted, removed) with search, and the sheet of
 * actions a member's row opens: write to them, promote or edit their rights, restrict,
 * remove, ban, unban.
 */
import { useCallback, useEffect, useState } from "react";

import { abilities } from "../../state/abilities";
import type { Member, MemberFilter } from "../../state/manage";
import { Avatar } from "../kit/Avatar";
import { Row, Section } from "../kit/Controls";
import { Modal, useDialogs } from "../kit/Dialogs";
import { Icon } from "../kit/Icon";
import { Header } from "./Header";
import { attempt, fullName, roleLabel, type ScreenProps, untilLabel, when } from "./nav";

const TITLES: Record<MemberFilter, string> = {
  recent: "Members",
  admins: "Administrators",
  banned: "Restricted Users",
  kicked: "Removed Users",
  bots: "Bots",
};

/** Load `filter`'s members, again when `version` changes. */
export function useMembers(props: ScreenProps, filter: MemberFilter, query = "", version = 0) {
  const { client, chatKey, notify } = props;
  const [members, setMembers] = useState<Member[] | null>(null);
  const reload = useCallback(() => {
    client
      .manage(chatKey)
      .members(filter, query)
      .then(setMembers)
      .catch((error: Error) => {
        setMembers([]);
        notify(error.message);
      });
  }, [client, chatKey, filter, query, notify]);
  useEffect(() => {
    if (version >= 0) reload();
  }, [reload, version]);
  return { members, reload };
}

export function Members(props: ScreenProps & { filter: MemberFilter }) {
  const { snapshot, chatKey, nav, filter } = props;
  const chat = snapshot.chats.get(chatKey);
  const can = abilities(chat);
  const [query, setQuery] = useState("");
  const [version, setVersion] = useState(0);
  const { members } = useMembers(props, filter, query, version);
  const [open, setOpen] = useState<Member | null>(null);
  const title = filter === "recent" && can.broadcast ? "Subscribers" : TITLES[filter];
  return (
    <>
      <Header title={title} onBack={nav.pop} />
      <div className="panel-body">
        <label className="search">
          <Icon name="search" size={20} />
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search"
            aria-label="Search members"
          />
        </label>
        {filter === "recent" && can.invites && (
          <Row
            icon="personAdd"
            title={can.broadcast ? "Add Subscribers" : "Add Members"}
            accent
            onClick={() => nav.push({ name: "add" })}
          />
        )}
        {filter === "admins" && can.promotes && (
          <Row
            icon="admin"
            title="Add Administrator"
            accent
            onClick={() => nav.push({ name: "members", filter: "recent" })}
          />
        )}
        <MemberList members={members} onOpen={setOpen} />
        {members?.length === 0 && (
          <p className="empty">{query ? "No one found" : emptyText(filter)}</p>
        )}
      </div>
      {open && (
        <MemberSheet
          {...props}
          member={open}
          onClose={() => setOpen(null)}
          onChanged={() => {
            setOpen(null);
            setVersion((v) => v + 1);
          }}
        />
      )}
    </>
  );
}

function emptyText(filter: MemberFilter): string {
  switch (filter) {
    case "kicked":
      return "Removed users can't come back through invite links.";
    case "banned":
      return "No one is restricted.";
    default:
      return "No one yet.";
  }
}

export function MemberList({
  members,
  onOpen,
}: {
  members: readonly Member[] | null;
  onOpen: (member: Member) => void;
}) {
  if (members === null) return <p className="empty">Loading…</p>;
  return (
    <div className="members">
      {members.map((member) => (
        <Row
          key={member.user.id}
          leading={<Avatar id={member.user.id} name={fullName(member.user)} size={42} />}
          title={fullName(member.user)}
          subtitle={memberSubtitle(member)}
          detail={roleLabel(member)}
          onClick={() => onOpen(member)}
        />
      ))}
    </div>
  );
}

function memberSubtitle(member: Member): string {
  if (member.role === "banned")
    return `Removed ${member.bannedRights ? untilLabel(member.bannedRights.until_date) : ""}`.trim();
  if (member.role === "restricted")
    return `Restricted ${member.bannedRights ? untilLabel(member.bannedRights.until_date) : ""}`.trim();
  if (member.user.bot) return member.user.username ? `bot · @${member.user.username}` : "bot";
  if (member.user.username) return `@${member.user.username}`;
  return member.date ? `joined ${when(member.date)}` : "";
}

/** What can be done with one member. */
export function MemberSheet(
  props: ScreenProps & { member: Member; onClose: () => void; onChanged: () => void },
) {
  const { client, snapshot, chatKey, nav, notify, onOpenChat, member, onClose, onChanged } = props;
  const dialogs = useDialogs();
  const can = abilities(snapshot.chats.get(chatKey));
  const manager = client.manage(chatKey);
  const isMe = member.user.id === snapshot.me?.id;
  const name = fullName(member.user);
  const chatName = can.broadcast ? "channel" : "group";
  const run = (action: () => Promise<unknown>) =>
    attempt(notify, async () => {
      await action();
      onChanged();
    });
  const isAdmin = member.role === "admin" || member.role === "creator";
  const editable = member.role !== "creator" && !isMe;
  const rows = [];
  if (!isMe) {
    rows.push(
      <Row
        key="write"
        icon="chat"
        title="Send Message"
        onClick={() => {
          onClose();
          onOpenChat(client.keyFor(member.user.id));
        }}
      />,
    );
  }
  if (can.promotes && editable && member.role !== "banned") {
    rows.push(
      <Row
        key="promote"
        icon="admin"
        title={isAdmin ? "Edit Admin Rights" : "Promote to Admin"}
        onClick={() => {
          onClose();
          nav.push({ name: "admin", member });
        }}
      />,
    );
  }
  if (can.promotes && can.basic && member.role === "admin") {
    rows.push(
      <Row
        key="dismiss"
        icon="admin"
        title="Dismiss Admin"
        onClick={() => run(() => manager.dismiss(member.user.id))}
      />,
    );
  }
  if (
    can.bans &&
    editable &&
    !can.basic &&
    !can.broadcast &&
    (member.role === "member" || member.role === "restricted")
  ) {
    rows.push(
      <Row
        key="restrict"
        icon="lock"
        title={member.role === "restricted" ? "Change Permissions" : "Restrict User"}
        onClick={() => {
          onClose();
          nav.push({ name: "restrict", member });
        }}
      />,
    );
  }
  if ((member.role === "banned" || member.role === "restricted") && can.bans) {
    rows.push(
      <Row
        key="unban"
        icon="unlock"
        title={member.role === "banned" ? "Unban" : "Lift Restrictions"}
        onClick={() => run(() => manager.unban(member.user.id))}
      />,
    );
  }
  if (
    can.bans &&
    editable &&
    member.role !== "banned" &&
    (!isAdmin || member.canEdit || can.creator)
  ) {
    rows.push(
      <Row
        key="remove"
        icon="personRemove"
        title={`Remove from ${chatName}`}
        danger
        onClick={async () => {
          const ok = await dialogs.confirm({
            title: `Remove ${name}?`,
            text: `${name} will be removed from the ${chatName} and can join again with an invite link.`,
            ok: "Remove",
            danger: true,
          });
          if (ok) run(() => manager.remove(member.user.id));
        }}
      />,
    );
    if (!can.basic) {
      rows.push(
        <Row
          key="ban"
          icon="block"
          title={`Ban from ${chatName}`}
          danger
          onClick={async () => {
            const ok = await dialogs.confirm({
              title: `Ban ${name}?`,
              text: `${name} will be removed and won't be able to return until an admin unbans them.`,
              ok: "Ban",
              danger: true,
            });
            if (ok) run(() => manager.ban(member.user.id));
          }}
        />,
      );
    }
  }
  return (
    <Modal title={name} onClose={onClose} actions={[{ label: "Close", onClick: onClose }]}>
      <div className="sheet-head">
        <Avatar id={member.user.id} name={name} size={56} />
        <div className="sheet-sub">
          {[
            roleLabel(member) ?? (member.user.bot ? "bot" : "member"),
            member.user.username && `@${member.user.username}`,
          ]
            .filter(Boolean)
            .join(" · ")}
        </div>
      </div>
      <Section>{rows.length > 0 ? rows : <Row title="Nothing you can change here" />}</Section>
    </Modal>
  );
}
