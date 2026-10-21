/** A chat's info: who or what it is, its description and link, and its members. */
import type { tl } from "@simula/schema";
import { useEffect, useState } from "react";

import { abilities } from "../../state/abilities";
import type { Member } from "../../state/manage";
import { displayName } from "../../state/peers";
import { Avatar } from "../kit/Avatar";
import { Row, Section } from "../kit/Controls";
import { useDialogs } from "../kit/Dialogs";
import { Icon } from "../kit/Icon";
import { Header } from "./Header";
import { MemberList, MemberSheet, useMembers } from "./Members";
import { attempt, type ScreenProps } from "./nav";

export function ChatInfo(props: ScreenProps) {
  return props.chatKey.startsWith("u") ? <UserInfo {...props} /> : <GroupInfo {...props} />;
}

function copy(text: string, notify: ScreenProps["notify"], what: string) {
  void navigator.clipboard?.writeText(text).catch(() => undefined);
  notify(`${what} copied to clipboard`);
}

function GroupInfo(props: ScreenProps) {
  const { client, snapshot, chatKey, nav, notify } = props;
  const dialogs = useDialogs();
  const chat = snapshot.chats.get(chatKey);
  const details = snapshot.details.get(chatKey);
  const can = abilities(chat);
  const [version, setVersion] = useState(0);
  const { members } = useMembers(props, "recent", "", can.seesMembers ? version : -1);
  const [open, setOpen] = useState<Member | null>(null);
  if (!chat) return null;
  const kind = can.broadcast ? "Channel" : "Group";
  const title = chat.title;
  const username = chat._ === "channel" ? chat.username : undefined;
  const count =
    details?.count ??
    (chat._ === "chat"
      ? chat.participants_count
      : chat._ === "channel"
        ? chat.participants_count
        : 0) ??
    0;
  const subtitle = `${count} ${can.broadcast ? (count === 1 ? "subscriber" : "subscribers") : count === 1 ? "member" : "members"}`;
  const link = username ? `https://t.me/${username}` : details?.inviteLink;
  const leave = async () => {
    const ok = await dialogs.confirm({
      title: `Leave ${kind.toLowerCase()}?`,
      text: `Are you sure you want to leave «${title}»?`,
      ok: "Leave",
      danger: true,
    });
    if (ok) attempt(notify, () => client.leave(chatKey).then(nav.close));
  };
  const remove = async () => {
    const ok = await dialogs.confirm({
      title: `Delete ${kind.toLowerCase()}?`,
      text: `«${title}» will be deleted for all members. This can't be undone.`,
      ok: "Delete",
      danger: true,
    });
    if (ok) attempt(notify, () => client.manage(chatKey).delete().then(nav.close));
  };
  return (
    <>
      <Header
        title={`${kind} Info`}
        onBack={nav.pop}
        closes
        actions={
          (can.changesInfo || can.admin) && (
            <button
              type="button"
              className="icon-button"
              title="Edit"
              aria-label="Edit"
              onClick={() => nav.push({ name: "edit" })}
            >
              <Icon name="edit" />
            </button>
          )
        }
      />
      <div className="panel-body">
        <div className="profile">
          <Avatar id={chat.id} name={title} size={110} />
          <h3 className="profile-name" dir="auto">
            {title}
          </h3>
          <p className="profile-sub">{subtitle}</p>
        </div>
        <Section>
          {details?.about && (
            <Row
              icon="info"
              title={<span className="multiline">{details.about}</span>}
              subtitle="Description"
            />
          )}
          {link && (
            <Row
              icon="link"
              title={link.replace(/^https:\/\//, "")}
              subtitle={username ? "Link" : "Invite link"}
              onClick={() => copy(link, notify, "Link")}
              trailing={<Icon name="copy" size={20} />}
            />
          )}
          {can.forum && (
            <Row icon="forum" title="Topics" subtitle="Messages are organized in topics" />
          )}
        </Section>
        {can.admin && !can.basic && (
          <Section>
            <Row
              icon="admin"
              title="Administrators"
              detail={details?.admins}
              chevron
              onClick={() => nav.push({ name: "members", filter: "admins" })}
            />
            {can.bans && (
              <Row
                icon="block"
                title="Removed Users"
                detail={details?.kicked || undefined}
                chevron
                onClick={() => nav.push({ name: "members", filter: "kicked" })}
              />
            )}
            {can.links && (
              <Row
                icon="link"
                title="Invite Links"
                chevron
                onClick={() => nav.push({ name: "links" })}
              />
            )}
            {(details?.requests ?? 0) > 0 && can.links && (
              <Row
                icon="requests"
                title="Join Requests"
                detail={details?.requests}
                chevron
                onClick={() => nav.push({ name: "requests" })}
              />
            )}
          </Section>
        )}
        {can.seesMembers && (
          <Section title={can.broadcast ? `${count} subscribers` : `${count} members`}>
            {can.invites && (
              <Row
                icon="personAdd"
                title={can.broadcast ? "Add Subscribers" : "Add Members"}
                accent
                onClick={() => nav.push({ name: "add" })}
              />
            )}
            <MemberList members={members ? members.slice(0, 50) : null} onOpen={setOpen} />
            {members && members.length > 50 && (
              <Row
                title="Show all members"
                accent
                onClick={() => nav.push({ name: "members", filter: "recent" })}
              />
            )}
          </Section>
        )}
        {can.member && (
          <Section>
            {can.creator && !can.basic ? (
              <Row icon="delete" title={`Delete ${kind}`} danger onClick={remove} />
            ) : (
              <Row icon="leave" title={`Leave ${kind}`} danger onClick={leave} />
            )}
          </Section>
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

function UserInfo({ client, snapshot, chatKey, nav, notify }: ScreenProps) {
  const user = client.userFor(chatKey);
  const [about, setAbout] = useState<string | undefined>();
  useEffect(() => {
    if (!user) return;
    client.session
      .call("users.getFullUser", {
        id:
          user.id === snapshot.me?.id
            ? { _: "inputUserSelf" }
            : { _: "inputUser", user_id: user.id, access_hash: user.access_hash ?? "0" },
      })
      .then((full) => {
        const info = full.full_user;
        if (info._ !== "userFull") return;
        const bot = info.bot_info?._ === "botInfo" ? info.bot_info : undefined;
        setAbout(bot?.description || info.about);
      })
      .catch(() => undefined);
  }, [client, user, snapshot.me?.id]);
  if (!user) return null;
  const name = displayName(user);
  return (
    <>
      <Header title={user.bot ? "Bot Info" : "User Info"} onBack={nav.pop} closes />
      <div className="panel-body">
        <div className="profile">
          <Avatar id={user.id} name={name} size={110} />
          <h3 className="profile-name" dir="auto">
            {name}
          </h3>
          <p className="profile-sub">{user.bot ? "bot" : "last seen recently"}</p>
        </div>
        <Section>
          {about && (
            <Row
              icon="info"
              title={<span className="multiline">{about}</span>}
              subtitle={user.bot ? "Description" : "Bio"}
            />
          )}
          {user.username && (
            <Row
              icon="person"
              title={`@${user.username}`}
              subtitle="Username"
              onClick={() => copy(`@${user.username}`, notify, "Username")}
              trailing={<Icon name="copy" size={20} />}
            />
          )}
          {user.phone && <Row icon="info" title={`+${user.phone}`} subtitle="Mobile" />}
          <Row
            icon="key"
            title={user.id}
            subtitle="ID"
            onClick={() => copy(user.id, notify, "ID")}
            trailing={<Icon name="copy" size={20} />}
          />
        </Section>
      </div>
    </>
  );
}

export type { tl };
