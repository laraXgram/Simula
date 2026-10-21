/** Editing a group or channel: name, description, type, topics, and the screens for its
 * permissions, admins, members, links, requests and removed users. */
import { useEffect, useState } from "react";

import { abilities } from "../../state/abilities";
import { Avatar } from "../kit/Avatar";
import { Row, Section, SwitchRow } from "../kit/Controls";
import { Field, useDialogs } from "../kit/Dialogs";
import { Icon } from "../kit/Icon";
import { Header } from "./Header";
import { attempt, type ScreenProps } from "./nav";
import { allowedCount, PERMISSIONS } from "./Permissions";

export function EditChat(props: ScreenProps) {
  const { client, snapshot, chatKey, nav, notify, onOpenChat } = props;
  const dialogs = useDialogs();
  const chat = snapshot.chats.get(chatKey);
  const details = snapshot.details.get(chatKey);
  const can = abilities(chat);
  const [title, setTitle] = useState(chat && "title" in chat ? chat.title : "");
  const [about, setAbout] = useState(details?.about ?? "");
  const [saving, setSaving] = useState(false);
  useEffect(() => setAbout(details?.about ?? ""), [details?.about]);
  if (!chat || (chat._ !== "chat" && chat._ !== "channel")) return null;
  const kind = can.broadcast ? "Channel" : "Group";
  const changed = title.trim() !== chat.title || about !== (details?.about ?? "");
  const manager = client.manage(chatKey);
  const save = () => {
    setSaving(true);
    attempt(notify, async () => {
      if (title.trim() !== chat.title) await manager.setTitle(title.trim());
      if (about !== (details?.about ?? "")) await manager.setAbout(about);
      nav.pop();
    }).finally(() => setSaving(false));
  };
  const username = chat._ === "channel" ? chat.username : undefined;
  const defaults =
    chat.default_banned_rights?._ === "chatBannedRights" ? chat.default_banned_rights : undefined;
  const permissions = PERMISSIONS.filter((p) => !p.forum || can.forum);
  const upgrade = async () => {
    const ok = await dialogs.confirm({
      title: "Upgrade to supergroup?",
      text: "Supergroups can have public links, topics, admins with chosen rights, and much more. Members move along; this can't be undone.",
      ok: "Upgrade",
    });
    if (ok)
      attempt(notify, () =>
        client.upgrade(chatKey).then((key) => {
          nav.close();
          onOpenChat(key);
        }),
      );
  };
  // Like Telegram, a basic group becomes a supergroup when it needs to: to go public or
  // to have topics.
  const upgradeFor = async (reason: string): Promise<string | null> => {
    const ok = await dialogs.confirm({
      title: "Upgrade to supergroup?",
      text: `${reason} The group becomes a supergroup; its members move along. This can't be undone.`,
      ok: "Upgrade",
    });
    if (!ok) return null;
    try {
      return await client.upgrade(chatKey);
    } catch (error) {
      notify((error as Error).message);
      return null;
    }
  };
  const makePublic = async () => {
    const key = await upgradeFor("Only supergroups can be public.");
    if (key) onOpenChat(key, { name: "type" });
  };
  const toggleTopics = async (on: boolean) => {
    if (can.basic) {
      const key = await upgradeFor("Only supergroups can have topics.");
      if (key)
        attempt(notify, async () => {
          await client.toggleForum(key, true);
          onOpenChat(key);
        });
      return;
    }
    const ok = await dialogs.confirm({
      title: on ? "Turn on topics?" : "Turn off topics?",
      text: on
        ? "The group's messages will be organized in topics. Existing messages move to General."
        : "Topics will be hidden; their messages stay in the group.",
      ok: on ? "Turn On" : "Turn Off",
    });
    if (ok) attempt(notify, () => client.toggleForum(chatKey, on));
  };
  const remove = async () => {
    const ok = await dialogs.confirm({
      title: `Delete ${kind.toLowerCase()}?`,
      text: `«${chat.title}» will be deleted for all members. This can't be undone.`,
      ok: "Delete",
      danger: true,
    });
    if (ok) attempt(notify, () => manager.delete().then(nav.close));
  };
  return (
    <>
      <Header
        title="Edit"
        onBack={nav.pop}
        actions={
          changed && (
            <button
              type="button"
              className="icon-button accent"
              title="Save"
              aria-label="Save"
              disabled={saving}
              onClick={save}
            >
              <Icon name="check" />
            </button>
          )
        }
      />
      <div className="panel-body">
        <div className="profile compact">
          <Avatar id={chat.id} name={title || chat.title} size={96} />
        </div>
        {can.changesInfo && (
          <div className="fields">
            <Field label={`${kind} name`} value={title} onChange={setTitle} maxLength={128} />
            <Field
              label="Description (optional)"
              value={about}
              onChange={setAbout}
              multiline
              maxLength={255}
            />
          </div>
        )}
        <Section>
          {can.creator && (
            <Row
              icon={username ? "public" : "lock"}
              title={`${kind} Type`}
              detail={username ? "Public" : "Private"}
              chevron
              onClick={() => (can.basic ? makePublic() : nav.push({ name: "type" }))}
            />
          )}
          {can.creator && !can.broadcast && (
            <SwitchRow
              icon="forum"
              title="Topics"
              subtitle="Organize messages in topics"
              checked={can.forum}
              onChange={toggleTopics}
            />
          )}
          {can.creator && can.basic && (
            <Row icon="upgrade" title="Upgrade to Supergroup" accent onClick={upgrade} />
          )}
        </Section>
        <Section>
          {!can.broadcast && (can.basic ? can.admin : can.bans) && (
            <Row
              icon="key"
              title="Permissions"
              detail={`${allowedCount(defaults, permissions)}/${permissions.length}`}
              chevron
              onClick={() => nav.push({ name: "permissions" })}
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
          {can.links && (details?.requests ?? 0) > 0 && (
            <Row
              icon="requests"
              title="Join Requests"
              detail={details?.requests}
              chevron
              onClick={() => nav.push({ name: "requests" })}
            />
          )}
          <Row
            icon="admin"
            title="Administrators"
            detail={details?.admins}
            chevron
            onClick={() => nav.push({ name: "members", filter: "admins" })}
          />
          {can.seesMembers && (
            <Row
              icon="group"
              title={can.broadcast ? "Subscribers" : "Members"}
              detail={details?.count}
              chevron
              onClick={() => nav.push({ name: "members", filter: "recent" })}
            />
          )}
          {!can.basic && can.bans && !can.broadcast && (
            <Row
              icon="lock"
              title="Restricted Users"
              detail={details?.restricted || undefined}
              chevron
              onClick={() => nav.push({ name: "members", filter: "banned" })}
            />
          )}
          {!can.basic && can.bans && (
            <Row
              icon="block"
              title="Removed Users"
              detail={details?.kicked || undefined}
              chevron
              onClick={() => nav.push({ name: "members", filter: "kicked" })}
            />
          )}
        </Section>
        {can.creator && !can.basic && (
          <Section>
            <Row icon="delete" title={`Delete ${kind}`} danger onClick={remove} />
          </Section>
        )}
      </div>
    </>
  );
}
