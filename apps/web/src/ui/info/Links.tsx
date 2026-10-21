/** Invite links: the primary link, extra links (with a name, an expiry, a usage limit or
 * approval), who joined through each, and revoked links. */
import { useCallback, useEffect, useState } from "react";

import type { Importer, Link, LinkOptions } from "../../state/manage";
import { Avatar } from "../kit/Avatar";
import { Check, Row, Section, SwitchRow } from "../kit/Controls";
import { Field, Modal, useDialogs } from "../kit/Dialogs";
import { Icon } from "../kit/Icon";
import { Header } from "./Header";
import { attempt, fullName, type ScreenProps, when } from "./nav";

function copy(link: string, notify: ScreenProps["notify"]) {
  void navigator.clipboard?.writeText(link).catch(() => undefined);
  notify("Link copied to clipboard");
}

/** "3 joined · expires in 2 days", "expired", "limit reached". */
function linkState(link: Link): string {
  const now = Date.now() / 1000;
  const parts: string[] = [];
  const joined = link.usage ?? 0;
  parts.push(joined ? `${joined} joined` : "no one joined yet");
  if (link.requested) parts.push(`${link.requested} requested`);
  if (link.revoked) return [...parts, "revoked"].join(" · ");
  if (link.expire_date && link.expire_date <= now) return [...parts, "expired"].join(" · ");
  if (link.usage_limit && joined >= link.usage_limit)
    return [...parts, "limit reached"].join(" · ");
  if (link.usage_limit) parts.push(`${link.usage_limit - joined} left`);
  if (link.expire_date) parts.push(`expires ${when(link.expire_date)}`);
  return parts.join(" · ");
}

function short(link: string): string {
  return link.replace(/^https:\/\//, "");
}

export function Links(props: ScreenProps) {
  const { client, chatKey, nav, notify } = props;
  const dialogs = useDialogs();
  const manager = client.manage(chatKey);
  const [live, setLive] = useState<Link[] | null>(null);
  const [revoked, setRevoked] = useState<Link[]>([]);
  const [editing, setEditing] = useState<Link | "new" | null>(null);
  const reload = useCallback(() => {
    const m = client.manage(chatKey);
    Promise.all([m.links(false), m.links(true)])
      .then(([l, r]) => {
        setLive(l);
        setRevoked(r);
      })
      .catch((error: Error) => notify(error.message));
  }, [client, chatKey, notify]);
  useEffect(reload, [reload]);
  const primary = live?.find((l) => l.permanent);
  const others = live?.filter((l) => !l.permanent) ?? [];
  const revoke = async (link: Link) => {
    const ok = await dialogs.confirm({
      title: "Revoke link?",
      text: link.permanent
        ? "The link will stop working; a new primary link replaces it."
        : "The link will stop working. People who joined stay.",
      ok: "Revoke",
      danger: true,
    });
    if (ok) attempt(notify, () => manager.revokeLink(link.link).then(reload));
  };
  return (
    <>
      <Header title="Invite Links" onBack={nav.pop} />
      <div className="panel-body">
        <Section
          title="Primary link"
          note="Anyone with this link can join. You can revoke it any time."
        >
          {primary ? (
            <div className="link-card">
              <span className="link-text" dir="ltr">
                {short(primary.link)}
              </span>
              <div className="link-buttons">
                <button
                  type="button"
                  className="pill-button primary"
                  onClick={() => copy(primary.link, notify)}
                >
                  <Icon name="copy" size={18} /> Copy
                </button>
                <button type="button" className="pill-button" onClick={() => revoke(primary)}>
                  Revoke
                </button>
              </div>
              <span className="link-state">{linkState(primary)}</span>
            </div>
          ) : (
            <Row
              icon="link"
              title="Create primary link"
              accent
              onClick={() =>
                attempt(notify, () =>
                  client.session
                    .call("messages.exportChatInvite", {
                      peer: client.inputPeerOf(chatKey) ?? { _: "inputPeerEmpty" },
                      legacy_revoke_permanent: true,
                    })
                    .then(reload),
                )
              }
            />
          )}
        </Section>
        <Section
          title="Additional links"
          note="Links can have a name, expire, admit a number of people, or ask admins to approve each request."
        >
          <Row icon="add" title="Create a New Link" accent onClick={() => setEditing("new")} />
          {others.map((link) => (
            <Row
              key={link.link}
              icon="link"
              title={link.title || short(link.link)}
              subtitle={linkState(link)}
              chevron
              onClick={() => nav.push({ name: "link", link })}
            />
          ))}
        </Section>
        {revoked.length > 0 && (
          <Section title="Revoked links">
            {revoked.map((link) => (
              <Row
                key={link.link}
                icon="link"
                title={link.title || short(link.link)}
                subtitle={linkState(link)}
                onClick={() => nav.push({ name: "link", link })}
              />
            ))}
            <Row
              icon="delete"
              title="Delete All Revoked Links"
              danger
              onClick={async () => {
                const ok = await dialogs.confirm({
                  title: "Delete all revoked links?",
                  ok: "Delete",
                  danger: true,
                });
                if (ok) attempt(notify, () => manager.deleteRevokedLinks().then(reload));
              }}
            />
          </Section>
        )}
      </div>
      {editing && (
        <LinkEditor
          link={editing === "new" ? undefined : editing}
          onClose={() => setEditing(null)}
          onSave={(options) =>
            attempt(notify, async () => {
              if (editing === "new") {
                const created = await manager.createLink(options);
                copy(created.link, notify);
              } else {
                await manager.editLink(editing.link, options);
              }
              setEditing(null);
              reload();
            })
          }
        />
      )}
    </>
  );
}

const EXPIRY: readonly [string, number][] = [
  ["No limit", 0],
  ["1 hour", 3600],
  ["1 day", 86400],
  ["1 week", 7 * 86400],
];
const LIMITS: readonly [string, number][] = [
  ["No limit", 0],
  ["1", 1],
  ["10", 10],
  ["100", 100],
];

/** Create or edit a link. */
export function LinkEditor({
  link,
  onClose,
  onSave,
}: {
  link?: Link;
  onClose: () => void;
  onSave: (options: LinkOptions) => void;
}) {
  const [title, setTitle] = useState(link?.title ?? "");
  const [request, setRequest] = useState(link?.request_needed === true);
  const now = Math.floor(Date.now() / 1000);
  const [expiry, setExpiry] = useState(0);
  const [limit, setLimit] = useState(link?.usage_limit ?? 0);
  return (
    <Modal
      title={link ? "Edit Link" : "New Link"}
      onClose={onClose}
      onSubmit={() =>
        onSave({
          title: title.trim(),
          requestNeeded: request,
          expireDate: expiry
            ? now + expiry
            : link?.expire_date && link.expire_date > now
              ? link.expire_date
              : 0,
          usageLimit: request ? 0 : limit,
        })
      }
      actions={[
        { label: "Cancel", onClick: onClose },
        { label: link ? "Save" : "Create", primary: true, submit: true },
      ]}
    >
      <Field
        label="Link name (optional)"
        value={title}
        onChange={setTitle}
        maxLength={32}
        hint="Only admins see the name."
      />
      <SwitchRow
        title="Request admin approval"
        subtitle="New members join after an admin approves them."
        checked={request}
        onChange={setRequest}
      />
      <p className="choice-title">Expires</p>
      <div className="chips">
        {EXPIRY.map(([label, seconds]) => (
          <button
            key={label}
            type="button"
            className={expiry === seconds ? "chip on" : "chip"}
            onClick={() => setExpiry(seconds)}
          >
            {label}
          </button>
        ))}
      </div>
      {!request && (
        <>
          <p className="choice-title">Number of uses</p>
          <div className="chips">
            {LIMITS.map(([label, count]) => (
              <button
                key={label}
                type="button"
                className={limit === count ? "chip on" : "chip"}
                onClick={() => setLimit(count)}
              >
                {label}
              </button>
            ))}
          </div>
        </>
      )}
    </Modal>
  );
}

/** One link: copy, edit, revoke or delete it, and who joined through it. */
export function LinkDetails(props: ScreenProps & { link: Link }) {
  const { client, chatKey, nav, notify, link } = props;
  const dialogs = useDialogs();
  const manager = client.manage(chatKey);
  const [joined, setJoined] = useState<Importer[] | null>(null);
  const [editing, setEditing] = useState(false);
  useEffect(() => {
    client
      .manage(chatKey)
      .importers(link.link)
      .then(setJoined)
      .catch(() => setJoined([]));
  }, [client, chatKey, link.link]);
  return (
    <>
      <Header title={link.title || "Invite Link"} onBack={nav.pop} />
      <div className="panel-body">
        <Section>
          <div className="link-card">
            <span className="link-text" dir="ltr">
              {short(link.link)}
            </span>
            <div className="link-buttons">
              {!link.revoked && (
                <button
                  type="button"
                  className="pill-button primary"
                  onClick={() => copy(link.link, notify)}
                >
                  <Icon name="copy" size={18} /> Copy
                </button>
              )}
              {!link.revoked && !link.permanent && (
                <button type="button" className="pill-button" onClick={() => setEditing(true)}>
                  Edit
                </button>
              )}
            </div>
            <span className="link-state">{linkState(link)}</span>
          </div>
        </Section>
        <Section title="Details">
          <Row icon="info" title={when(link.date)} subtitle="Created" />
          {link.request_needed && <Row icon="requests" title="Admins approve each request" />}
        </Section>
        <Section title={joined?.length ? `${joined.length} joined` : "Joined"}>
          {joined === null && <p className="empty">Loading…</p>}
          {joined?.length === 0 && <p className="empty">No one has joined with this link yet.</p>}
          {joined?.map((i) => (
            <Row
              key={i.user.id}
              leading={<Avatar id={i.user.id} name={fullName(i.user)} size={40} />}
              title={fullName(i.user)}
              subtitle={`joined ${when(i.date)}`}
            />
          ))}
        </Section>
        <Section>
          {link.revoked ? (
            <Row
              icon="delete"
              title="Delete Link"
              danger
              onClick={async () => {
                const ok = await dialogs.confirm({
                  title: "Delete link?",
                  ok: "Delete",
                  danger: true,
                });
                if (ok) attempt(notify, () => manager.deleteLink(link.link).then(nav.pop));
              }}
            />
          ) : (
            <Row
              icon="block"
              title="Revoke Link"
              danger
              onClick={async () => {
                const ok = await dialogs.confirm({
                  title: "Revoke link?",
                  text: "The link will stop working.",
                  ok: "Revoke",
                  danger: true,
                });
                if (ok) attempt(notify, () => manager.revokeLink(link.link).then(nav.pop));
              }}
            />
          )}
        </Section>
      </div>
      {editing && (
        <LinkEditor
          link={link}
          onClose={() => setEditing(false)}
          onSave={(options) =>
            attempt(notify, () => manager.editLink(link.link, options).then(nav.pop))
          }
        />
      )}
    </>
  );
}

export { Check };
