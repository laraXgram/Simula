/** Public (with a t.me link anyone can find) or private (joined by invite link). */
import { useEffect, useState } from "react";

import { abilities } from "../../state/abilities";
import { Check, Row, Section } from "../kit/Controls";
import { Field } from "../kit/Dialogs";
import { Icon } from "../kit/Icon";
import { Header } from "./Header";
import { attempt, readable, type ScreenProps } from "./nav";

export function ChatType({ client, snapshot, chatKey, nav, notify }: ScreenProps) {
  const chat = snapshot.chats.get(chatKey);
  const details = snapshot.details.get(chatKey);
  const can = abilities(chat);
  const current = chat?._ === "channel" ? (chat.username ?? "") : "";
  const [isPublic, setPublic] = useState(current !== "");
  const [username, setUsername] = useState(current);
  const [state, setState] = useState<"" | "checking" | "free" | string>("");
  const kind = can.broadcast ? "channel" : "group";
  const manager = client.manage(chatKey);
  useEffect(() => {
    if (!isPublic || username === current || username.length < 5) {
      setState(
        username && username.length < 5 && isPublic ? "Links must have at least 5 characters" : "",
      );
      return;
    }
    setState("checking");
    const timer = setTimeout(() => {
      manager
        .usernameFree(username)
        .then((free) => setState(free ? "free" : "This link is already taken"))
        .catch((error: Error) => setState(readable(error.message)));
    }, 400);
    return () => clearTimeout(timer);
  }, [isPublic, username, current, manager]);
  const target = isPublic ? username : "";
  const changed = target !== current;
  const valid = !isPublic || state === "free" || username === current;
  const save = () => attempt(notify, () => manager.setUsername(target).then(nav.pop));
  return (
    <>
      <Header
        title={`${kind === "channel" ? "Channel" : "Group"} Type`}
        onBack={nav.pop}
        actions={
          changed &&
          valid && (
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
        <Section title={`${kind === "channel" ? "Channel" : "Group"} type`}>
          <Row
            leading={<Check checked={!isPublic} />}
            title={`Private ${kind}`}
            subtitle={`Private ${kind}s can only be joined by invite link.`}
            onClick={() => setPublic(false)}
          />
          <Row
            leading={<Check checked={isPublic} />}
            title={`Public ${kind}`}
            subtitle={`Public ${kind}s can be found in search; anyone can join.`}
            onClick={() => setPublic(true)}
          />
        </Section>
        {isPublic ? (
          <div className="fields">
            <Field
              label="Link"
              prefix="t.me/"
              value={username}
              onChange={(value) => setUsername(value.replace(/[^A-Za-z0-9_]/g, ""))}
              maxLength={32}
              error={state && state !== "free" && state !== "checking" ? state : undefined}
              hint={
                state === "free"
                  ? `${username} is available.`
                  : state === "checking"
                    ? "Checking…"
                    : `People can share this link with others and find your ${kind} in search.`
              }
            />
          </div>
        ) : (
          <Section
            title="Invite link"
            note={`People can join your ${kind} by following this link. You can revoke it any time.`}
          >
            <Row
              icon="link"
              title={details?.inviteLink?.replace(/^https:\/\//, "") ?? "Manage invite links"}
              chevron
              onClick={() => nav.push({ name: "links" })}
            />
          </Section>
        )}
      </div>
    </>
  );
}
