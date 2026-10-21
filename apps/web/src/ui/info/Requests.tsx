/** People who asked to join through a link that needs approval. */
import { useCallback, useEffect, useState } from "react";

import type { Importer } from "../../state/manage";
import { Avatar } from "../kit/Avatar";
import { Section } from "../kit/Controls";
import { Header } from "./Header";
import { attempt, fullName, type ScreenProps, when } from "./nav";

export function Requests({ client, chatKey, nav, notify }: ScreenProps) {
  const [requests, setRequests] = useState<Importer[] | null>(null);
  const reload = useCallback(() => {
    client
      .manage(chatKey)
      .importers()
      .then(setRequests)
      .catch((error: Error) => notify(error.message));
  }, [client, chatKey, notify]);
  useEffect(reload, [reload]);
  const decide = (userId: string, approve: boolean) =>
    attempt(notify, () => client.manage(chatKey).decide(userId, approve).then(reload));
  return (
    <>
      <Header title="Join Requests" onBack={nav.pop} />
      <div className="panel-body">
        <Section note="People who follow a link that needs approval wait here until an admin adds or dismisses them.">
          {requests === null && <p className="empty">Loading…</p>}
          {requests?.length === 0 && <p className="empty">No pending requests.</p>}
          {requests?.map((r) => (
            <div key={r.user.id} className="request">
              <Avatar id={r.user.id} name={fullName(r.user)} size={44} />
              <div className="request-body">
                <span className="row-title" dir="auto">
                  {fullName(r.user)}
                </span>
                <span className="row-subtitle">requested {when(r.date)}</span>
                {r.about && <span className="request-about">{r.about}</span>}
                <div className="link-buttons">
                  <button
                    type="button"
                    className="pill-button primary"
                    onClick={() => decide(r.user.id, true)}
                  >
                    Add to Chat
                  </button>
                  <button
                    type="button"
                    className="pill-button"
                    onClick={() => decide(r.user.id, false)}
                  >
                    Dismiss
                  </button>
                </div>
              </div>
            </div>
          ))}
        </Section>
      </div>
    </>
  );
}
