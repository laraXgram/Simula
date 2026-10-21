/** Pick another account to open a chat as, in a new tab. */
import { useEffect, useState } from "react";

import { type Account, accounts } from "../api/control";
import { openTab } from "../app/account";
import { Avatar } from "./kit/Avatar";
import { Row, Section } from "./kit/Controls";
import { Modal } from "./kit/Dialogs";

export function OpenAs({
  me,
  chatKey,
  onClose,
}: {
  me: string;
  chatKey: string;
  onClose: () => void;
}) {
  const [people, setPeople] = useState<Account[] | null>(null);
  useEffect(() => {
    accounts()
      .then((all) =>
        setPeople(all.filter((a) => !a.is_bot && !a.extra?.deleted && String(a.id) !== me)),
      )
      .catch(() => setPeople([]));
  }, [me]);
  const open = (id: number) => {
    // A private chat with that account opens from their side.
    openTab(id, chatKey === `u${id}` ? `u${me}` : chatKey);
    onClose();
  };
  return (
    <Modal title="Open as…" onClose={onClose} actions={[{ label: "Cancel", onClick: onClose }]}>
      <p className="modal-text">The chat opens in a new tab, signed in as the account you pick.</p>
      {people === null && <p className="empty">Loading…</p>}
      {people?.length === 0 && (
        <p className="empty">There's no other account. Create one from “Switch account”.</p>
      )}
      {people && people.length > 0 && (
        <Section>
          {people.map((a) => {
            const name = [a.first_name, a.last_name].filter(Boolean).join(" ");
            return (
              <Row
                key={a.id}
                leading={<Avatar id={String(a.id)} name={name} size={42} />}
                title={name}
                subtitle={a.username ? `@${a.username}` : String(a.id)}
                onClick={() => open(a.id)}
              />
            );
          })}
        </Section>
      )}
    </Modal>
  );
}
