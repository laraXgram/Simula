/** Edit my own profile the way Telegram's settings do: name, bio and username. */
import { useEffect, useState } from "react";

import type { Client } from "../state/client";
import { Avatar } from "./kit/Avatar";
import { Field, Modal } from "./kit/Dialogs";

const ERRORS: Record<string, string> = {
  USERNAME_OCCUPIED: "This username is already taken",
  USERNAME_INVALID: "Usernames have 5–32 letters, digits or _, and start with a letter",
  FIRSTNAME_INVALID: "Enter your first name",
  ABOUT_TOO_LONG: "The bio is too long",
};

export function ProfileDialog({ client, onClose }: { client: Client; onClose: () => void }) {
  const me = client.store.snapshot().me;
  const [first, setFirst] = useState(me?.first_name ?? "");
  const [last, setLast] = useState(me?.last_name ?? "");
  const [about, setAbout] = useState<string | null>(null);
  const [username, setUsername] = useState(me?.username ?? "");
  const [taken, setTaken] = useState(false);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    client
      .myBio()
      .then(setAbout)
      .catch(() => setAbout(""));
  }, [client]);
  // Say whether a new username is free while it's typed, like Telegram does.
  useEffect(() => {
    setTaken(false);
    if (username.length < 5 || username === (me?.username ?? "")) return;
    const timer = setTimeout(() => {
      client
        .usernameFree(username)
        .then((free) => setTaken(!free))
        .catch(() => {});
    }, 300);
    return () => clearTimeout(timer);
  }, [client, username, me?.username]);
  const save = async () => {
    setBusy(true);
    setError("");
    try {
      await client.updateProfile(first.trim(), last.trim(), about ?? "");
      if (username !== (me?.username ?? "")) await client.setUsername(username);
      onClose();
    } catch (e) {
      const message = (e as Error).message;
      setError(ERRORS[message] ?? message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <Modal
      title="Edit Profile"
      onClose={onClose}
      onSubmit={save}
      actions={[
        { label: "Cancel", onClick: onClose },
        {
          label: "Save",
          primary: true,
          submit: true,
          disabled: busy || !first.trim() || about === null || taken,
        },
      ]}
    >
      <div className="profile compact">
        <Avatar id={me?.id ?? ""} name={`${first} ${last}`} size={80} />
        {me?.phone && <p className="profile-sub">+{me.phone}</p>}
      </div>
      <Field label="First name" value={first} onChange={setFirst} maxLength={64} autoFocus />
      <Field label="Last name (optional)" value={last} onChange={setLast} maxLength={64} />
      <Field
        label="Bio"
        value={about ?? ""}
        onChange={setAbout}
        maxLength={70}
        multiline
        hint="Any details such as age, occupation or city."
      />
      <Field
        label="Username"
        prefix="@"
        value={username}
        onChange={(v) => setUsername(v.replace(/[^A-Za-z0-9_]/g, ""))}
        maxLength={32}
        hint={
          taken
            ? "This username is already taken."
            : "People can find you by this username and write to you without your phone number."
        }
      />
      {error && <p className="find-error">{error}</p>}
    </Modal>
  );
}
