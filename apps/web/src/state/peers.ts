/** Peers as the client keys and addresses them. */
import type { tl } from "@simula/schema";

/** A stable key for a dialog: "u<id>", "c<id>" or "ch<id>". */
export function peerKey(peer: tl.Peer): string {
  switch (peer._) {
    case "peerUser":
      return `u${peer.user_id}`;
    case "peerChat":
      return `c${peer.chat_id}`;
    case "peerChannel":
      return `ch${peer.channel_id}`;
  }
}

export function userPeer(id: string): tl.Peer {
  return { _: "peerUser", user_id: id };
}

/** How this account addresses a user: with the access hash it was given. */
export function inputPeer(user: tl.user, me: string): tl.InputPeer {
  if (user.id === me) return { _: "inputPeerSelf" };
  return { _: "inputPeerUser", user_id: user.id, access_hash: user.access_hash ?? "0" };
}

export function inputUser(user: tl.user, me: string): tl.InputUser {
  if (user.id === me) return { _: "inputUserSelf" };
  return { _: "inputUser", user_id: user.id, access_hash: user.access_hash ?? "0" };
}

export function displayName(user: tl.user | undefined): string {
  if (!user) return "Unknown";
  return (
    [user.first_name, user.last_name].filter(Boolean).join(" ") ||
    user.username ||
    "Deleted account"
  );
}
