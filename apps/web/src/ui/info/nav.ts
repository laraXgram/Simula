/** The info panel's screens and how screens move between them. */
import type { tl } from "@simula/schema";

import type { Client } from "../../state/client";
import type { Link, Member, MemberFilter } from "../../state/manage";
import type { Snapshot } from "../../state/store";

export type Screen =
  | { name: "info" }
  | { name: "edit" }
  | { name: "type" }
  | { name: "members"; filter: MemberFilter }
  | { name: "admin"; member: Member }
  | { name: "restrict"; member: Member }
  | { name: "permissions" }
  | { name: "links" }
  | { name: "link"; link: Link }
  | { name: "requests" }
  | { name: "add" };

export interface Nav {
  push: (screen: Screen) => void;
  pop: () => void;
  close: () => void;
}

export interface ScreenProps {
  client: Client;
  snapshot: Snapshot;
  chatKey: string;
  nav: Nav;
  notify: (text: string, options?: { alert?: boolean }) => void;
  onOpenChat: (key: string) => void;
}

/** "today at 14:05", "yesterday at 9:12", "12 Mar 2026". */
export function when(date: number): string {
  if (!date) return "";
  const d = new Date(date * 1000);
  const now = new Date();
  const time = d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
  const days = Math.floor((now.setHours(0, 0, 0, 0) - new Date(d).setHours(0, 0, 0, 0)) / 86400000);
  if (days === 0) return `today at ${time}`;
  if (days === 1) return `yesterday at ${time}`;
  return d.toLocaleDateString(undefined, { day: "numeric", month: "short", year: "numeric" });
}

export function fullName(user: tl.user | undefined): string {
  if (!user) return "Unknown";
  return (
    [user.first_name, user.last_name].filter(Boolean).join(" ") ||
    user.username ||
    "Deleted account"
  );
}

/** A member's role as the lists show it ("owner", "admin", a custom title). */
export function roleLabel(member: Member): string | undefined {
  if (member.rank) return member.rank;
  if (member.role === "creator") return "owner";
  if (member.role === "admin") return "admin";
  return undefined;
}

/** How long a restriction lasts, in words. */
export function untilLabel(until: number | undefined): string {
  if (!until || until > 2_000_000_000) return "forever";
  return `until ${new Date(until * 1000).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" })}`;
}

/** Run an action and report its error as a notice. */
export function attempt(notify: ScreenProps["notify"], action: () => Promise<unknown>) {
  return action().catch((error: Error) => notify(readable(error.message)));
}

/** Server errors in words a person understands. */
export function readable(message: string): string {
  const known: Record<string, string> = {
    CHAT_ADMIN_REQUIRED: "You don't have the rights to do this",
    RIGHT_FORBIDDEN: "You can't grant rights you don't have",
    USER_CREATOR: "The owner can't be changed",
    USER_NOT_PARTICIPANT: "This user is not a member",
    USER_ALREADY_PARTICIPANT: "Already a member",
    USER_ADMIN_INVALID: "You can only change admins you promoted",
    USERNAME_OCCUPIED: "This link is already taken",
    USERNAME_INVALID: "This link is invalid",
    USERNAME_NOT_MODIFIED: "That's already the link",
    CHAT_NOT_MODIFIED: "Nothing changed",
    CHAT_ABOUT_NOT_MODIFIED: "Nothing changed",
    CHAT_TITLE_EMPTY: "The name can't be empty",
    USER_BOT: "Bots can only be added to channels as admins",
    BOT_GROUPS_BLOCKED: "This bot can't be added to groups",
    USER_KICKED: "This user was removed from the chat",
    INVITE_REVOKED_MISSING: "Revoke the link first",
    USERNAME_NOT_OCCUPIED: "No one has that username",
  };
  return known[message] ?? message;
}
