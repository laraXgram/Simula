/**
 * Shape of `spec/tl/*.json` — parsed TL schemas (telegram_api, mtproto_api).
 */

export type TlType =
  /** A named type: boxed ("Updates", "messages.Chats", "Bool") or bare ("int", "long", "string"). */
  | { kind: "named"; name: string; bare: boolean }
  /** `Vector<T>` (boxed, with the vector constructor id) or `vector<T>` (bare). */
  | { kind: "vector"; of: TlType; bare: boolean }
  /** A generic parameter: `X` in results, `!X` in params (a serialized function call). */
  | { kind: "generic"; name: string; query: boolean }
  /** `#` — the flags bitmask field itself. */
  | { kind: "flags" };

export interface TlParam {
  name: string;
  type: TlType;
  /** Present when the param is conditional: `flags.3?Type` / `flags2.0?true`. */
  flag?: { field: string; bit: number };
}

export interface TlCombinator {
  name: string;
  /** Constructor id as 8 lowercase hex digits. */
  id: string;
  params: TlParam[];
  /** Constructors: the boxed type they build. Functions: the result type. */
  result: TlType;
  /** Generic type parameters, e.g. `{X:Type}` on `invokeWithLayer`. */
  generics?: string[];
}

export interface TlSchema {
  file: string;
  layer?: number;
  constructors: TlCombinator[];
  functions: TlCombinator[];
}

export interface TlSource {
  layer: number;
  tdlib_commit: string;
  tdlib_date: string;
}
