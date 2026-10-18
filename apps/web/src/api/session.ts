/**
 * The client's connection to Simula: JSON-TL over a WebSocket (`/session`), as one
 * simulated account. Calls are typed by the generated `Methods` map; updates for the
 * account arrive through `onUpdates`.
 */
import type { tl } from "@simula/schema";

type Methods = tl.Methods;
export type MethodName = keyof Methods;
/** A method's parameters, without the `_` tag the session adds. */
export type Params<N extends MethodName> = Omit<Methods[N]["params"], "_">;
export type Result<N extends MethodName> = Methods[N]["result"];

/** An MTProto error, e.g. `400 PEER_ID_INVALID`. */
export class RpcError extends Error {
  constructor(
    readonly code: number,
    message: string,
  ) {
    super(message);
  }
}

type Frame =
  | { session: { id: string; account: string } }
  | { id: number; result: unknown }
  | { id: number; error: { code: number; message: string } }
  | { updates: tl.Updates }
  | { lagged: number };

export class Session {
  private nextId = 1;
  private pending = new Map<
    number,
    { resolve: (v: unknown) => void; reject: (e: Error) => void }
  >();
  private listeners = new Set<(updates: tl.Updates) => void>();

  private constructor(
    private socket: WebSocket,
    readonly account: string,
  ) {
    socket.addEventListener("message", (event) =>
      this.receive(JSON.parse(String(event.data)) as Frame),
    );
    socket.addEventListener("close", () => {
      for (const { reject } of this.pending.values()) reject(new RpcError(500, "SESSION_CLOSED"));
      this.pending.clear();
    });
  }

  /** Open a session for `account` and wait for the server's greeting. */
  static open(account: string): Promise<Session> {
    const scheme = location.protocol === "https:" ? "wss" : "ws";
    const socket = new WebSocket(`${scheme}://${location.host}/session?account=${account}`);
    return new Promise((resolve, reject) => {
      socket.addEventListener("message", function greeted(event) {
        const frame = JSON.parse(String(event.data)) as Frame;
        if ("session" in frame) {
          socket.removeEventListener("message", greeted);
          resolve(new Session(socket, account));
        }
      });
      socket.addEventListener("error", () =>
        reject(new Error("can't connect to the Simula server")),
      );
    });
  }

  call<N extends MethodName>(name: N, params: Params<N>): Promise<Result<N>> {
    const id = this.nextId++;
    this.socket.send(JSON.stringify({ id, call: { _: name, ...params } }));
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve: resolve as (v: unknown) => void, reject });
    });
  }

  onUpdates(listener: (updates: tl.Updates) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  close() {
    this.socket.close();
  }

  private receive(frame: Frame) {
    if ("updates" in frame) {
      for (const listener of this.listeners) listener(frame.updates);
    } else if ("id" in frame) {
      const waiting = this.pending.get(frame.id);
      this.pending.delete(frame.id);
      if (!waiting) return;
      if ("error" in frame) waiting.reject(new RpcError(frame.error.code, frame.error.message));
      else waiting.resolve(frame.result);
    }
  }
}
