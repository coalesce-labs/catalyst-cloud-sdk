import type {
  WebSocketFactory,
  WebSocketLike,
} from "../../src/live-sync-client.js";
import { PING_FRAME, PONG_FRAME } from "../../src/types.js";

export class FakeWebSocket implements WebSocketLike {
  sent: string[] = [];
  closed = false;
  onopen: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onclose: ((ev: unknown) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  constructor(public answerPings = false) {}
  send(data: string): void {
    this.sent.push(data);
    if (this.answerPings && data === PING_FRAME) {
      // A real pong arrives after send returns and the client starts its timeout.
      queueMicrotask(() => this.deliver(PONG_FRAME));
    }
  }
  close(): void {
    this.closed = true;
  }
  /** The cloud accepted the upgrade and confirmed the subscription. */
  subscribe(): void {
    this.onopen?.({});
    this.deliver({ type: "channels", channels: ["events"], fields: "full" });
  }
  deliver(frame: unknown): void {
    this.onmessage?.({
      data: typeof frame === "string" ? frame : JSON.stringify(frame),
    });
  }
  serverClose(code: number): void {
    this.onclose?.({ code, reason: "" });
  }
}

export function recordingFactory(options: { answerPings?: boolean } = {}): {
  sockets: FakeWebSocket[];
  urls: string[];
  factory: WebSocketFactory;
} {
  const sockets: FakeWebSocket[] = [];
  const urls: string[] = [];
  return {
    sockets,
    urls,
    factory: (url) => {
      urls.push(url);
      const ws = new FakeWebSocket(options.answerPings ?? false);
      sockets.push(ws);
      return ws;
    },
  };
}
