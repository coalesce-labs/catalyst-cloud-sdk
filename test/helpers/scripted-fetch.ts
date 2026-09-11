// test/helpers/scripted-fetch.ts — CTC-2004. A recording, scripted `fetch` for the tenant-client tests.
//
// Real `Response`/`Headers` objects are returned so header lookups are case-insensitive exactly as
// they are against the Worker; every call is recorded (url, method, headers, body) so a test can
// assert the exact wire shape the client sent — the param NAMES the DO reads, the bearer, the
// conditional-GET header — rather than only what came back.

export interface RecordedCall {
  url: string;
  method: string;
  headers: Record<string, string>;
  body: unknown;
}

export type Scripted = (call: RecordedCall) => Response | Promise<Response> | Error;

export interface ScriptedFetch {
  fetch: typeof fetch;
  calls: RecordedCall[];
}

/** `script` answers each call in order; the last entry repeats. Return an `Error` to make fetch throw. */
export function scriptedFetch(script: Scripted[]): ScriptedFetch {
  const calls: RecordedCall[] = [];
  const impl = async (input: string | URL | Request, init?: RequestInit): Promise<Response> => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    const headers: Record<string, string> = {};
    new Headers(init?.headers).forEach((v, k) => {
      headers[k] = v;
    });
    const rawBody = init?.body;
    const body = typeof rawBody === "string" ? (JSON.parse(rawBody) as unknown) : rawBody ?? null;
    const call: RecordedCall = { url, method: init?.method ?? "GET", headers, body };
    calls.push(call);
    const step = script[Math.min(calls.length - 1, script.length - 1)];
    if (step === undefined) throw new Error("scriptedFetch: empty script");
    const answer = await step(call);
    if (answer instanceof Error) throw answer;
    return answer;
  };
  return { fetch: impl, calls };
}

export function json(status: number, body: unknown, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}

export function text(status: number, body: string, headers: Record<string, string> = {}): Response {
  return new Response(body, { status, headers });
}

export function empty(status: number, headers: Record<string, string> = {}): Response {
  return new Response(null, { status, headers });
}
