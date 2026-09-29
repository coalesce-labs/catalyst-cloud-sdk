// ndjson.ts — CTC-2132. Extracted verbatim from src/replica/catalyst-replica.ts (was module-private
// there) so the tenant client's `/changes` NDJSON stream and the replica's `/snapshot` seed share one
// implementation instead of two copies drifting apart. A leaf module: no import from `./replica/`.

/**
 * Iterate an NDJSON Response as non-empty lines. Streams `response.body` (chunked + partial-line
 * buffered) when present — the production path that never buffers the whole snapshot; falls back to a
 * buffered `await res.text()` when the body is absent (e.g. a test fetch stand-in).
 *
 * Cancellation (CTC-281): `opts.signal` bounds the read loop — on abort the reader is cancelled, which
 * settles a pending `read()` promptly ({done:true}) EVEN when the Response was never wired to the
 * signal (injected fetch stand-ins), and the loop then throws the abort reason. The `finally` ALWAYS
 * cancels the reader: an abnormal exit in the CALLER (a JSON.parse throw, a closed-engine throw)
 * previously ended the generator with the body still locked, leaving the connection referenced —
 * a leaked handle that held process exit hostage after close(). `opts.onProgress` fires per chunk
 * (the seed's idle-deadline refund).
 */
export async function* iterateNdjson(
  res: Response,
  opts?: { signal?: AbortSignal; onProgress?: () => void },
): AsyncGenerator<string> {
  const signal = opts?.signal;
  const throwIfAborted = (): void => {
    if (!signal?.aborted) return;
    throw signal.reason instanceof Error ? signal.reason : new Error("snapshot read aborted");
  };
  const body = res.body;
  if (body && typeof body.getReader === "function") {
    const reader = body.getReader();
    const onAbort = (): void => {
      void reader.cancel().catch(() => {
        // already released/closed — the abort still surfaces via throwIfAborted
      });
    };
    if (signal) {
      if (signal.aborted) onAbort();
      else signal.addEventListener("abort", onAbort, { once: true });
    }
    const decoder = new TextDecoder();
    let buf = "";
    try {
      for (;;) {
        const { done, value } = await reader.read();
        throwIfAborted(); // an abort settles read() via cancel — surface it as an error, not EOF
        if (done) break;
        opts?.onProgress?.();
        buf += decoder.decode(value, { stream: true });
        let nl: number;
        while ((nl = buf.indexOf("\n")) >= 0) {
          const line = buf.slice(0, nl);
          buf = buf.slice(nl + 1);
          if (line.length > 0) yield line;
        }
      }
      buf += decoder.decode();
      if (buf.length > 0) {
        for (const line of buf.split("\n")) if (line.length > 0) yield line;
      }
    } finally {
      signal?.removeEventListener("abort", onAbort);
      try {
        await reader.cancel(); // release the body/connection on EVERY exit path (no-op when done)
      } catch {
        // already released/closed
      }
    }
    return;
  }
  const text = await res.text();
  throwIfAborted();
  for (const line of text.split("\n")) if (line.length > 0) yield line;
}
