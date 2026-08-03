// replica/browser/support.ts — feature-detect, FROM THE MAIN THREAD, whether this engine can run the
// OPFS SAHPool replica, so an unsupported browser falls back to its normal fetch path instead of
// booting a Worker that can only fail. Two main-thread-observable gates:
//
//   • OPFS root  — `navigator.storage.getDirectory`. Exposed on BOTH the window and a dedicated worker,
//                  AND only present in a secure context (`StorageManager` is `[SecureContext]`), so its
//                  presence is a sound main-thread proxy for "this engine has OPFS" — which on every
//                  shipping engine (Chromium 102+, Safari 16.4+, Firefox 111+) means the worker also has
//                  SyncAccessHandle.
//   • ES-module Worker — `Worker` exists (the DB worker is `{ type: "module" }`).
//
// Deliberately NOT probed: `FileSystemFileHandle.prototype.createSyncAccessHandle`. That method is
// `[Exposed=DedicatedWorker]` — it does NOT exist on the main-thread prototype in ANY browser (incl.
// fully-capable Chrome), so a main-thread `typeof … === "function"` probe ALWAYS returns false and
// wrongly reports "unsupported" everywhere (CTC-51 bug). The AUTHORITATIVE SyncAccessHandle check is
// the worker-side `installOpfsSAHPoolVfs()` in sqlite-db.ts; a browser that has OPFS in a secure
// context but lacks worker SAH degrades there.
//
// Wrapped so a partial / non-browser runtime returns false rather than throwing.

export function isBrowserReplicaSupported(): boolean {
  try {
    if (typeof navigator === "undefined" || typeof Worker === "undefined") return false;
    return typeof navigator.storage?.getDirectory === "function";
  } catch {
    return false;
  }
}
