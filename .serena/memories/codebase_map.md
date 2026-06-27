# catalyst-cloud-sdk — codebase map

Published as `@catalyst-cloud/sdk`. A tiny isomorphic (browser + node/bun) read
layer over a WebSocket: it keeps a live local copy of catalyst-cloud
change-feed data (Linear/GitHub mirror) without polling or webhook tunnels.

## Source layout (`src/`)
- `index.ts` — public entrypoint; re-exports the client + wire types only.
- `live-sync-client.ts` — the core. `LiveSyncClient` class (storage-agnostic,
  auth-injected) plus helpers `buildConnectUrl`, `parseFrame`, `toWsOrigin`,
  and the `AuthStrategy` / `LiveSyncClientOptions` / `LiveSyncStatus` /
  `LogLevel` / `WebSocketLike` / `WebSocketFactory` types.
- `types.ts` — self-contained change-feed wire contract: `EntityName` union +
  runtime `ENTITY_NAMES`, `ChangeOp` + `CHANGE_OPS`, and the
  `ChangeFrame` / `ResyncFrame` / `SyncFrame` / `ServerFrame` shapes; `AccountId`.

## Exported entry points (`package.json` `exports["."]`)
Single entry `.` → `dist/index.js` (+ `.d.ts`). ESM only (`"type": "module"`),
`sideEffects: false`, node >= 22.

## Tests (`test/`)
- `live-sync-client.test.ts` — client behavior.
- `contract.test.ts` — pins the literal wire-contract members against drift.

## Tooling
Build `tsc -p tsconfig.build.json`; typecheck `tsc --noEmit`; test `vitest run`.
Read `README.md` first for the connect/replay/apply model.
