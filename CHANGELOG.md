# Changelog

## Unreleased — CTC-4549

`issues.get` and `issues.execution` take an optional `{ fields, projection }` and send `?fields=a,b` and `?projection=<name>`. A selected issue is typed partial with `identifier` kept, and a name outside the server's catalog comes back as `select-refused` with the allowed names. A call without options sends exactly the request it sent before.

`events.query` reads one page of `GET /api/v1/events/query` (ticket, type, limit, order, afterSeq, beforeSeq) and returns `events`, `next` and `coverage`. `events.pages` follows `next`. A server without the route answers `unsupported`, so a caller can fall back.

- `QueriedEvent` is `QueriedBackboneEvent | QueriedEventStub`. Branch on `bodyUnavailable === true`: the stub stands in for an event whose archive write the cloud gave up on.
- A 413 is `too-large`, carrying the event's `sequence` and the cursor past it. `pages()` yields it and carries on.
- A 503 is `unavailable`, with `retryable: true`, the failed `sequence` and `resume` (the cursor the request sent).
- A 400 for an unknown event type is `event-type-refused`, with `types`.

`IssueDetail` (the `issues.get` issue) and `ProjectedIssueDetail` carry an optional typed `repairLoop` (`TicketRepairLoop`). `TicketExecutionReport` types `ticket`, which the server always sends. `identifier` stays optional for compatibility; the server never sent it.

**Outcome changes for existing callers.** Both changes are on the new `events.query` route only:
- A 503 from it is now `outcome: "unavailable"`, where the shared failure union would have said `http`.
- An unknown event type is now `outcome: "event-type-refused"`, where it would have been `rejected`.

A caller switching on `outcome` must handle both new arms.

Needs a mirror that serves `GET /api/v1/events/query` and the read selection (catalyst-cloud CTC-4549).

## Unreleased — CTC-3780

`createTenantClient` gains `getProjectWipLimit(team)` and `setProjectWipLimit(team, limit)`, over `GET|POST /api/v1/agent/team-wip-limit` with a person's own key or CLI login. `team` is the project's key (`CTC`) or its Linear team id. Any active member reads; only an admin or owner sets, and anyone else gets `outcome: "forbidden"`. `limit` is clamped by the cloud to 0..9999, and `null` returns the project to the default. A non-finite `limit` is refused locally (`error: "invalid-limit"`), because JSON would send it as `null`. Both answer `{team: {id, key}, limit, source, stored, inProgress, countedAt}`.

Additive, so a patch release. Needs a mirror that serves the route (catalyst-cloud CTC-3780).

## 0.13.0 — CTC-2132

`createTenantClient` now covers every route the `catalyst-cloud-skills` bundle's own `src/http.ts`
transport still hand-rolled: `issues.execution`, `diagnostics.workEligibility/dispatchQueue/
fleetActivity/agentRoster/leaseAttributions/codingAccounts`, `cycles.list`, `search`,
`workflowStages`, `changes.stream`/`changes.list` (the NDJSON change feed), `snapshot.head`, and a
generic `request()` escape hatch for any route not otherwise enumerated. `createTenantClient` also
accepts the CTC-2111 `{kind:"token"|"cookie"|"bearer"}` `AuthStrategy` as an alternative to `key`, so
a single credential provider can drive every SDK surface; a `bearer` token is resolved fresh on every
request.

This is the SDK-side prerequisite for CTC-2004 Tier 2 — the bundle's deletion of its own `src/http.ts`
is a separate, linked ticket in `catalyst-cloud-skills` that this release unblocks but does not itself
perform.

Not included: typed `agent.ticketRelease`/`agent.ticketReleaseClass` (CTC-2156) — that cloud route
does not exist yet; see the deferral comment in `src/tenant-client.ts` next to `AGENT_ROUTE_NAMES`.

`changes.stream()`'s success arm carries a `close()` alongside `rows`, for the caller that reads
`head` and never iterates: it cancels the response body and stops the idle deadline. Draining the rows
or `break`ing out of the `for await` already releases both.

Non-breaking: every existing `createTenantClient({ key, baseUrl })` caller keeps working with
identical wire behaviour.
