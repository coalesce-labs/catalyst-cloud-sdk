# Changelog

## 0.12.0 — CTC-2132

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
