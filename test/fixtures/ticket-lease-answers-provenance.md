# Ticket lease answer fixture

`ticket-lease-answers.json` holds complete wire bodies for the seat ticket-lease routes:
the 200 `taken`/`renewed`/`released` answers with holder details, the boundary-decorated 200
`refused` answers (top-level `code`/`reason` included), the 400 validation answers, the
`GET /api/v1/agent/ticket-lease` read answers and the `GET /api/v1/agent/ticket-leases` team
list. `test/ticket-lease-client.test.ts` feeds these bodies through the real client against a
scripted fetch, so every guard, field name and nullable passthrough the SDK declares is checked
against them (CTC-8872). The document describes the API only; it contains no credentials or
customer data.

The bodies are reconstructed from the CTC-8871 (slice 1) documents in the tenant thoughts pool and
the CTC-8872 plan's design section, not fetched from a live server: no cloud credential exists in
this checkout. Identifiers (`CTC-6480`, `factory-droid`, `review-2`, `key_factory`) are the
documents' own illustrative values.

| Field | Value |
| --- | --- |
| Slice 1 plan | `repos/catalyst/shared/plans/2026-10-31-CTC-8871-lease-holder-details-slice-1-the-cloud-and-the-contract.md` (canonical type block) |
| Slice 1 PR | `repos/catalyst/shared/prs/2026-10-31-CTC-8871-pr.md` (holder/list additions, refusal decoration) |
| CTC-8872 plan design section | `prior/plan.md`, "Design for review" (take/renew/release/read/list bodies, error fields) |
| Protocol source restated | `packages/protocol/src/lease.ts` in coalesce-labs/catalyst-cloud (snapshot 2026-10-31, pre-slice-1) |
| Recorded (UTC) | 2026-11-02T03:30:00Z |

The SDK's wire declarations in `src/ticket-lease-client.ts` restate that protocol file under its
exact names because `@catalyst-cloud/protocol` is private and unpublished (npm 404, reproduced in
the CTC-8872 research). When protocol becomes installable, replace the restatement with
`export type { … } from "@catalyst-cloud/protocol"` and re-run the suite against this fixture: the
transport tests then pin the canonical declarations to these same bodies.

To refresh it, quote the new bodies from a cloud on contract 2.65 (or its protocol declarations),
keep every row complete (no ellipsis placeholders), record the sources here, and re-run
`bunx vitest run test/ticket-lease-client.test.ts`.
