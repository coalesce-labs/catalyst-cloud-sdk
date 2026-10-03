# A coordinated release

A coordinated release moves every member to the same new line, `0.N.0`, at once. It is a plan a person approves before anything is bumped. The person who owns the release may change the order; their plan wins over this page.

## The proposal to put in front of them

Write it as a table and nothing gets lost:

| Member | Now (train-status) | Proposed | Repository and step |
| --- | --- | --- | --- |
| CLI, forwarder, skills bundle, plugin | 0.15.2 | 0.16.0 | catalyst-cloud-skills: bump, `npm run version:sync`, CHANGELOG, tag |
| SDK and both replica modules | 0.14.1 | 0.16.0 | catalyst-cloud-sdk: versions and peers, CHANGELOG, GitHub Release |
| schema, replicate, read-model | 0.13.1 | 0.16.0 | catalyst-cloud: publish chain |
| design package `@coalesce-labs/catalyst-design` | not published | 0.16.0 | catalyst-cloud: version bump merged to main |
| installer | 0.13.17 | 0.16.0 | catalyst-cloud: `INSTALL_SCRIPT_REVISION` |
| contract `releaseLine` | 0.13 | 0.16 | catalyst-cloud: `RELEASE_LINE` |

The numbers above are an example of the shape; read the real ones from train-status. Also say why the train moves (the breaking change or major feature, or "realigning a split train"), and whether anything breaks for an installed CLI.

## What must change, member by member

**catalyst-cloud-skills (CLI, forwarder, skills, plugin).** `package.json` version; `npm run version:sync` writes `.claude-plugin/plugin.json`, the forwarder's version and its exact CLI pin, and the `vendored-from` stamp in every `skills/*/SKILL.md`; a `## X.Y.Z` entry in `CHANGELOG.md`. Push tag `skills-bundle-vX.Y.Z`; the publish workflow checks the tag matches the version and that both packages agree, publishes both, and deprecates the forwarder version.

**catalyst-cloud-sdk.** `package.json`; `modules/replica-node/package.json` and `modules/replica-browser/package.json`, each version plus its `"@catalyst-cloud/sdk": "^X.Y.Z"` peer; `CHANGELOG.md`. Publish by creating the GitHub Release `vX.Y.Z`; `scripts/release.mjs` refuses mismatched versions, a tag that does not match, and a re-publish with different bytes. If schema, replicate or read-model moved, update their pins here too.

**catalyst-cloud, packages.** Schema first, then replicate with its exact schema pin, then read-model. They publish by hand with `npm publish`; `docs/devops.md` has the schema chain and its checks. Then the `@catalyst-cloud/sdk` ranges in catalyst-cloud's manifests and `bun.lock` once the SDK is out.

**catalyst-cloud, design package.** `packages/design/package.json`. A reviewed version bump merged to main publishes `@coalesce-labs/catalyst-design` to private GitHub Packages; an offline guard in the required Check fails a change to shipped files without a bump. Nothing else pins it, so it publishes beside the schema packages. Its first publish is 0.16.0.

**catalyst-cloud, cloud.** The first change moves `RELEASE_LINE` in `packages/types/src/install-block.ts`, the contract fixture (`bun run contract:fixture --refresh-version`), and `INSTALL_SCRIPT_REVISION` to `0.N.0` with the literals that pin it in `apps/mirror/test/install-bootstrap.test.ts` and `apps/mirror/test/install-script.test.ts` (search both for the old revision). The installer's CLI pin (`INSTALL_SETUP_CLI_VERSION` in `apps/mirror/src/skills/install-bootstrap.ts`) moves in the last step, once the new CLI is on npm. Both go live when the cloud deploys.

## A breaking contract change

Removing or changing a contract field is breaking. The contract's own MAJOR moves (`CONTRACT_MAJOR` in `packages/types/src/contract-entry.ts`, with a new `packages/types/contract/entries/<major>/` directory, in catalyst-cloud), and because it is breaking, the train MINOR moves for every member too.

Installed CLIs accept the contract by MAJOR range (`tenantContractRange` in catalyst-cloud-skills' `package.json`, `1.x || 2.x` today) and refuse anything outside it by name. So the order changes: first a CLI release whose range also accepts the new MAJOR, then the cloud change that serves it. Every CLI older than that release refuses the new contract until its owner updates; say so in the proposal, and let the release owner choose when to start that window.

## Order

The order approved for the first coordinated release, and the default for a change that does not break the contract, unless the release owner says otherwise:

1. the cloud change that moves the contract's `releaseLine`, deployed after checking that installed CLIs still accept the contract;
2. schema, then replicate and read-model, and the design package;
3. the SDK, published from its GitHub Release, and checked against its real consumers;
4. the CLI, forwarder, skills bundle and plugin;
5. the installer, then a check that every member is served on the new line.

A test in catalyst-cloud requires `INSTALL_SCRIPT_REVISION` to start with `RELEASE_LINE`, so the change that moves the line also moves the installer revision's prefix. The installer's CLI pin and body follow in the last step. That prefix change is part of the transition, not the finished release: the installer is done only in step 5, and the release only when every member is served on the new line.

Keep the dependency pins as they are built: replicate pins schema exactly, the SDK pins schema and replicate exactly and read-model within the line, the CLI pins the SDK exactly, and the forwarder pins the CLI exactly.

After each step, run train-status and check the new version on npm or in the installer header before starting the next. A step that fails stops the release; report where it stopped rather than skipping ahead.

## Done means

train-status exits 0 on the new line, the contract's `releaseLine` names it, and each repository's CHANGELOG or release notes say what changed in customer words.
