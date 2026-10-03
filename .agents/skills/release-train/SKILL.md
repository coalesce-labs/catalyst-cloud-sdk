---
name: release-train
description: Load this first, before reading or changing anything, whenever a request would set, change or decide the released version of any Catalyst release-train member, even in another repository. That covers bumping, releasing, cutting, tagging (a skills-bundle-v* or v* tag), publishing to npm or GitHub Packages, npm version, a GitHub Release, setting install.sh's revision, and changing the agent contract's releaseLine or version. The members are @catalyst-cloud/cli, the catalyst-skills forwarder, the skills bundle and Claude plugin, @catalyst-cloud/sdk and its replica modules, @catalyst-cloud/schema, replicate and read-model, the private @coalesce-labs/catalyst-design package, the installer, and the agent contract, in catalyst-cloud-skills, catalyst-cloud-sdk and catalyst-cloud. They share one MAJOR.MINOR train that an ordinary bump breaks. Also load it to decide patch versus minor or for a breaking contract change. Not for third-party dependency upgrades, changelog wording, a merge-queue hold, or a failed CI run.
---

# Release train

Every customer-facing Catalyst release shares one MAJOR.MINOR line, the train, so a person can tell at a glance which CLI, skills, SDK and installer belong together. Patch numbers are free per member. The train is pre-1.0 (`0.MINOR`).

**Paths.** `${CLAUDE_SKILL_DIR}` is this skill's directory, which Claude Code fills in. On another harness, use the directory that holds this SKILL.md.

## The members

| Member | Repository | Version lives in | Publishes on |
| --- | --- | --- | --- |
| CLI `@catalyst-cloud/cli`, forwarder `@catalyst-cloud/catalyst-skills`, skills bundle and Claude plugin | catalyst-cloud-skills | `package.json`; `npm run version:sync` stamps the plugin, forwarder and SKILL.md files | tag `skills-bundle-vX.Y.Z` |
| SDK `@catalyst-cloud/sdk` and `sdk-replica-node`, `sdk-replica-browser` | catalyst-cloud-sdk | `package.json` plus `modules/*/package.json` (versions and `^X.Y.Z` peers) | GitHub Release `vX.Y.Z` |
| `@catalyst-cloud/schema`, `replicate`, `read-model` | catalyst-cloud | `packages/<name>/package.json` | `npm publish` by hand, schema first |
| Installer | catalyst-cloud | `INSTALL_SCRIPT_REVISION` in `apps/mirror/src/skills/install-script.ts` | the cloud deploy |
| Design package `@coalesce-labs/catalyst-design` (internal: private GitHub Packages, for Coalesce's own app and website) | catalyst-cloud | `packages/design/package.json` (workspace name `@catalyst-cloud/design`) | merge to main with a version bump; the release workflow publishes it |
| Agent contract `releaseLine` | catalyst-cloud | `RELEASE_LINE` in `packages/types/src/install-block.ts` | the cloud deploy |

The design package's first publish is 0.16.0, at the coordinated release that realigns the train; until then train-status reports it as not published.

`RELEASE_LINE` is the declared train. The contract serves it as `releaseLine`, and the installer revision must start with it (a test enforces that). The contract's own `contractVersion` is a separate 2.x number about the document's shape; it is not the train.

## The rules

1. **Additive changes are PATCH, for that member alone.** A new command, flag, field, event, SDK method or schema table ships as `X.Y.(Z+1)`. This departs from semver on purpose: the train MINOR means "these belong together", not "new API".
2. **A breaking change or a major feature in any member moves the shared MINOR for every member at once.** Nobody goes to `0.(Y+1)` alone, not even with a good reason. The answer to "bump the CLI to 0.16" when the train is 0.15 is a coordinated release, or a patch.
3. **The contract keeps its own number.** A breaking contract change moves `contractVersion`'s MAJOR (installed CLIs accept the contract by MAJOR range and refuse a new one by name). It is also a breaking change, so the train MINOR moves with it.
4. **Nothing goes backwards**: no member's version, no contract version.
5. **A MINOR that is not a breaking change or a major feature is not yours to choose.** If you cannot tell, ask.

## Before you change any version

1. **See where the train stands.** Run `node "${CLAUDE_SKILL_DIR}/scripts/train-status.mjs"`. It reads every member's published version and the declared line, read-only. Exit 0 means every member is on the line, 1 means split (it names the members off the line), and 2 means it could not read something, which is not a pass. In a catalyst-cloud checkout, add `--catalyst-cloud .` to read the line from a freshly fetched `origin/main`. The installer is read from staging unless `CATALYST_CLOUD_BASE_URL` or `--base-url` names the live host.
2. **Classify the change** with the rules above, from what changed, not from what the request says. "Bump the CLI to 0.16.0" is a request for a version, not evidence of a breaking change.
3. **A patch**: the member must already sit on the declared line. Release it with that repository's own steps (`CONTRIBUTING.md`, `scripts/release.mjs`, or the schema chain in catalyst-cloud's `docs/devops.md`).
4. **A train move**, or a member already off the line: do not bump anything yet. Plan the coordinated release ([references/coordinated-release.md](references/coordinated-release.md)) and stop for a decision.

## Standing exception until 0.16 ships

The owner's decision: until the coordinated 0.16 release ships, a 0.15.x PATCH of the CLI, the forwarder or the skills bundle and plugin may proceed without stopping, even though the train is split. It must be a patch (`0.15.Z` to `0.15.(Z+1)`), it moves no MINOR, and it changes nothing in the SDK or any other member. Release it with the repository's own steps and say in your reply that it rides this exception.

The exception ends when 0.16 ships: once train-status shows the declared line or the CLI on 0.16 or later, follow the rules above with no exception. Anything else (a 0.16 CLI-only bump, an SDK release, a schema or installer change) still stops as below.

## Stop and ask instead of splitting the train

Stop, show what you found, and ask the person who owns the release when:

- the request moves one member to a MINOR the others are not on;
- the train is already split (train-status exits 1), and the request would release on top of the split, unless it is a 0.15.x CLI-side patch under the standing exception above;
- you cannot tell whether a change is breaking or a major feature;
- the change breaks the agent contract;
- train-status could not read a member (exit 2).

Put the full proposal in the question: every member, its current version and its proposed version, and the order. In a Catalyst workflow phase, raise it as a decision for a person through the workflow's ask route rather than continuing. Never repair a split by moving only the repository you are in.

## The release check

The release check refuses a version whose MAJOR.MINOR differs from the declared train and names the members still on the old line. Its interface is `bun run release:check` (behind `scripts/release-train-check.mjs`), the required CI job `Release train`, and the declaration `release-train.json` in catalyst-cloud. Where those exist in the repository you are in, run the check before you release and treat its failure as the train working: never bypass, weaken or skip it to get a release out. Where they do not exist yet, train-status is the check: run it before you release and again after the registry or installer shows the new version.

During an approved coordinated release, members publish one after another, so train-status reports the train split until the last member is out. That split is expected only when you can point to the approval: an answered decision, or a named person's written approval, that lists this member and the exact version you are about to publish. Publish that version and no other. Without that record, a split train means stop. The release is done when train-status exits 0 on the new line.

## Words a customer reads

Changelogs, GitHub Release notes, npm descriptions and installer output are read by customers. Say "workspace", never "tenant". Leave out hosts, runners, internal service names and people's names. Describe what changed for them, then what they do about it.
