---
paths:
  - "package.json"
  - "modules/replica-node/package.json"
  - "modules/replica-browser/package.json"
  - "CHANGELOG.md"
  - "scripts/release.mjs"
  - ".github/workflows/publish.yml"
---

# Release train

This repository releases three members of Catalyst's shared release train: `@catalyst-cloud/sdk`, `@catalyst-cloud/sdk-replica-node` and `@catalyst-cloud/sdk-replica-browser`. Before you change their version, create a `v*` GitHub Release, or edit the publish workflow, load the `release-train` skill (`.agents/skills/release-train/SKILL.md`) and run its `.agents/skills/release-train/scripts/train-status.mjs`.

The short form: every member shares one MAJOR.MINOR. Additive changes, such as a new method or field, are patches. A MINOR moves for every member at once, with the CLI and catalyst-cloud, or not at all. When a request would split the train, stop and ask instead of bumping.
