#!/usr/bin/env bun
// scripts/check-entity-drift.ts — the CLI the scheduled workflow runs. CTC-672.
//
// Thin on purpose: everything worth testing lives in `deployed-entity-drift.ts`, and this file is
// only the wiring plus the exit code.

import { checkDeployedEntityDrift } from "./deployed-entity-drift";
import { ENTITY_NAMES } from "../src/index";
import pkg from "../package.json" with { type: "json" };

const { ok } = await checkDeployedEntityDrift({
  sdkEntityNames: ENTITY_NAMES,
  sdkVersion: (pkg as { version: string }).version,
});

// ⛔ A NON-ZERO EXIT IS THE WHOLE POINT. This runs unattended on a cron; if it exited 0 on drift the
// only signal would be a log line nobody reads.
process.exit(ok ? 0 : 1);
