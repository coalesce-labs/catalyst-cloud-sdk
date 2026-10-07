import { readFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const canonicalUrl = "https://staging.catalystcloud.dev/release-train.json";
const members = {
  cli: ["catalyst-cloud-skills", "package.json"],
  forwarder: ["catalyst-cloud-skills", "packages/catalyst-skills/package.json"],
  plugin: ["catalyst-cloud-skills", ".claude-plugin/plugin.json"],
  sdk: ["catalyst-cloud-sdk", "package.json"],
  "sdk-replica-node": ["catalyst-cloud-sdk", "modules/replica-node/package.json"],
  "sdk-replica-browser": ["catalyst-cloud-sdk", "modules/replica-browser/package.json"],
  schema: ["catalyst-cloud", "packages/schema/package.json"],
  replicate: ["catalyst-cloud", "packages/replicate/package.json"],
  "read-model": ["catalyst-cloud", "packages/read-model/package.json"],
  design: ["catalyst-cloud", "packages/design/package.json"],
  installer: ["catalyst-cloud", "packages/install-script/src/install-script.ts"],
};
const versionPattern = /^\d+\.\d+\.\d+$/;
const lineOf = (version) => version.split(".").slice(0, 2).join(".");

export function validateDeclaration(plan) {
  if (
    !plan ||
    !/^\d+\.\d+$/.test(plan.releaseLine) ||
    !["coordinated", "aligned"].includes(plan.state)
  )
    throw new Error("Invalid release line or transition state");
  const keys = Object.keys(plan.members ?? {}).sort();
  if (JSON.stringify(keys) !== JSON.stringify(Object.keys(members).sort()))
    throw new Error("Release declaration must include every member, exactly once");
  if (
    plan.state === "coordinated" &&
    (!/^CTC-\d+$/.test(plan.approval?.decision) ||
      plan.approval?.option !== "A" ||
      !plan.approval?.answeredBy ||
      !Number.isFinite(Date.parse(plan.approval?.answeredAt)) ||
      !plan.approval?.url?.startsWith("https://linear.app/coalesce-labs/issue/"))
  )
    throw new Error("Coordinated release needs its answered approval record");
  for (const [id, [repository, path]] of Object.entries(members)) {
    const entry = plan.members[id];
    if (entry.repository !== repository || entry.path !== path)
      throw new Error(`Wrong source identity for ${id}`);
    if (
      !versionPattern.test(entry.version) ||
      !versionPattern.test(entry.sourceVersion) ||
      lineOf(entry.version) !== plan.releaseLine
    )
      throw new Error(`Wrong version or line for ${id}`);
    const before = entry.sourceVersion.split(".").map(Number),
      after = entry.version.split(".").map(Number);
    const first = after.findIndex((n, i) => n !== before[i]);
    if (first >= 0 && after[first] < before[first])
      throw new Error(`Version would go backwards for ${id}`);
    const tag =
      repository === "catalyst-cloud-skills"
        ? "skills-bundle-v{version}"
        : repository === "catalyst-cloud-sdk"
          ? "v{version}"
          : null;
    if (entry.tag !== tag) throw new Error(`Wrong tag policy for ${id}`);
    if (typeof entry.publish !== "boolean")
      throw new Error(`Missing publication authority for ${id}`);
  }
  return plan;
}

export function validateVersion(plan, id, version, publishing = false) {
  const entry = plan.members[id];
  if (!entry || !versionPattern.test(version))
    throw new Error(`Unknown member or version: ${id}@${version}`);
  if (publishing && !entry.publish) throw new Error(`No publication approved for ${id}`);
  if (
    id === "design" &&
    !publishing &&
    lineOf(version) === plan.releaseLine &&
    Number(version.split(".")[2]) >= Number(entry.version.split(".")[2])
  )
    return;
  if (plan.state === "coordinated") {
    const allowed = publishing ? [entry.version] : [entry.sourceVersion, entry.version];
    if (!allowed.includes(version))
      throw new Error(
        `${id}@${version} is outside ${plan.approval.decision}'s exact approved transition`,
      );
  } else if (lineOf(version) !== plan.releaseLine)
    throw new Error(`${id}@${version} is off release line ${plan.releaseLine}`);
}

export function validateTag(plan, id, version, tag) {
  const pattern = plan.members[id]?.tag;
  if (pattern && tag !== pattern.replace("{version}", version))
    throw new Error(`Tag ${tag || "(missing)"} does not match ${id}@${version}`);
}

async function readJson(path) {
  return JSON.parse(await readFile(path, "utf8"));
}

export async function checkRelease(options = {}) {
  const checkout = options.root ?? root;
  const dryRun = options.dryRun === true || process.env.npm_config_dry_run === "true";
  const local = validateDeclaration(await readJson(join(checkout, "release-train.json")));
  const manifest = await readJson(join(checkout, "package.json"));
  const repository =
    manifest.name === "@catalyst-cloud/cli"
      ? "catalyst-cloud-skills"
      : manifest.name === "@catalyst-cloud/sdk"
        ? "catalyst-cloud-sdk"
        : manifest.name === "catalyst-cloud"
          ? "catalyst-cloud"
          : null;
  if (!repository) throw new Error("Unknown release-train repository");
  const publishIds =
    options.publish === "sdk"
      ? ["sdk", "sdk-replica-node", "sdk-replica-browser"]
      : options.publish === "cli"
        ? ["cli", "forwarder", "plugin"]
        : options.publish
          ? [options.publish]
          : [];
  if (publishIds.some((id) => !members[id] || members[id][0] !== repository))
    throw new Error("Publication member does not belong to this repository");
  if (publishIds.length) {
    const response = await (options.fetcher ?? fetch)(canonicalUrl, {
      signal: AbortSignal.timeout(15000),
      headers: { "cache-control": "no-cache" },
    });
    if (!response.ok)
      throw new Error(`Cannot read canonical release declaration: HTTP ${response.status}`);
    const canonical = validateDeclaration(await response.json());
    if (JSON.stringify(canonical) !== JSON.stringify(local))
      throw new Error(
        "Local release declaration differs from canonical deployed cloud; deploy the reviewed declaration before publication",
      );
    const deployed = await (options.fetcher ?? fetch)(
      "https://staging.catalystcloud.dev/install.sh",
      {
        signal: AbortSignal.timeout(15000),
        headers: { "cache-control": "no-cache" },
      },
    );
    const revision = deployed.headers.get("x-catalyst-install-script-revision");
    await deployed.body?.cancel();
    if (
      !deployed.ok ||
      !versionPattern.test(revision ?? "") ||
      lineOf(revision) !== local.releaseLine
    )
      throw new Error("The approved cloud release line is not deployed; stop before publication");
  }
  const packages = {};
  for (const [id, [repo, path]] of Object.entries(members)) {
    if (repo !== repository) continue;
    let version;
    if (id === "installer") {
      const source = await readFile(join(checkout, path), "utf8");
      version = source.match(/export const INSTALL_SCRIPT_REVISION = "([^"]+)"/)?.[1];
    } else {
      packages[id] = await readJson(join(checkout, path));
      version = packages[id].version;
    }
    validateVersion(local, id, version, publishIds.includes(id));
    if (publishIds.includes(id) && !dryRun)
      validateTag(local, id, version, options.tag ?? process.env.GITHUB_REF_NAME);
  }
  if (repository === "catalyst-cloud") {
    const source = await readFile(join(checkout, "packages/types/src/install-block.ts"), "utf8");
    if (source.match(/export const RELEASE_LINE = "([^"]+)"/)?.[1] !== local.releaseLine)
      throw new Error("Cloud RELEASE_LINE differs from its declaration");
    const pin = packages.replicate.dependencies?.["@catalyst-cloud/schema"];
    if (pin !== packages.schema.version) throw new Error("Replicate must pin this schema exactly");
  } else if (repository === "catalyst-cloud-sdk") {
    const version = packages.sdk.version;
    for (const id of ["sdk-replica-node", "sdk-replica-browser"]) {
      if (
        packages[id].version !== version ||
        packages[id].peerDependencies?.["@catalyst-cloud/sdk"] !== `^${version}`
      )
        throw new Error("SDK replicas must match core version and peer range");
    }
    if (publishIds.length) {
      const pinSets = [
        ["sdk", "devDependencies"],
        ["sdk", "peerDependencies"],
        ["sdk-replica-node", "dependencies"],
        ["sdk-replica-browser", "dependencies"],
      ];
      for (const [member, field] of pinSets) {
        for (const id of ["schema", "replicate"])
          if (packages[member][field]?.[`@catalyst-cloud/${id}`] !== local.members[id].version)
            throw new Error(`${member} ${field} must pin ${id} exactly`);
        if (
          packages[member][field]?.["@catalyst-cloud/read-model"] !==
          `^${local.members["read-model"].version}`
        )
          throw new Error(`${member} read-model range must stay on the declared line`);
      }
    }
  } else {
    const version = packages.cli.version;
    if (
      packages.forwarder.version !== version ||
      packages.plugin.version !== version ||
      packages.forwarder.dependencies?.["@catalyst-cloud/cli"] !== version
    )
      throw new Error("CLI, forwarder and plugin must carry one exact version");
    if (
      publishIds.length &&
      packages.cli.dependencies?.["@catalyst-cloud/sdk"] !== local.members.sdk.version
    )
      throw new Error("CLI must pin the approved SDK exactly");
  }
  return {
    repository,
    releaseLine: local.releaseLine,
    state: local.state,
    approval: local.approval?.decision,
    publishing: publishIds,
    dryRun,
  };
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    const args = process.argv.slice(2),
      options = {};
    for (let i = 0; i < args.length; i++) {
      const arg = args[i];
      if (arg === "--dry-run") {
        options.dryRun = true;
        continue;
      }
      if (!["--publish", "--tag"].includes(arg) || !args[i + 1])
        throw new Error(
          "Usage: release-train-check.mjs [--publish MEMBER] [--tag TAG] [--dry-run]",
        );
      options[arg.slice(2)] = args[++i];
    }
    console.log(JSON.stringify(await checkRelease(options)));
  } catch (error) {
    console.error(`Release train refused: ${error.message}`);
    process.exitCode = 1;
  }
}
