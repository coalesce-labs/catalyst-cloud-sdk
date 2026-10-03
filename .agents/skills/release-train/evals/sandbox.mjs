// sandbox.mjs — the environment an eval agent runs in, so that nothing it does can publish, tag,
// push or write to GitHub, whatever the command looks like.
//
// - `git` and `gh` on the agent's PATH are shims. They pass reads through to the real tools and
//   refuse pushes, tag writes, remote changes, credential reads and every GitHub write.
// - Tokens and passwords are removed from the agent's environment. The gh shim alone holds the
//   GitHub token, so read-only `gh api` calls (train-status reads the declared line that way) work.
// - npm reads an empty user config, so it has no credential to publish with.
// Call agentSandbox() once per run and dispose() it afterwards.

import { execFileSync } from "node:child_process";
import { chmodSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const GH_READ = {
  repo: ["view", "list"],
  pr: ["view", "list", "diff", "checks", "status"],
  release: ["view", "list"],
  run: ["view", "list"],
  issue: ["view", "list", "status"],
  workflow: ["view", "list"],
  auth: ["status"],
};

/** May the eval agent run `gh <args>`? Reads only. */
export function ghAllowed(args) {
  const [cmd, sub] = args;
  if (cmd === "search") return true;
  if (cmd === "api") {
    for (let i = 1; i < args.length; i++) {
      const a = args[i];
      if (/^(-f|-F|--field|--raw-field|--input)(=|$)/.test(a) || /^-[fF]./.test(a)) return false;
      const m = /^(?:-X|--method)(?:=?(.+))?$/.exec(a);
      if (m) {
        const method = (m[1] ?? args[i + 1] ?? "").toUpperCase();
        if (method !== "GET") return false;
      }
    }
    return true;
  }
  return GH_READ[cmd]?.includes(sub) ?? false;
}

/** May the eval agent run `git <args>`? Everything except pushes, tag writes and remote or credential changes. */
export function gitAllowed(args) {
  let i = 0;
  while (i < args.length) {
    if (args[i] === "-C" || args[i] === "-c") i += 2;
    else if (/^--(git-dir|work-tree|namespace)=/.test(args[i]) || /^-c./.test(args[i])) i += 1;
    else break;
  }
  const [sub, ...rest] = args.slice(i);
  if (sub === "push" || sub === "credential" || sub === "send-pack") return false;
  if (sub === "remote") return !["add", "set-url", "rename", "remove", "rm"].includes(rest[0]);
  if (sub === "config") return !rest.some((a) => /credential|^url\.|insteadof|pushurl/i.test(a));
  if (sub === "tag") {
    if (rest.length === 0) return true;
    return rest.some((a) => /^(-l|--list|--contains|--no-contains|--points-at|--merged|--no-merged|--sort(=.*)?|-n\d*|--format(=.*)?|--column|-v|--verify)$/.test(a));
  }
  return true;
}

const SECRET = /(^|_)(TOKEN|PASSWD|PASSWORD|SECRET|AUTH)(_|$)/i;

function shim(dir, name, real, decide, extraEnv) {
  const file = join(dir, name);
  const body = `#!/usr/bin/env node
const { spawnSync } = require("node:child_process");
const args = process.argv.slice(2);
const allowed = (${decide.toString()})(args);
if (!allowed) {
  console.error("refused by the eval sandbox: ${name} " + args.join(" "));
  process.exit(1);
}
const r = spawnSync(${JSON.stringify(real)}, args, { stdio: "inherit", env: { ...process.env, ...${JSON.stringify(extraEnv)} } });
process.exit(r.status ?? 1);
`;
  writeFileSync(file, body, { mode: 0o700 });
  chmodSync(file, 0o700);
}

/** agentSandbox() → { env, dispose }. env is the environment to start the agent with. */
export function agentSandbox(base = process.env) {
  const dir = mkdtempSync(join(tmpdir(), "release-train-sandbox-"));
  chmodSync(dir, 0o700);
  const which = (n) => execFileSync("sh", ["-c", `command -v ${n}`], { encoding: "utf8" }).trim();
  const ghToken = base.GH_TOKEN || base.GITHUB_TOKEN || "";
  // The function bodies are inlined into the shims, so they must be self-contained (gh's uses GH_READ).
  const ghDecide = new Function("args", `const GH_READ = ${JSON.stringify(GH_READ)}; return (${ghAllowed.toString()})(args);`);
  shim(dir, "git", which("git"), gitAllowed, {});
  shim(dir, "gh", which("gh"), ghDecide, ghToken ? { GH_TOKEN: ghToken } : {});
  writeFileSync(join(dir, "npmrc"), "");
  const env = {};
  for (const [k, v] of Object.entries(base)) {
    if (SECRET.test(k) && !/^(ANTHROPIC|CLAUDE)_/.test(k)) continue;
    env[k] = v;
  }
  env.PATH = `${dir}:${base.PATH ?? ""}`;
  env.NPM_CONFIG_USERCONFIG = join(dir, "npmrc");
  env.CLAUDECODE = "";
  return { env, dispose: () => rmSync(dir, { recursive: true, force: true }) };
}
