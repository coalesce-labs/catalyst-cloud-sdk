#!/usr/bin/env node
// run-trigger.mjs — does an agent working in a repository load the release-train skill before it
// changes anything? The in-repo counterpart to skill-creator's run_eval.py.
//
//   node run-trigger.mjs --repo <checkout> --out <file.json> [--ref <ref>] [--runs N] [--workers N]
//        [--model <id>] [--eval-set trigger-eval.json] [--only <substring>]
//
// Each run happens inside agentSandbox() (sandbox.mjs): no tokens in the agent's environment, and
// git and gh shims that refuse pushes, tag writes and GitHub writes.
//
// run_eval.py counts a trigger only when the agent's FIRST tool call reads the skill, with the
// description installed as a temporary command. Agents asked to release usually look at the repo
// first, so that score reads near zero even when the skill is used. This runner instead installs the
// real skill in a scratch clone of the repository and counts a trigger when the agent loads it at
// any point before its first file change. The run stops at the trigger, at the first Edit or Write,
// at the end of the reply, or after --timeout seconds; nothing it does leaves the clone.
//
// --ref picks what the clone is checked out at (default origin/main). Point it at a branch that
// already carries the skill, AGENTS.md pointer and rule to measure the whole set; otherwise the
// skill alone is copied in from this directory.

import { execFileSync, spawn } from "node:child_process";
import { cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync, realpathSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { agentSandbox } from "./sandbox.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const skillDir = resolve(here, "..");
const git = (dir, args) => execFileSync("git", ["-C", dir, ...args], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();

/**
 * classify(toolUse) → "trigger" when it loads the release-train skill, "change" when it edits a file
 * or runs a command that would change a version, otherwise null.
 */
/** A `git tag` that creates or deletes a tag; listing forms (-l, --list, --contains, bare) are reads. */
export function isTagWrite(command) {
  return command.split(/&&|\|\||;|\||\n/).some((part) => {
    const m = /\bgit(?:\s+(?:-C|-c)\s+\S+)*\s+tag\b(.*)$/.exec(part.trim());
    if (!m) return false;
    const args = m[1].trim().split(/\s+/).filter(Boolean);
    if (args.length === 0) return false;
    if (args.some((a) => /^(-l|--list|--contains|--no-contains|--points-at|--merged|--no-merged|--sort(=.*)?|-n\d*|--format(=.*)?|--column)$/.test(a))) return false;
    return true;
  });
}

/** A shell command that edits a file in place: sed -i, perl -i, npm pkg set, or a redirect into a file. */
export function isFileEdit(command) {
  return /\bsed\s+(-[a-zA-Z]*i|--in-place)|\bperl\s+-[a-zA-Z]*i|\bnpm\s+pkg\s+set\b|\btee\s+(-a\s+)?[\w./-]+\.(json|ts|md|mjs|js|sh|yml|yaml)\b|>{1,2}\s*[\w./-]+\.(json|ts|md|mjs|js|sh|yml|yaml)\b/.test(command);
}

/** Has anything in the clone changed since the setup commit? Catches a write by any means. */
export function worktreeChanged(dir) {
  return execFileSync("git", ["-C", dir, "status", "--porcelain"], { encoding: "utf8" }).trim() !== "";
}

export function classify(name, input) {
  const s = String(input?.command ?? "");
  if (name === "Skill" && /release-train/.test(String(input?.skill ?? ""))) return "trigger";
  if (name === "Read" && /release-train\/SKILL\.md/.test(String(input?.file_path ?? ""))) return "trigger";
  if (["Edit", "Write", "MultiEdit", "NotebookEdit"].includes(name)) return "change";
  if (name === "Bash" && (/npm version|npm publish|git push|gh release create|version:sync/.test(s) || isTagWrite(s) || isFileEdit(s))) return "change";
  return null;
}

function clone(checkout, ref) {
  const dir = mkdtempSync(join(tmpdir(), "release-train-trigger-"));
  const sha = git(checkout, ["rev-parse", ref]);
  execFileSync("git", ["clone", "-q", "--shared", "--no-checkout", "--no-tags", checkout, dir]);
  git(dir, ["checkout", "-q", "--detach", sha]);
  // Drop every ref but the detached HEAD, and the remote: other branches of the checkout (this
  // skill's own work branch, for one) must not reach the agent through git log --all or git grep.
  for (const ref of git(dir, ["for-each-ref", "--format=%(refname)"]).split("\n").filter(Boolean)) git(dir, ["update-ref", "-d", ref]);
  git(dir, ["remote", "remove", "origin"]);
  const target = join(dir, ".agents/skills/release-train");
  if (!existsSync(join(target, "SKILL.md"))) {
    mkdirSync(dirname(target), { recursive: true });
    // The answer key stays out of the clone: copy the skill without its evals.
    cpSync(skillDir, target, { recursive: true, filter: (p) => !p.startsWith(join(skillDir, "evals")) });
  }
  if (!existsSync(join(dir, ".claude/skills"))) {
    mkdirSync(join(dir, ".claude"), { recursive: true });
    symlinkSync("../.agents/skills", join(dir, ".claude/skills"));
  }
  // Commit the setup so a clean tree means the agent has changed nothing.
  git(dir, ["add", "-A"]);
  git(dir, ["-c", "user.email=eval@release-train.invalid", "-c", "user.name=eval", "commit", "-q", "--allow-empty", "-m", "eval setup"]);
  return dir;
}

const DENY = [
  "Bash(git push*)", "Bash(npm publish*)", "Bash(npm dist-tag*)", "Bash(npm deprecate*)",
  "Bash(bun publish*)", "Bash(gh pr create*)", "Bash(gh release create*)", "Bash(gh workflow run*)", "Bash(wrangler*)", "Bash(bunx wrangler*)",
];

function runOne(dir, query, model, timeoutS) {
  return new Promise((done) => {
    // The sandbox's shims are what stop a push or a GitHub write; the deny list only saves turns.
    const sb = agentSandbox();
    const p = spawn("claude", ["-p", query, "--output-format", "stream-json", "--verbose", "--include-partial-messages", "--model", model, "--disallowedTools", ...DENY], {
      cwd: dir,
      env: sb.env,
      stdio: ["ignore", "pipe", "ignore"],
      detached: true,
    });
    let buf = "";
    let pending = null;
    let json = "";
    const tools = [];
    let finished = false;
    const finish = (outcome) => {
      if (finished) return;
      finished = true;
      clearTimeout(timer);
      // Kill the whole process group: a Bash child the agent started must not outlive the run.
      try {
        process.kill(-p.pid, "SIGKILL");
      } catch {
        // already gone
      }
      sb.dispose();
      done({ outcome, tools });
    };
    const timer = setTimeout(() => finish("timeout"), timeoutS * 1000);
    p.stdout.on("data", (chunk) => {
      buf += chunk;
      let i;
      while ((i = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, i).trim();
        buf = buf.slice(i + 1);
        if (!line) continue;
        let ev;
        try {
          ev = JSON.parse(line);
        } catch {
          continue;
        }
        if (ev.type === "stream_event") {
          const se = ev.event ?? {};
          if (se.type === "content_block_start" && se.content_block?.type === "tool_use") {
            pending = se.content_block.name;
            json = "";
          } else if (se.type === "content_block_delta" && pending && se.delta?.type === "input_json_delta") json += se.delta.partial_json;
          else if (se.type === "content_block_stop" && pending) {
            let input;
            try {
              input = JSON.parse(json || "{}");
            } catch {
              input = { unparsed: json };
            }
            tools.push(`${pending}: ${JSON.stringify(input).slice(0, 600)}`);
            const c = classify(pending, input);
            pending = null;
            // A file changed before the skill loaded (by a command no regex recognises) is a change.
            if (c === "trigger") return finish(worktreeChanged(dir) ? "change" : "trigger");
            if (c === "change") return finish("change");
          }
        } else if (ev.type === "result") return finish("end");
      }
    });
    p.on("close", () => finish("end"));
  });
}

async function pool(items, n, fn) {
  const out = new Array(items.length);
  let next = 0;
  await Promise.all(
    Array.from({ length: n }, async () => {
      while (next < items.length) {
        const i = next++;
        out[i] = await fn(items[i]);
      }
    }),
  );
  return out;
}

async function main(argv) {
  const arg = (n) => (argv.indexOf(`--${n}`) === -1 ? undefined : argv[argv.indexOf(`--${n}`) + 1]);
  const repo = arg("repo");
  if (!repo) throw new Error("--repo <checkout> is required");
  const set = JSON.parse(readFileSync(arg("eval-set") ?? join(here, "trigger-eval.json"), "utf8")).filter((q) => !arg("only") || q.query.includes(arg("only")));
  const runs = Number(arg("runs") ?? 2);
  const jobs = set.flatMap((q) => Array.from({ length: runs }, (_, k) => ({ ...q, k })));
  const results = await pool(jobs, Number(arg("workers") ?? 6), async (job) => {
    const dir = clone(resolve(repo), arg("ref") ?? "origin/main");
    try {
      const r = await runOne(dir, job.query, arg("model") ?? "claude-opus-5-5", Number(arg("timeout") ?? 180));
      process.stderr.write(`${r.outcome.padEnd(7)} ${job.should_trigger ? "+" : "-"} ${job.query.slice(0, 70)}\n`);
      return { ...job, ...r };
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
  const byQuery = set.map((q) => {
    const rs = results.filter((r) => r.query === q.query);
    const triggers = rs.filter((r) => r.outcome === "trigger").length;
    const rate = triggers / rs.length;
    return { query: q.query, should_trigger: q.should_trigger, triggers, runs: rs.length, trigger_rate: rate, pass: q.should_trigger ? rate >= 0.5 : rate < 0.5, outcomes: rs.map((r) => r.outcome), tools: rs.map((r) => r.tools) };
  });
  const pos = byQuery.filter((q) => q.should_trigger);
  const neg = byQuery.filter((q) => !q.should_trigger);
  const summary = {
    repo: resolve(repo),
    ref: arg("ref") ?? "origin/main",
    passed: byQuery.filter((q) => q.pass).length,
    total: byQuery.length,
    positive_trigger_rate: pos.reduce((a, q) => a + q.triggers, 0) / Math.max(1, pos.reduce((a, q) => a + q.runs, 0)),
    negative_trigger_rate: neg.reduce((a, q) => a + q.triggers, 0) / Math.max(1, neg.reduce((a, q) => a + q.runs, 0)),
  };
  const out = arg("out");
  if (out) writeFileSync(out, `${JSON.stringify({ summary, results: byQuery }, null, 2)}\n`);
  console.log(JSON.stringify(summary));
}

// Compare real paths: agents run these through the .claude/skills symlink, and a plain path
// comparison would skip main() there and exit 0 having done nothing.
const isMain = (() => {
  try {
    return Boolean(process.argv[1]) && realpathSync(process.argv[1]) === realpathSync(fileURLToPath(import.meta.url));
  } catch {
    return false;
  }
})();
if (isMain) {
  main(process.argv.slice(2)).catch((e) => {
    console.error(`run-trigger: ${e.message}`);
    process.exit(1);
  });
}
