#!/usr/bin/env node
// run-outcome.mjs — run the release-train outcome evals (evals.json) with a real agent and grade them.
//
//   node run-outcome.mjs --out <dir> --config with_skill|without_skill [--eval <name>] [--runs N]
//        --catalyst-cloud-skills <checkout> --catalyst-cloud-sdk <checkout> --catalyst-cloud <checkout>
//        [--model <id>]
//
// Each run gets its own clone of the named repository at that checkout's origin/main. The clone
// shares objects with the checkout (git clone --shared) but keeps no ref except a detached HEAD and
// no remote, so other branches cannot reach the agent. With with_skill the skill is copied into the clone's .agents/skills (and linked from
// .claude/skills); without_skill removes any copy. The agent runs inside agentSandbox()
// (sandbox.mjs): no tokens in its environment, an empty npm config, and git and gh shims that refuse
// pushes, tag writes and GitHub writes however they are spelled.
//
// Grading: the first assertion of every eval ("no released version was changed …") is checked from
// everything the clone holds beyond its starting commit: commits, working-tree edits, untracked files
// and tags. The rest are judged by a separate `claude -p` call that sees the reply,
// the tool calls and the diff. Output per run: <out>/<eval>/<config>/run-<n>/{transcript.jsonl,
// reply.md, diff.patch, grading.json, timing.json}, in skill-creator's shapes, plus <out>/summary.json.
// Needs the `claude` CLI signed in. It costs real model usage: about one agent run and one judge
// call per eval per run.

import { execFileSync, spawn, spawnSync } from "node:child_process";
import { cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync, realpathSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { agentSandbox } from "./sandbox.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const skillDir = resolve(here, "..");
const EVALS = JSON.parse(readFileSync(join(here, "evals.json"), "utf8")).evals;

// The sandbox's shims are what stop a push or a GitHub write; this list only saves the agent turns.
const DENY = [
  "Bash(git push*)", "Bash(npm publish*)", "Bash(npm dist-tag*)", "Bash(npm deprecate*)",
  "Bash(bun publish*)", "Bash(gh pr create*)", "Bash(gh release create*)", "Bash(gh workflow run*)", "Bash(wrangler*)", "Bash(bunx wrangler*)",
];
const ALLOW = ["Bash", "Read", "Edit", "Write", "Glob", "Grep", "Skill"];

/** Lines of a diff that change a released version, a release line or a changelog version heading. */
export function versionChanges(diff) {
  return diff
    .split("\n")
    .filter((l) => /^[+-](?![+-])/.test(l))
    .filter((l) => /"version"\s*:|INSTALL_SCRIPT_REVISION\s*=|INSTALL_SETUP_CLI_VERSION\s*=|RELEASE_LINE\s*=|"releaseLine"\s*:|^[+-]##\s+v?\d+\.\d+\.\d+/.test(l));
}

/**
 * firstCheck(check, changed, newTags) → { passed, evidence } for an eval's first, model-free
 * assertion. With no check, nothing may change. With { patch_only: "X.Y.Z" }, every added version
 * line must name exactly X.Y.Z and none may touch the release line; tags are never allowed.
 */
export function firstCheck(check, changed, newTags) {
  const tags = newTags.length ? `; new tags: ${newTags.join(", ")}` : "";
  if (!check?.patch_only) {
    return changed.length === 0 && newTags.length === 0
      ? { passed: true, evidence: "no version lines changed, no new tags" }
      : { passed: false, evidence: `changed: ${changed.slice(0, 5).join(" | ")}${tags}` };
  }
  const want = check.patch_only;
  const added = changed.filter((l) => l.startsWith("+"));
  const wrong = added.filter((l) => /RELEASE_LINE|releaseLine|INSTALL_SCRIPT_REVISION|INSTALL_SETUP_CLI_VERSION/.test(l) || !l.includes(want));
  if (added.length === 0) return { passed: false, evidence: `no version line was changed to ${want}${tags}` };
  if (wrong.length || newTags.length) return { passed: false, evidence: `not ${want} only: ${wrong.slice(0, 5).join(" | ")}${tags}` };
  return { passed: true, evidence: `${added.length} version line(s), all ${want}; no new tags` };
}

function arg(argv, name) {
  const i = argv.indexOf(`--${name}`);
  return i === -1 ? undefined : argv[i + 1];
}

const git = (dir, args) => execFileSync("git", ["-C", dir, ...args], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();

function scratchClone(checkout, withSkill) {
  const dir = mkdtempSync(join(tmpdir(), "release-train-eval-"));
  const sha = git(checkout, ["rev-parse", "origin/main"]);
    execFileSync("git", ["clone", "-q", "--shared", "--no-checkout", "--no-tags", checkout, dir]);
  git(dir, ["checkout", "-q", "--detach", sha]);
  // Drop every ref but the detached HEAD, and the remote: other branches of the checkout (this
  // skill's own work branch, for one) must not reach the agent through git log --all or git grep.
  for (const ref of git(dir, ["for-each-ref", "--format=%(refname)"]).split("\n").filter(Boolean)) git(dir, ["update-ref", "-d", ref]);
  git(dir, ["remote", "remove", "origin"]);
  const target = join(dir, ".agents/skills/release-train");
  rmSync(target, { recursive: true, force: true });
  if (withSkill) {
    mkdirSync(dirname(target), { recursive: true });
    // The answer key stays out of the clone: copy the skill without its evals.
    cpSync(skillDir, target, { recursive: true, filter: (p) => !p.startsWith(join(skillDir, "evals")) });
    // Claude Code reads .claude/skills; link it when the repository does not already.
    if (!existsSync(join(dir, ".claude/skills"))) {
      mkdirSync(join(dir, ".claude"), { recursive: true });
      symlinkSync("../.agents/skills", join(dir, ".claude/skills"));
    }
  }
  // Commit the setup (the copied skill, the link) so grading starts after it, not at origin/main.
  git(dir, ["add", "-A"]);
  git(dir, ["-c", "user.email=eval@release-train.invalid", "-c", "user.name=eval", "commit", "-q", "--allow-empty", "-m", "eval setup"]);
  return { dir, startSha: git(dir, ["rev-parse", "HEAD"]), tagsBefore: git(dir, ["tag", "-l"]) };
}

/**
 * changesSince(dir, startSha) → { diff, stat, untracked }: commits, working-tree edits and untracked
 * files since the run began, so a bump the agent committed or left in a new file still shows.
 */
export function changesSince(dir, startSha) {
  const untracked = git(dir, ["ls-files", "--others", "--exclude-standard"]).split("\n").filter(Boolean);
  const added = untracked
    .map((f) => {
      try {
        return [`+++ b/${f}`, ...readFileSync(join(dir, f), "utf8").split("\n").map((l) => `+${l}`)].join("\n");
      } catch {
        return `+++ b/${f}`;
      }
    })
    .join("\n");
  const diff = [git(dir, ["diff", startSha]), added].filter(Boolean).join("\n");
  const stat = [git(dir, ["diff", startSha, "--stat"]), ...untracked.map((f) => ` ${f} (new, untracked)`)].filter(Boolean).join("\n");
  return { diff, stat, untracked };
}

function runAgent(dir, prompt, model) {
  const t0 = Date.now();
  const sb = agentSandbox();
  return new Promise((done) => {
    const p = spawn(
      "claude",
      ["-p", prompt, "--output-format", "stream-json", "--verbose", "--model", model, "--allowedTools", ...ALLOW, "--disallowedTools", ...DENY],
      { cwd: dir, env: sb.env, stdio: ["ignore", "pipe", "ignore"], detached: true },
    );
    let stdout = "";
    p.stdout.on("data", (c) => (stdout += c));
    // Kill the whole process group at the end or on timeout: no Bash child outlives the run.
    const killGroup = () => {
      try {
        process.kill(-p.pid, "SIGKILL");
      } catch {
        // already gone
      }
    };
    const timer = setTimeout(killGroup, 15 * 60_000);
    p.on("close", () => {
      clearTimeout(timer);
      killGroup();
      sb.dispose();
      const events = stdout.split("\n").filter(Boolean).flatMap((l) => {
        try {
          return [JSON.parse(l)];
        } catch {
          return [];
        }
      });
      const result = events.find((e) => e.type === "result");
      const tools = events
        .filter((e) => e.type === "assistant")
        .flatMap((e) => e.message?.content ?? [])
        .filter((c) => c.type === "tool_use")
        .map((c) => `${c.name}: ${JSON.stringify(c.input).slice(0, 240)}`);
      done({
        stdout,
        reply: result?.result ?? "",
        tools,
        loadedSkill: tools.some((t) => t.startsWith("Skill:") && t.includes("release-train")) || tools.some((t) => t.includes("release-train/SKILL.md")),
        seconds: (Date.now() - t0) / 1000,
        tokens: (result?.usage?.input_tokens ?? 0) + (result?.usage?.output_tokens ?? 0) + (result?.usage?.cache_read_input_tokens ?? 0),
      });
    });
  });
}

function judge(evalDef, run, diffStat, model) {
  const questions = evalDef.assertions.slice(1);
  const prompt = `You grade one agent run against assertions. Judge only from the evidence below; quote it.

Task given to the agent:
${evalDef.prompt}

Tool calls the agent made (in order, truncated):
${run.tools.join("\n") || "(none)"}

Files the agent changed (git diff --stat):
${diffStat || "(none)"}

The agent's final reply:
<<<
${run.reply}
>>>

Assertions:
${questions.map((q, i) => `${i + 1}. ${q}`).join("\n")}

Answer with only JSON, no prose: {"expectations":[{"text":"<assertion>","passed":true|false,"evidence":"<short quote or reason>"}]} with one entry per assertion, in order.`;
  const r = spawnSync("claude", ["-p", prompt, "--model", model, "--output-format", "json", "--tools", ""], {
    encoding: "utf8",
    maxBuffer: 64 << 20,
    timeout: 5 * 60_000,
    env: { ...process.env, CLAUDECODE: "" },
  });
  const text = (() => {
    try {
      return JSON.parse(r.stdout).result ?? "";
    } catch {
      return r.stdout;
    }
  })();
  const json = /\{[\s\S]*\}/.exec(text)?.[0];
  try {
    const parsed = JSON.parse(json).expectations;
    if (!Array.isArray(parsed) || parsed.length !== questions.length) throw new Error("wrong length");
    return parsed.map((e, i) => ({ text: questions[i], passed: e.passed === true, evidence: String(e.evidence ?? "") }));
  } catch {
    return questions.map((q) => ({ text: q, passed: false, evidence: `judge returned no usable verdict: ${text.slice(0, 200)}` }));
  }
}

async function main(argv) {
  const out = resolve(arg(argv, "out") ?? "release-train-outcome");
  const config = arg(argv, "config") ?? "with_skill";
  const model = arg(argv, "model") ?? "claude-opus-5-5";
  const runs = Number(arg(argv, "runs") ?? 1);
  const only = arg(argv, "eval");
  if (!["with_skill", "without_skill"].includes(config)) throw new Error("--config with_skill|without_skill");
  const summary = [];
  for (const e of EVALS.filter((x) => !only || x.name === only)) {
    const checkout = arg(argv, e.repo);
    if (!checkout) throw new Error(`--${e.repo} <checkout> is required for eval ${e.name}`);
    for (let n = 1; n <= runs; n++) {
      const runDir = join(out, e.name, config, `run-${n}`);
      mkdirSync(runDir, { recursive: true });
      const clone = scratchClone(resolve(checkout), config === "with_skill");
      try {
        const run = await runAgent(clone.dir, e.prompt, model);
        const { diff, stat: diffStat } = changesSince(clone.dir, clone.startSha);
        const newTags = git(clone.dir, ["tag", "-l"]).split("\n").filter((t) => t && !clone.tagsBefore.split("\n").includes(t));
        const changed = versionChanges(diff);
        const expectations = [{ text: e.assertions[0], ...firstCheck(e.check, changed, newTags) }, 
          ...judge(e, run, diffStat, model),
        ];
        const passed = expectations.filter((x) => x.passed).length;
        // A baseline that read the skill from anywhere measured the skill, not its absence.
        const contaminated = config === "without_skill" && run.tools.some((t) => /skills\/release-train|release-train\/SKILL\.md|train-status\.mjs/.test(t));
        const grading = {
          contaminated,
          expectations,
          summary: { passed, failed: expectations.length - passed, total: expectations.length, pass_rate: Math.round((passed / expectations.length) * 100) / 100 },
          loaded_skill: run.loadedSkill,
        };
        writeFileSync(join(runDir, "transcript.jsonl"), run.stdout);
        writeFileSync(join(runDir, "reply.md"), run.reply);
        writeFileSync(join(runDir, "diff.patch"), diff);
        writeFileSync(join(runDir, "grading.json"), `${JSON.stringify(grading, null, 2)}\n`);
        writeFileSync(join(runDir, "timing.json"), `${JSON.stringify({ total_tokens: run.tokens, total_duration_seconds: run.seconds }, null, 2)}\n`);
        summary.push({ eval: e.name, config, run: n, loaded_skill: run.loadedSkill, contaminated, ...grading.summary, failed_assertions: expectations.filter((x) => !x.passed).map((x) => x.text) });
        console.log(`${e.name} ${config} run-${n}: ${passed}/${expectations.length}${run.loadedSkill ? " (loaded release-train)" : ""}${contaminated ? " CONTAMINATED: the baseline read the skill; leave this run out" : ""}`);
      } finally {
        rmSync(clone.dir, { recursive: true, force: true });
      }
    }
  }
  mkdirSync(out, { recursive: true });
  writeFileSync(join(out, `summary-${config}.json`), `${JSON.stringify(summary, null, 2)}\n`);
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
    console.error(`run-outcome: ${e.message}`);
    process.exit(1);
  });
}
