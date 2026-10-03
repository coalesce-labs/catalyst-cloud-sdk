# release-train evals

Two questions: does an agent load this skill when it is about to change a release version, and once it has, does it keep the train whole?

| file | what it is |
| -- | -- |
| `trigger-eval.json` | 22 queries in skill-creator's format: 12 that should load the skill (bumps, publishes, tags, the installer revision, the contract line) and 10 near misses that share words with it (a dependency bump, changelog wording, a merge-queue hold, a failed publish run). |
| `run-trigger.mjs` | Runs each query with the real skill installed in a scratch clone of a repository, and counts a trigger when the agent loads the skill before its first file change. |
| `evals.json` | Three outcome evals in skill-creator's format: a MINOR bump to the CLI alone, an additive SDK change, and a breaking contract change. |
| `run-outcome.mjs` | Runs each outcome eval in a scratch clone of its repository, with or without the skill, then grades it: the diff and tags for "no version changed", a judge model for the rest. Writes skill-creator's `grading.json` and `timing.json` per run. |
| `sandbox.mjs` | The agent's environment for both runners: no tokens, an empty npm config, and refusing `git` and `gh` shims. |
| `runners.test.mjs` | Unit tests for the parts of the runners that need no model, the sandbox's shims included. |
| `results/` | Recorded runs: date, model, repository, scores. |

Both runners need the `claude` CLI signed in and cost real model usage. Each run happens in a scratch clone that keeps no ref except a detached HEAD and no remote, so other branches cannot hand a baseline agent the skill, inside `sandbox.mjs`: the agent's environment holds no tokens, npm reads an empty config, and `git` and `gh` are shims that refuse pushes, tag writes, remote changes and every GitHub write however they are spelled. When a run ends, its whole process group is killed.

## Trigger

```bash
node run-trigger.mjs --repo <checkout> --runs 2 --out results/<date>-trigger-<repo>.json
```

`--ref` checks the clone out at a branch that already carries the skill, the AGENTS.md pointer and the rule, to measure the whole set rather than the skill alone.

skill-creator's own `run_eval.py` also runs against `trigger-eval.json`, but it counts a trigger only when the agent's first tool call reads the skill, with the description installed as a temporary command. An agent asked to release looks at the repository first, so that score stays near zero even when the skill is used. Use it to compare descriptions with each other; use `run-trigger.mjs` for whether the skill is reached.

## Outcome

```bash
node run-outcome.mjs --config with_skill --out <dir> \
  --catalyst-cloud-skills <checkout> --catalyst-cloud-sdk <checkout> --catalyst-cloud <checkout>
node run-outcome.mjs --config without_skill --out <dir> …
```

Compare `summary-with_skill.json` with `summary-without_skill.json`. A baseline run that read the skill anyway (from disk elsewhere on the machine) is marked `contaminated`; leave it out. The skill earns its place when a with-skill run passes an assertion the baseline fails, above all "no released version was changed".

Re-run both after changing the description, the rules, or the members table.
