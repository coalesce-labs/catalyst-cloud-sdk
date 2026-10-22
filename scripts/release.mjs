// Publish one tested SDK release, core first. Recover safely after a partial publication.
import { checkRelease } from './release-train-check.mjs';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

export const releasePaths = ['.', 'modules/replica-node', 'modules/replica-browser'];
export function releaseCatalog(root) {
  const entries = releasePaths.map(path => ({path, ...JSON.parse(readFileSync(join(root, path, 'package.json'), 'utf8'))}));
  const version = entries[0].version;
  if (entries.some(entry => entry.version !== version)) throw new Error('Core and replica versions must match');
  if (entries.slice(1).some(entry => entry.peerDependencies['@catalyst-cloud/sdk'] !== `^${version}`)) throw new Error('Replica SDK peers must match this release');
  return entries;
}
export async function publishedIntegrity(name, version, fetcher = fetch) {
  const response = await fetcher(`https://registry.npmjs.org/${encodeURIComponent(name)}/${encodeURIComponent(version)}`, {signal:AbortSignal.timeout(15_000), headers:{"cache-control":"no-cache"}});
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(`Registry lookup failed for ${name}@${version}: HTTP ${response.status}`);
  const metadata = await response.json();
  if (metadata.name !== name || metadata.version !== version || typeof metadata.dist?.integrity !== 'string') throw new Error(`Invalid registry metadata for ${name}@${version}`);
  return metadata.dist.integrity;
}
export function publicationNeeded(entry, packedIntegrity, registryIntegrity) {
  if (registryIntegrity === null) return true;
  if (registryIntegrity !== packedIntegrity) throw new Error(`Published bytes differ for ${entry.name}@${entry.version}; bump the version instead of replacing it`);
  return false;
}
// CTC-6545: npm accepted every publish behind the red 0.16.12, 0.16.16 and 0.16.17 releases, and its
// per-version document only became readable after the old 300 s window (0.16.17 read 200 by about
// 8 minutes after its publish). A read that fails or times out is "not visible yet", never the
// release's verdict.
export const VISIBILITY_WINDOW_MS = 900_000;
export async function waitForPublication(entry, integrity, options = {}) {
  const lookup = options.lookup ?? publishedIntegrity;
  const now = options.now ?? Date.now;
  const pause = options.pause ?? (ms => new Promise(resolve => setTimeout(resolve, ms)));
  const log = options.log ?? console.log;
  const started = options.started ?? now();
  const deadline = started + VISIBILITY_WINDOW_MS;
  let lastError = null;
  // Read at least once: a package checked after an earlier one spent the shared window may already
  // be visible, and a window that has passed must not fail it unread.
  for (;;) {
    let published = null;
    try {
      published = await lookup(entry.name, entry.version);
      lastError = null;
    } catch (error) {
      lastError = error;
    }
    if (published !== null) {
      if (published !== integrity) throw new Error(`Publication integrity mismatch for ${entry.name}@${entry.version}`);
      log(`Registry visible after ${Math.round((now() - started) / 1000)} s: ${entry.name}@${entry.version}`);
      return;
    }
    if (now() >= deadline) break;
    await pause(Math.min(5_000, Math.max(0, deadline - now())));
  }
  const cause = lastError === null ? '' : ` (last read: ${lastError.message})`;
  throw new Error(`Registry visibility timed out after ${VISIBILITY_WINDOW_MS / 1000} seconds for ${entry.name}@${entry.version}${cause}; rerun this release`);
}
export async function release(root = process.cwd()) {
  await checkRelease({root, publish: 'sdk'});
  const catalog = releaseCatalog(root);
  if (process.env.GITHUB_EVENT_NAME === 'release' && process.env.GITHUB_REF_NAME !== `v${catalog[0].version}`) throw new Error('Release tag does not match the package version');
  const scratch = mkdtempSync(join(tmpdir(), 'catalyst-sdk-release-'));
  const published = [];
  try {
    for (const entry of catalog) {
      const packed = JSON.parse(execFileSync('npm', ['pack', '--json', '--pack-destination', scratch], {cwd:resolve(root, entry.path), encoding:'utf8'}))[0];
      const existing = await publishedIntegrity(entry.name, entry.version);
      if (!publicationNeeded(entry, packed.integrity, existing)) {
        console.log(`Already published identical artifact: ${entry.name}@${entry.version}`);
        continue;
      }
      const env = {...process.env};
      // Only the two new module names may use the short-lived bootstrap credential.
      // Core and ordinary releases keep npm's GitHub OIDC exchange.
      if (entry.path !== '.' && process.env.SDK_BOOTSTRAP_TOKEN) {
        const config = join(scratch, 'bootstrap.npmrc');
        writeFileSync(config, '//registry.npmjs.org/:_authToken=${SDK_BOOTSTRAP_TOKEN}\n', {mode:0o600});
        env.NPM_CONFIG_USERCONFIG = config;
      } else {
        delete env.SDK_BOOTSTRAP_TOKEN;
      }
      execFileSync('npm', ['publish', join(scratch, packed.filename), '--provenance', '--access', 'public'], {cwd:root, env, stdio:'inherit'});
      published.push({entry, integrity:packed.integrity});
    }
  } finally { rmSync(scratch, {recursive:true, force:true}); }
  // One window for every package published above, measured from the last publish, so a slow
  // read-back on one package does not spend the job's timeout three times over.
  const started = Date.now();
  for (const {entry, integrity} of published) {
    await waitForPublication(entry, integrity, {started});
    console.log(`Verified publication: ${entry.name}@${entry.version}`);
  }
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) await release();
