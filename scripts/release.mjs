// Publish one tested SDK release, core first. Recover safely after a partial publication.
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
  const response = await fetcher(`https://registry.npmjs.org/${encodeURIComponent(name)}/${encodeURIComponent(version)}`, {signal:AbortSignal.timeout(15_000)});
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
export async function waitForPublication(entry, integrity, options = {}) {
  const lookup = options.lookup ?? publishedIntegrity;
  const now = options.now ?? Date.now;
  const pause = options.pause ?? (ms => new Promise(resolve => setTimeout(resolve, ms)));
  const deadline = now() + 120_000;
  while (now() < deadline) {
    const published = await lookup(entry.name, entry.version);
    if (published !== null) {
      if (published !== integrity) throw new Error(`Publication integrity mismatch for ${entry.name}@${entry.version}`);
      return;
    }
    await pause(Math.min(5_000, Math.max(0, deadline - now())));
  }
  throw new Error(`Registry visibility timed out after 120 seconds for ${entry.name}@${entry.version}; rerun this release`);
}
export async function release(root = process.cwd()) {
  const catalog = releaseCatalog(root);
  if (process.env.GITHUB_EVENT_NAME === 'release' && process.env.GITHUB_REF_NAME !== `v${catalog[0].version}`) throw new Error('Release tag does not match the package version');
  const scratch = mkdtempSync(join(tmpdir(), 'catalyst-sdk-release-'));
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
      await waitForPublication(entry, packed.integrity);
      console.log(`Verified publication: ${entry.name}@${entry.version}`);
    }
  } finally { rmSync(scratch, {recursive:true, force:true}); }
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) await release();
