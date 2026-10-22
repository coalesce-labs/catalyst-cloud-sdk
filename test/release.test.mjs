import {describe, it, expect} from 'vitest';
import {npmRefusedExistingVersion, publishPacked, publishedIntegrity, publicationNeeded, readPublishedIntegrity, releaseCatalog, waitForPublication} from '../scripts/release.mjs';

describe('release recovery', () => {
  const entry = {name:'@catalyst-cloud/sdk', version:'0.14.0'};
  it('publishes an absent version and skips only identical published bytes', () => {
    expect(publicationNeeded(entry, 'sha512-new', null)).toBe(true);
    expect(publicationNeeded(entry, 'sha512-new', 'sha512-new')).toBe(false);
    expect(() => publicationNeeded(entry, 'sha512-new', 'sha512-old')).toThrow('bump the version');
  });
  it('does not treat auth or registry failure as package absence', async () => {
    for (const status of [401, 403, 429, 500]) {
      await expect(publishedIntegrity(entry.name, entry.version, async () => new Response('', {status}))).rejects.toThrow(`HTTP ${status}`);
    }
    expect(await publishedIntegrity(entry.name, entry.version, async () => new Response('', {status:404}))).toBeNull();
    expect(await publishedIntegrity(entry.name, entry.version, async () => Response.json({...entry, dist:{integrity:'sha512-ok'}}))).toBe('sha512-ok');
    await expect(publishedIntegrity(entry.name, entry.version, async () => Response.json({...entry, version:'0.13.1', dist:{integrity:'sha512-ok'}}))).rejects.toThrow('Invalid registry metadata');
  });
  it('keeps optional modules on the core version and publishes core first', () => {
    const entries = releaseCatalog(process.cwd());
    expect(entries.map(entry => entry.name)).toEqual(['@catalyst-cloud/sdk','@catalyst-cloud/sdk-replica-node','@catalyst-cloud/sdk-replica-browser']);
  });
});

describe('registry publication visibility', () => {
  const entry = {name:'@catalyst-cloud/sdk', version:'0.14.0'};
  it('waits for a processing package to become visible', async () => {
    let clock = 0;
    let calls = 0;
    await waitForPublication(entry, 'sha512-ok', {now:() => clock, pause:async ms => {clock += ms;}, lookup:async () => ++calls < 3 ? null : 'sha512-ok', log:() => {}});
    expect(calls).toBe(3);
    expect(clock).toBe(10_000);
  });
  it('has a finite deadline when npm never makes the publication visible', async () => {
    let clock = 0;
    await expect(waitForPublication(entry, 'sha512-ok', {now:() => clock, pause:async ms => {clock += ms;}, lookup:async () => null, log:() => {}})).rejects.toThrow('900 seconds');
    expect(clock).toBe(900_000);
  });
  // CTC-6545 — 0.16.12 went red on one 15 s lookup timeout although npm had accepted the publish.
  it('keeps waiting through a read that fails, and names the last failure if npm never answers', async () => {
    let clock = 0;
    let calls = 0;
    const timeout = () => { throw new DOMException('The operation was aborted due to timeout', 'TimeoutError'); };
    await waitForPublication(entry, 'sha512-ok', {now:() => clock, pause:async ms => {clock += ms;}, lookup:async () => ++calls < 3 ? timeout() : 'sha512-ok', log:() => {}});
    expect(calls).toBe(3);
    clock = 0;
    await expect(waitForPublication(entry, 'sha512-ok', {now:() => clock, pause:async ms => {clock += ms;}, lookup:async () => timeout(), log:() => {}})).rejects.toThrow('last read: The operation was aborted due to timeout');
  });
  it('logs how long npm took to show the version', async () => {
    let clock = 0;
    const lines = [];
    await waitForPublication(entry, 'sha512-ok', {now:() => clock, pause:async ms => {clock += ms;}, lookup:async () => clock < 420_000 ? null : 'sha512-ok', log:line => lines.push(line)});
    expect(lines).toEqual(['Registry visible after 420 s: @catalyst-cloud/sdk@0.14.0']);
  });
  // CTC-6545 review F1 — the window is shared across packages, so a later package can start past it.
  it('reads a package once even when the shared window has already passed', async () => {
    let calls = 0;
    await waitForPublication(entry, 'sha512-ok', {started:0, now:() => 900_001, pause:async () => {}, lookup:async () => { calls++; return 'sha512-ok'; }, log:() => {}});
    expect(calls).toBe(1);
    calls = 0;
    await expect(waitForPublication(entry, 'sha512-ok', {started:0, now:() => 900_001, pause:async () => {}, lookup:async () => { calls++; return null; }, log:() => {}})).rejects.toThrow('900 seconds');
    expect(calls).toBe(1);
  });
  it('refuses incorrect bytes instead of retrying them', async () => {
    await expect(waitForPublication(entry, 'sha512-ok', {lookup:async () => 'sha512-wrong'})).rejects.toThrow('integrity mismatch');
  });
});

describe('release rerun (CTC-6552)', () => {
  const entry = {name:'@catalyst-cloud/sdk-replica-browser', version:'0.16.17'};
  const timeout = () => { throw new DOMException('The operation was aborted due to timeout', 'TimeoutError'); };
  it('reads again when the pre-publish read times out once', async () => {
    let calls = 0;
    const pauses = [];
    const integrity = await readPublishedIntegrity(entry, {lookup:async () => ++calls === 1 ? timeout() : 'sha512-ok', pause:async ms => { pauses.push(ms); }});
    expect(integrity).toBe('sha512-ok');
    expect(calls).toBe(2);
    expect(pauses).toEqual([5_000]);
  });
  it('returns a 404 answer at once and fails only after every attempt throws', async () => {
    let calls = 0;
    expect(await readPublishedIntegrity(entry, {lookup:async () => { calls++; return null; }, pause:async () => {}})).toBeNull();
    expect(calls).toBe(1);
    calls = 0;
    await expect(readPublishedIntegrity(entry, {lookup:async () => { calls++; timeout(); }, pause:async () => {}})).rejects.toThrow('aborted due to timeout');
    expect(calls).toBe(4);
  });
  it('recognises npm refusing a version it already holds, and nothing else', () => {
    expect(npmRefusedExistingVersion('npm error code E403\nnpm error 403 403 Forbidden - PUT https://registry.npmjs.org/@catalyst-cloud%2fsdk-replica-browser - You cannot publish over the previously published versions: 0.16.17.')).toBe(true);
    expect(npmRefusedExistingVersion('npm error code EPUBLISHCONFLICT')).toBe(true);
    for (const stderr of ['npm error code E401\nnpm error Unable to authenticate', 'npm error code E403\nnpm error 403 Forbidden - You do not have permission to publish', '', undefined]) {
      expect(npmRefusedExistingVersion(stderr), String(stderr)).toBe(false);
    }
  });
  it('treats a refused republish as already accepted and fails any other publish', () => {
    const ran = result => () => ({status:1, signal:null, stderr:'', ...result});
    const quiet = {writeErr:() => {}};
    expect(publishPacked('pkg.tgz', {...quiet, spawn:ran({status:0})})).toBe('published');
    expect(publishPacked('pkg.tgz', {...quiet, spawn:ran({stderr:'npm error 403 403 Forbidden - You cannot publish over the previously published versions: 0.16.17.'})})).toBe('already-accepted');
    expect(() => publishPacked('pkg.tgz', {...quiet, spawn:ran({stderr:'npm error code E401'})})).toThrow('npm publish pkg.tgz failed with exit 1');
    expect(() => publishPacked('pkg.tgz', {...quiet, spawn:ran({stderr:undefined})})).toThrow('failed with exit 1');
    expect(() => publishPacked('pkg.tgz', {...quiet, spawn:ran({error:new Error('spawn npm ENOENT')})})).toThrow('spawn npm ENOENT');
  });
  // Review F1 on #219 — a green publish keeps npm's notices, the provenance link among them.
  it('writes npm stderr to the log on success as well as on failure', () => {
    const notice = 'npm notice publish Provenance statement published to transparency log: https://search.sigstore.dev/?logIndex=1\n';
    for (const status of [0, 1]) {
      const written = [];
      try {
        publishPacked('pkg.tgz', {spawn:() => ({status, signal:null, stderr:notice}), writeErr:text => written.push(text)});
      } catch {}
      expect(written, `status ${status}`).toEqual([notice]);
    }
  });
  it('still refuses different bytes after a refused republish: the wait checks integrity', async () => {
    await expect(waitForPublication(entry, 'sha512-new', {now:() => 0, pause:async () => {}, lookup:async () => 'sha512-old', log:() => {}})).rejects.toThrow('integrity mismatch');
  });
});
