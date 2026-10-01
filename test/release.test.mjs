import {describe, it, expect} from 'vitest';
import {publishedIntegrity, publicationNeeded, releaseCatalog, waitForPublication} from '../scripts/release.mjs';

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
    await waitForPublication(entry, 'sha512-ok', {now:() => clock, pause:async ms => {clock += ms;}, lookup:async () => ++calls < 3 ? null : 'sha512-ok'});
    expect(calls).toBe(3);
    expect(clock).toBe(10_000);
  });
  it('has a finite deadline when npm never makes the publication visible', async () => {
    let clock = 0;
    await expect(waitForPublication(entry, 'sha512-ok', {now:() => clock, pause:async ms => {clock += ms;}, lookup:async () => null})).rejects.toThrow('120 seconds');
    expect(clock).toBe(120_000);
  });
  it('refuses incorrect bytes instead of retrying them', async () => {
    await expect(waitForPublication(entry, 'sha512-ok', {lookup:async () => 'sha512-wrong'})).rejects.toThrow('integrity mismatch');
  });
});
