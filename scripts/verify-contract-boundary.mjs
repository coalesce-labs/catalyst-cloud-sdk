// CTC-4633: execute integrity-verified published CLI/SDK artifacts against complete
// synthetic cloud documents. Adapted from onboarding-runner's published CLI boundary
// instrument; no real identity, tenant, provider, browser or mutation is used.
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {mkdtempSync,readFileSync,writeFileSync,mkdirSync,symlinkSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {pathToFileURL} from 'node:url';

const workspace = process.cwd();
const scratch = mkdtempSync(join(tmpdir(), 'sdk-contract-boundary-'));
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
const integrity = bytes => 'sha512-' + createHash('sha512').update(bytes).digest('base64');
const expected = {
  cli: 'sha512-tw85zQDHSLJymPRyKd/1uexTGwimIxijdfm0BJPtJEsb4iW7dwiebPyja3PwzSAFdY/zzDtqP124PmA2fXpZvQ==',
  '0.13.1': 'sha512-EjgkgsmoMqw+thw1hhC7t+xE1NExW2hIPJn9Q3IacCxMH+ITAiZmVUPOQdHqRnfy/ODEYmHvdMeEPDM9oaN8Ow==',
  '0.14.0': 'sha512-94CBW6Q84h6I8VjTUGr0I6EUpvYs7PZI+GUCkIE/s5wgPsGWrIP+DCICvbZR/rfsd+O5VJPClweOs5q76b35xg==',
};
async function published(name, version, expectedIntegrity) {
  const response = await fetch(`https://registry.npmjs.org/@catalyst-cloud/${name}/${version}`, {signal: AbortSignal.timeout(30000)});
  assert.equal(response.status, 200);
  const metadata = await response.json();
  assert.equal(metadata.version, version);
  assert.equal(metadata.dist.integrity, expectedIntegrity);
  const download = await fetch(metadata.dist.tarball, {signal: AbortSignal.timeout(30000)});
  assert.equal(download.status, 200);
  const bytes = Buffer.from(await download.arrayBuffer());
  assert.equal(integrity(bytes), metadata.dist.integrity);
  return {bytes, version, integrity: metadata.dist.integrity, sha256: sha256(bytes)};
}
function unpack(artifact, directory) {
  mkdirSync(directory, {recursive: true});
  const tar = join(directory, 'artifact.tgz');
  writeFileSync(tar, artifact.bytes);
  execFileSync('tar', ['-xzf', tar, '-C', directory]);
  return join(directory, 'package');
}
try {
  const cli = await published('cli', '0.14.10', expected.cli);
  const baseline13 = await published('sdk', '0.13.1', expected['0.13.1']);
  const baseline14 = await published('sdk', '0.14.0', expected['0.14.0']);
  const pack = JSON.parse(execFileSync('npm', ['pack', '--json', '--pack-destination', scratch], {cwd: workspace, encoding: 'utf8'}))[0];
  const candidateBytes = readFileSync(join(scratch, pack.filename));
  assert.equal(integrity(candidateBytes), pack.integrity);
  const candidate = {bytes: candidateBytes, version: pack.version, integrity: pack.integrity, sha256: sha256(candidateBytes)};
  const deps = join(scratch, 'cli-dependencies');
  mkdirSync(deps);
  writeFileSync(join(deps, 'package.json'), JSON.stringify({private: true, dependencies: {yaml: '^2.9.0', 'smol-toml': '^1.7.1', '@clack/prompts': '1.8.1'}}));
  execFileSync('npm', ['install', '--ignore-scripts', '--no-audit', '--no-fund'], {cwd: deps, stdio: 'pipe'});
  const results = [];
  for (const [variant, artifact, admitted] of [['published-0.13.1', baseline13, false], ['published-0.14.0', baseline14, false], ['candidate', candidate, true]]) {
    const variantRoot = join(scratch, variant);
    const sdkRoot = unpack(artifact, join(variantRoot, 'sdk'));
    symlinkSync(join(workspace, 'node_modules'), join(sdkRoot, 'node_modules'), 'dir');
    const cliRoot = unpack(cli, join(variantRoot, 'cli'));
    const modules = join(variantRoot, 'node_modules');
    mkdirSync(join(modules, '@catalyst-cloud'), {recursive: true});
    symlinkSync(sdkRoot, join(modules, '@catalyst-cloud/sdk'), 'dir');
    for (const dependency of ['yaml', 'smol-toml', '@clack']) symlinkSync(join(deps, 'node_modules', dependency), join(modules, dependency), 'dir');
    symlinkSync(modules, join(cliRoot, 'node_modules'), 'dir');
    assert.equal(JSON.parse(readFileSync(join(sdkRoot, 'package.json'))).version, artifact.version);
    const sdk = await import(pathToFileURL(join(sdkRoot, 'dist/index.js')).href);
    const config = await import(pathToFileURL(join(cliRoot, 'dist/config.js')).href);
    const {main} = await import(pathToFileURL(join(cliRoot, 'dist/cli.js')).href);
    const {loadContract} = await import(pathToFileURL(join(cliRoot, 'dist/contract.js')).href);
    const {observeCloudOnboarding} = await import(pathToFileURL(join(cliRoot, 'dist/onboard-ready.js')).href);
    for (const name of ['main-2a05ad4a', 'shipping-684ef744']) {
      const full = readFileSync(resolve('test/fixtures', name + '.json'));
      const doc = JSON.parse(full);
      const home = join(variantRoot, name); mkdirSync(home);
      const calls = [], out = [], err = [];
      const server = {id:'synthetic-server',name:'fixture-server',url:'https://mcp-fixture.invalid',auth:{kind:'none'},status:'ready'};
      const origin = 'https://published-boundary.test';
      const user = {id:'synthetic-person',role:'owner',label:'Fixture',email:null,linearUserId:'synthetic-linear'};
      const ctx = {...config.defaultCtx(), home, cwd: home, env: {}, stdout: text => out.push(text), stderr: text => err.push(text), fetch: async(input, init) => {
        const url = new URL(String(input)); assert.equal(url.origin, origin);
        assert.equal(init?.method ?? 'GET', 'GET', 'no production mutations');
        calls.push(url.pathname);
        if (url.pathname === '/api/v1/me') return Response.json({account:doc.account.id,slug:doc.account.slug,name:'Fixture',principal:'service',permissions:['mirror:read','mirror:write','mirror:feed'],user});
        if (url.pathname === '/api/v1/agent/contract') return Response.json(doc);
        if (url.pathname === '/api/v1/agent/portal-servers') return Response.json({outcome:'ok',servers:[server]});
        return Response.json({error:'fixture_read_unavailable'}, {status:404});
      }};
      const runnerDeps = {isTty: () => false, openBrowser: () => {throw new Error('browser prohibited');}};
      assert.equal(await main(['login','--key','ctc_user_synthetic_only','--base-url',origin,'--skills-dir',join(home,'skills')], ctx, runnerDeps), 0, JSON.stringify(err));
      assert.deepEqual((await loadContract(ctx, config.loadConfig(home), {refresh:true})).doc, doc);
      assert.equal(await main(['contract','--refresh','--json'], ctx, runnerDeps), 0);
      const observation = await observeCloudOnboarding(ctx, {teamIds: doc.teams.map(team => team.id)});
      assert.equal(await main(['onboard','--only','signin','--yes','--json'], ctx, runnerDeps), 0);
      const beforeReady = calls.length;
      const onboardExit = await main(['onboard','--only','ready','--yes','--json'], ctx, runnerDeps);
      assert.equal(onboardExit, 11, 'genuine starter-ticket proof absent');
      assert.ok(calls.slice(beforeReady).includes('/api/v1/agent/contract'));
      assert.equal(sdk.isTenantContract(doc), admitted);
      const sdkContract = await sdk.createTenantClient({baseUrl:origin,key:'ctc_user_synthetic_only',fetch:ctx.fetch}).contract();
      assert.equal(sdkContract.outcome, admitted ? 'ok' : 'shape');
      if (admitted) {
        assert.deepEqual(sdkContract.doc, doc);
        const known = doc.routes.find(route => route.method === 'PUT');
        assert.deepEqual(known, {method:'PUT',path:'/api/v1/agent/tenant/review-agents/write',takesWriteBudgetUnit:false,since:'2.8.0',idempotencyKeyField:null});
        const mutations = [
          {...known,path:'/api/v1/agent/custom'}, {...known,method:'DELETE'},
          {...known,method:'PATCH'}, {...known,method:'CUSTOM'},
          {...known,since:'2.9.0'}, {...known,takesWriteBudgetUnit:true},
          {...known,idempotencyKeyField:'requestId'},
          {method:'PUT',path:known.path,takesWriteBudgetUnit:false,since:'2.8.0'},
        ];
        for (const row of mutations) {
          const invalid = {...doc,routes:[...doc.routes,row]};
          assert.equal(sdk.isTenantContract(invalid), false, 'packed SDK must refuse unrecognized routes');
          const rejected = await sdk.createTenantClient({baseUrl:origin,key:'synthetic-only',fetch:async()=>Response.json(invalid)}).contract();
          assert.equal(rejected.outcome, 'shape');
        }
      }
      const beforeMcp = calls.length, beforeOut = out.length, beforeErr = err.length;
      const mcpExit = await main(['mcp','list','--json'], ctx, runnerDeps);
      if (mcpExit !== (admitted ? 0 : 2)) console.log(JSON.stringify({boundaryDiagnostic:variant,fixture:name,mcpExit,output:out.slice(beforeOut),errors:err.slice(beforeErr),sdkContractOutcome:sdkContract.outcome,requests:calls.slice(beforeMcp)}));
      assert.equal(mcpExit, admitted ? 0 : 2, 'actual published MCP dispatcher');
      assert.ok(calls.slice(beforeMcp).includes('/api/v1/agent/contract'));
      const mcpOutput = out.slice(beforeOut), mcpErrors = err.slice(beforeErr);
      if (admitted) {
        assert.deepEqual(mcpOutput.map(text=>JSON.parse(text)), [{outcome:'ok',status:200,servers:[server]}]);
        assert.deepEqual(calls.slice(beforeMcp), ['/api/v1/agent/contract','/api/v1/agent/portal-servers']);
        assert.equal(mcpErrors.some(line => /unexpected shape|does not read as a TenantContract/.test(line)), false);
      } else {
        assert.ok(mcpErrors.some(line => /does not read as a TenantContract/.test(line)));
        assert.deepEqual(calls.slice(beforeMcp), ['/api/v1/agent/contract']);
      }
      results.push({variant, fixture:name, fixtureSHA256:sha256(full), sdkWholeGuard:admitted?'PASS':'REFUSED', packedNegativeControls:admitted?8:null, sdkContractOutcome:sdkContract.outcome, mcpListExit:mcpExit, mcpOutput, mcpErrors, loadContract:'PASS_FULL_UNMODIFIED', onboardReadyExit:onboardExit, readinessWork:observation.work.state, requests:calls.map(path=>({method:'GET',path}))});
    }
  }
  const receipt = {result:'SDK_CONTRACT_PUBLISHED_BOUNDARY_PASS', cli:{version:cli.version,integrity:cli.integrity,sha256:cli.sha256}, sdkArtifacts:[baseline13,baseline14,candidate].map(({version,integrity,sha256})=>({version,integrity,sha256})), results, limitations:'Complete captured synthetic documents and isolated synthetic identity. Actual integrity-verified published CLI dispatcher and SDK artifact guards executed; candidate SDK deliberately substituted outside CLI semver range for compatibility evidence. No provider/tenant/browser/live operations. Ready remains waiting for real first-ticket proof. Candidate artifact is locally packed; this instrument does not publish it.'};
  if (process.env.SDK_BOUNDARY_ARTIFACT_DIR) {
    mkdirSync(process.env.SDK_BOUNDARY_ARTIFACT_DIR, {recursive:true});
    writeFileSync(join(process.env.SDK_BOUNDARY_ARTIFACT_DIR, 'sdk-candidate-'+candidate.version+'.tgz'), candidateBytes);
  }
  console.log(JSON.stringify(receipt));
  if (process.env.SDK_BOUNDARY_RECEIPT) writeFileSync(process.env.SDK_BOUNDARY_RECEIPT, JSON.stringify(receipt, null, 2)+'\n');
} finally {rmSync(scratch, {recursive:true,force:true});}
