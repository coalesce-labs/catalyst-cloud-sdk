// Exercise the published artifact in a consumer with no replica dependencies.
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { execFileSync } from 'node:child_process';
const consumer = mkdtempSync(join(tmpdir(), 'catalyst-sdk-pack-'));
const root = process.cwd();
try {
  const pack = JSON.parse(execFileSync('npm', ['pack','--json','--pack-destination',consumer], {encoding:'utf8'}))[0];
  writeFileSync(join(consumer,'package.json'), JSON.stringify({private:true,type:'module'}));
  execFileSync('npm',['install','--ignore-scripts','--no-audit','--no-fund',join(consumer,pack.filename)], {cwd:consumer,stdio:'pipe'});
  for (const name of ['schema','replicate','read-model']) assert.equal(existsSync(join(consumer,'node_modules/@catalyst-cloud',name)),false,`default installed ${name}`);
  for (const name of ['better-sqlite3','@sqlite.org/sqlite-wasm']) assert.equal(existsSync(join(consumer,'node_modules',name)),false,`default installed ${name}`);
  execFileSync('node',['--input-type=module','-e','const sdk = await import("@catalyst-cloud/sdk"); const http = await import("@catalyst-cloud/sdk/http"); const live = await import("@catalyst-cloud/sdk/live"); if (typeof sdk.createTenantClient !== "function" || typeof http.createTenantClient !== "function" || typeof live.LiveSyncClient !== "function") process.exit(1);'],{cwd:consumer,stdio:'inherit'});
  writeFileSync(join(consumer,'index.ts'),'import {createTenantClient, type IssueGetResult} from "@catalyst-cloud/sdk"; import {LiveSyncClient} from "@catalyst-cloud/sdk/live"; const client = createTenantClient({baseUrl:"https://example.invalid",key:"test"}); const read: Promise<IssueGetResult> = client.issues.get("CTC-1"); void read; void LiveSyncClient;\n');
  writeFileSync(join(consumer,'tsconfig.json'), JSON.stringify({compilerOptions:{noEmit:true,strict:true,module:'ES2022',moduleResolution:'Bundler',target:'ES2022',types:[]},files:['index.ts']}));
  execFileSync(resolve('node_modules/.bin/tsc'),['-p',join(consumer,'tsconfig.json')],{cwd:consumer,stdio:'inherit'});
  // Positive control: the explicit native module installs its SQL implementation.
  const modulePack = JSON.parse(execFileSync('npm',['pack','--json','--pack-destination',consumer],{cwd:join(root,'modules/replica-node'),encoding:'utf8'}))[0];
  execFileSync('npm',['install','--ignore-scripts','--no-audit','--no-fund',join(consumer,modulePack.filename)],{cwd:consumer,stdio:'pipe'});
  assert.equal(existsSync(join(consumer,'node_modules/@catalyst-cloud/replicate')),true,'optional module did not install replication');
  // Skip third-party SQL declaration checks; the live-class assignment is still checked.
  writeFileSync(join(consumer,'identity.ts'),'import {LiveSyncClient as CoreClient} from "@catalyst-cloud/sdk"; import {LiveSyncClient as NativeClient} from "@catalyst-cloud/sdk-replica-node"; const core: typeof CoreClient = NativeClient; void core;\n');
  writeFileSync(join(consumer,'tsconfig.json'), JSON.stringify({compilerOptions:{noEmit:true,strict:true,module:'ES2022',moduleResolution:'Bundler',target:'ES2022',types:[],skipLibCheck:true},files:['index.ts','identity.ts']}));
  execFileSync(resolve('node_modules/.bin/tsc'),['-p',join(consumer,'tsconfig.json')],{cwd:consumer,stdio:'inherit'});
  execFileSync('node',['--input-type=module','-e','const {CatalystReplica} = await import("@catalyst-cloud/sdk-replica-node"); if (typeof CatalystReplica.openReadOnly !== "function") process.exit(1); const core = await import("@catalyst-cloud/sdk"); const native = await import("@catalyst-cloud/sdk-replica-node"); if (core.AuthError !== native.AuthError || core.LiveSyncClient !== native.LiveSyncClient) throw new Error("Native entry duplicated live classes");'],{cwd:consumer,stdio:'inherit'});
  const browserPack = JSON.parse(execFileSync('npm',['pack','--json','--pack-destination',consumer],{cwd:join(root,'modules/replica-browser'),encoding:'utf8'}))[0];
  execFileSync('npm',['install','--ignore-scripts','--no-audit','--no-fund',join(consumer,browserPack.filename)],{cwd:consumer,stdio:'pipe'});
  assert.equal(existsSync(join(consumer,'node_modules/@sqlite.org/sqlite-wasm')),true,'browser module did not resolve driver peer');
  writeFileSync(join(consumer,'browser.ts'),'import {BrowserReplica} from "@catalyst-cloud/sdk-replica-browser"; console.log(BrowserReplica);\n');
  execFileSync(resolve('node_modules/.bin/esbuild'),[join(consumer,'browser.ts'),'--bundle','--platform=browser','--format=esm','--external:@opentelemetry/api',`--outfile=${join(consumer,'browser.js')}`],{cwd:consumer,stdio:'pipe'});
  writeFileSync(join(consumer,'worker.ts'),'import "@catalyst-cloud/sdk-replica-browser/db-worker";\n');
  execFileSync(resolve('node_modules/.bin/esbuild'),[join(consumer,'worker.ts'),'--bundle','--platform=browser','--format=esm','--external:@sqlite.org/sqlite-wasm','--external:@opentelemetry/api',`--outfile=${join(consumer,'worker.js')}`],{cwd:consumer,stdio:'pipe'});
  console.log(JSON.stringify({corePackedBytes:pack.size,coreUnpackedBytes:pack.unpackedSize,nodeModuleBytes:modulePack.size,browserModuleBytes:browserPack.size,defaultSqlDependencies:0,consumerTypes:'passed',optionalModules:'passed',browserAndWorkerBundles:'passed'}));
} finally { rmSync(consumer,{recursive:true,force:true}); }
