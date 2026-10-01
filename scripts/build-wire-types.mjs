// The root HTTP declarations must work without the optional SQL packages installed.
// Generate from the pinned shared types rather than maintaining a second set of view interfaces.
import { readdirSync, mkdirSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
const source = dirname(fileURLToPath(import.meta.resolve('@catalyst-cloud/read-model')));
mkdirSync('.types-cache/read-model', { recursive: true });
const files = readdirSync(source).filter(file => file.endsWith('.ts')).map(file => join(source, file));
execFileSync('node_modules/.bin/tsc', ['--declaration','--emitDeclarationOnly','--module','ES2022','--moduleResolution','Bundler','--target','ES2022','--skipLibCheck','--strict','--outDir','.types-cache/read-model', ...files], {stdio:'inherit'});
for (const [entry, output] of [['tenant-client','tenant-client']]) {
  execFileSync('node_modules/.bin/dts-bundle-generator', ['--no-check','--project','tsconfig.bundle.json','--external-inlines','@catalyst-cloud/read-model','--out-file',`dist/${output}.d.ts`,`src/${entry}.ts`], {stdio:'inherit'});
}
