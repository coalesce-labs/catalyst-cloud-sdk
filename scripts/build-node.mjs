// Bundle source-published SQL packages for Node, retaining the core transport module identity.
import { build } from 'esbuild';
import { dirname, resolve, basename } from 'node:path';
const sourceRoot = resolve('src');
await build({
  entryPoints: ['src/node.ts'],
  outfile: 'dist/node.js',
  bundle: true,
  platform: 'node',
  format: 'esm',
  target: 'node22',
  external: ['bun:sqlite', 'better-sqlite3', '@opentelemetry/api'],
  plugins: [{
    name: 'shared-core-modules',
    setup(builder) {
      builder.onResolve({filter: /\.js$/}, args => {
        if (!args.importer || !args.path.startsWith('.')) return;
        const target = resolve(dirname(args.importer), args.path);
        // Files directly under src are already emitted beside node.js by tsc.
        // Both root and replica imports must use that same live/otel/types module.
        if (dirname(target) === sourceRoot) return {path: './' + basename(target), external: true};
      });
    },
  }],
});
