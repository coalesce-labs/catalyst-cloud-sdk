// CTC-599: the test suite runs on Node, so its typecheck needs @types/node.
//
// The root tsconfig sets `"types": []` so the PUBLISHED package (tsconfig.build.json, src only)
// never inherits an ambient Node dependency; that stays true because this file lives under test/.
// Until vitest 4, vitest's own declarations pulled Node's types into `bun run typecheck` by
// accident. vitest 4 / vite 8 no longer do, so the tests declare the dependency themselves.
/// <reference types="node" />
