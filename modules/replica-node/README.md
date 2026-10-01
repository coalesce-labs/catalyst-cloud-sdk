# Optional node replica

Install this module only when you need local SQL. The default `@catalyst-cloud/sdk` reads the cloud and receives live updates without SQLite. This module installs the schema, shared queries and replication packages. Bun and Node 22 provide built-in SQLite. Install better-sqlite3 only when you choose that injected driver.

```ts
import { CatalystReplica } from "@catalyst-cloud/sdk-replica-node";
```

An optional native cache shares the supervised tenant daemon. Readers use `openReadOnly`; they never start a second writer. No offline-browsing promise.
