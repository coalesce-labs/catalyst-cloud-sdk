# Optional browser replica

Install this module only when you need local SQL. The default `@catalyst-cloud/sdk` reads the cloud and receives live updates without SQLite. This module installs the schema, shared queries and replication packages. Install the SQLite WASM peer in the browser app.

```ts
import { BrowserReplica } from "@catalyst-cloud/sdk-replica-browser";
```

An optional native cache shares the supervised tenant daemon. Readers use `openReadOnly`; they never start a second writer. No offline-browsing promise.
