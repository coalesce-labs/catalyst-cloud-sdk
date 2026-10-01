# Optional browser replica

Install this module only when you need local SQL. The default `@catalyst-cloud/sdk` reads the cloud and receives live updates without SQLite. This module installs the schema, shared queries and replication packages. Install the SQLite WASM peer in the browser app.

```ts
import { BrowserReplica } from "@catalyst-cloud/sdk-replica-browser";
```

The browser replica uses the SDK worker and origin lock for its lifecycle. This is an explicit opt-in; it is not the default web read path and makes no offline-browsing promise. A native desktop cache uses the Node module and the supervised tenant daemon instead.
