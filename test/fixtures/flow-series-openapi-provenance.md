# Flow series OpenAPI fixture

`flow-series-openapi.json` is a subset of the cloud's public OpenAPI document: the `/api/v1/flow-series` operation, the `FlowSeries` schema it answers, and the two security schemes the operation names. Every value is copied unchanged. `test/tenant-client-flow.test.ts` checks the SDK's `FLOW_PERIODS`, `FLOW_GRANULARITIES` and the Flow field names it declares against it (CTC-6367). The document describes the API only; it contains no credentials or customer data.

| Field | Value |
| --- | --- |
| Source URL | `https://api.catalyst-cloud.coalescelabs.ai/api/v1/openapi.json` |
| `info.version` | `0.46.0` |
| Retrieved (UTC) | `2026-10-22T13:07:52Z` |
| Served document SHA256 | `d9807571b208e87ef33f072c40bbd142a14df16702ac65e2facef8b1e1c1f6e7` |
| Fixture SHA256 | `5b3e29dbc42a862310ed7c63cb773635a022f2133cab208185c70ccf3c24997c` |

The staging document (`https://staging.catalystcloud.dev/api/v1/openapi.json`) had the same SHA256 at the same time.

To refresh it, run this from the repository root, then make the SDK's Flow constants and types match whatever the test reports:

```bash
curl -sS https://api.catalyst-cloud.coalescelabs.ai/api/v1/openapi.json -o /tmp/openapi.json
sha256sum /tmp/openapi.json
node -e '
const d=require("/tmp/openapi.json");
const out={openapi:d.openapi, info:{title:d.info.title, version:d.info.version},
  paths:{"/api/v1/flow-series":d.paths["/api/v1/flow-series"]},
  components:{schemas:{FlowSeries:d.components.schemas.FlowSeries},
    securitySchemes:{bearerKey:d.components.securitySchemes.bearerKey, sessionCookie:d.components.securitySchemes.sessionCookie}}};
require("fs").writeFileSync("test/fixtures/flow-series-openapi.json", JSON.stringify(out,null,2)+"\n");'
sha256sum test/fixtures/flow-series-openapi.json
```
