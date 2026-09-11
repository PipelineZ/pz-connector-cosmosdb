# Pz.Connector.CosmosDb

Azure Cosmos DB (NoSQL API) source and sink for [PipelineZ](https://pipelinez.dev) (`pz`), served
out of process. A container reads as a **table**: the schema is declared or inferred from a
sample, documents stream through the query API page by page, and the engine's incremental
watermarks become a typed range on a cursor field. A sink output **appends** or **merges**
(upserts) by the document's own identity; `replace` is not supported (see below). NoSQL API only —
Mongo-API accounts use `pz-connector-mongodb`; Table, Gremlin and Cassandra APIs are out of scope.

## Installation

```yaml
# project.yml
connectors:
  - package: Pz.Connector.CosmosDb
    version: 0.1.0
```

`pz restore` installs the self-contained binary for your platform (linux-x64, linux-arm64,
osx-arm64, win-x64) and `pz run` spawns it. Needs **pz 0.6.1 or newer**. Built on
`Microsoft.Azure.Cosmos` 3.63.0.

The package ships **self-contained** (a full CoreCLR runtime per RID), not Native AOT: the SDK
deserializes its gateway query plan through Newtonsoft into an internal generic type Native AOT
never instantiates, so reads fail under AOT even with every assembly rooted (writes alone would
survive AOT, reads would not). Expect roughly **90 MB installed per platform**, versus a Native AOT
connector's tens of MB.

| RID | status |
|---|---|
| `linux-x64` | the platform every test and the packaging proof run on |
| `linux-arm64`, `osx-arm64`, `win-x64` | shipped, never exercised by this connector's own CI |

## Connection

```yaml
# connections.yml
cosmos:
  connector: cosmosdb
  database: shop                        # required
  auth: account_key                     # required; one of the five below
  endpoint: https://acct.documents.azure.com:443/   # required for every auth except connection_string
  account_key: ${COSMOS_KEY}            # auth: account_key
  connection_mode: direct               # optional; direct (default) | gateway
  consistency: session                  # optional; eventual | consistent_prefix | session | bounded_staleness | strong
  timeout: 30                           # optional; seconds, per-request timeout (1..600)
```

`auth` vocabulary and required fields mirror the first-party `azure` (blob) connector, so a project
with both declares them the same way:

| `auth` | required fields | credential |
|---|---|---|
| `connection_string` | `connection_string` | `AccountEndpoint=...;AccountKey=...;` |
| `account_key` | `endpoint`, `account_key` | account key |
| `service_principal` | `endpoint`, `tenant_id`, `client_id`, `client_secret` | Entra client secret |
| `credential_chain` | `endpoint` | `DefaultAzureCredential` |
| `managed_identity` | `endpoint` (+ optional `client_id`) | managed identity (user- or system-assigned) |

Required-field checks are offline and aggregate — one error naming every missing field, not
fail-one-at-a-time. An unknown `auth`, `connection_mode` or `consistency` value is refused naming
the allowed set.

`consistency` may only **lower** the account's default; asking for a stronger level than the
account provides is refused as a fatal `PZCS0101` naming both levels (the requested level and the
account's default). The vNext emulator's default consistency is **Eventual**, so a project tested
against it that later points at a real account with a stronger default should leave `consistency`
unset rather than assume Eventual works everywhere.

`connection_mode: gateway` is what the emulator supports (no direct TCP path); `direct` is the SDK
default and what production uses — this connector's own Docker-backed tests run gateway, and an
env-gated live suite proves direct against a real account.

Secrets: `account_key`, `client_secret` and `connection_string` are redacted from every error and
log line; the redactor also strips `AccountKey=<...>` fragments and the emulator's well-known key
wherever they appear in a message.

`pz connector check` runs `ReadAccountAsync` then a database read and reports
`Cosmos DB account <id>, consistency <level>`. A missing database is a fatal `PZCS0102` naming the
database.

## Reading a container

```yaml
  entities:
    orders:
      read:
        container: orders                    # optional; defaults to the entity name
        query: SELECT c.id, c.pk, c.total, c._ts FROM c WHERE c.status = 'shipped'
                                               # optional; default SELECT * FROM c
        fields:                               # optional; declared schema, skips inference
          id: string
          total: decimal
          placed_at: timestamp
          address.city: string
          items: json
        sample_size: 1000                     # optional; documents inspected for inference (1..100000)
        page_size: 1000                       # optional; items per query page (1..10000)
        partitions: auto                      # optional; auto (one per feed range) | an integer 1..N
```

**Declared schema.** `fields:` maps a JSON path (dotted for nested objects) to a type: `string`,
`int32`, `int64`, `double`, `decimal`, `bool`, `timestamp`, `date`, `json`. The columns are exactly
those, in that order.

**Inferred schema.** Without `fields:`, the first `sample_size` documents of `query` are inspected,
sampled in an order stable across the two callers that need to agree (the schema probe and the
dataset's first read): every scalar leaf becomes a column, nested objects flatten to dotted
columns, and arrays, mixed-type fields, and fields under a dynamic-key object land as `json` (the
raw JSON text). More than 2000 field paths is a refusal naming the ceiling. A document field whose
own name contains a dot is refused during sampling, naming the field and the document's `id` (it
would silently collide with the flattened path of the same name); under `fields:` a dotted key is
always a path, never a literal.

**Numbers.** JSON numbers arrive untyped — Cosmos stores every number as an IEEE double.
Inference: every sampled value integral and within ±2^53 → `int64`; any fractional value →
`double`; a mix of both → `double`. `int32` and `decimal` are declared-only types. Reading into a
declared column converts when lossless (int64 → double; a number into `int32` when it fits; a
number or numeric string into `decimal128(38,9)` when it round-trips exactly) and otherwise fails
the read naming the field and `id` — never silently narrowed or stringified.

**Other kinds.** `string` → utf8; `bool` → bool; a declared `timestamp` accepts an ISO-8601 string
(UTC, or an offset normalised to UTC) or an integral number of epoch seconds (so `_ts` can be
declared `timestamp`), landing as timestamp microseconds UTC; a declared `date` accepts
`yyyy-MM-dd`; `json` accepts anything as raw JSON text. Null and missing values carry no type and
do not count for inference.

**System properties.** `_rid`, `_self`, `_etag`, `_attachments` are excluded from inference and
from the default projection (declaring them under `fields:` as `string` still works). `_ts`
(int64 epoch seconds) is inferred as `int64`. `id` is always the trailing column, even when nothing
else wants it, so a refusal can always name the document.

The engine's own `columns:` read option (stamped from a dataset's `columns:` contract) is accepted
and ignored — the plan always comes from `fields:` or inference.

**Reads.** `partitions: auto` uses one pz partition per Cosmos feed range (the container's physical
partitions). An integer `N` below the feed-range count assigns ranges to `N` pz partitions
round-robin; `N` at or above the count behaves as `auto` (never an error). `page_size` sets the
per-page item count (`MaxItemCount`).

**Column pruning.** With the default query, the engine's wanted columns become
`SELECT c["a"], c["b"], c["id"] FROM c` — the distinct top-level segments of the wanted paths,
bracket-quoted so a reserved word or a dotted name works, `id` always included, with nested paths
still walked out of the returned document. A user-supplied `query:` is passed through untouched —
you own its projection; a column your query does not return reads as null. There is no native scan
tier and no SQL predicate pushdown beyond the incremental watermark bound below.

**Incremental reads.** Declare the cursor in SQL as for any pz source:

```sql
select * from {{ source('cosmos', 'orders') }}
where placed_at > {{ watermark('cosmos', 'orders') }}
```

The cursor column must be `int32`, `int64`, `double`, `timestamp` or `date`; `string`, `bool`,
`json` and `decimal` cursors are refused naming the column (`decimal`'s wire form is a string,
which would compare ordinally, not numerically). Bounds are applied by wrapping the query, never by
rewriting your `WHERE`: `SELECT * FROM (<query>) c WHERE c["<cursor>"] > @pz_lower AND
c["<cursor>"] <= @pz_upper` (`>=` on the lower bound when the engine's bound is inclusive; each
clause appears only when the engine supplies that bound). A `timestamp` or `date` cursor other than
`_ts` must be stored in exactly the string shape the connector writes
(`yyyy-MM-ddTHH:mm:ss.ffffffZ` / `yyyy-MM-dd`) — Cosmos compares strings ordinally, which is correct
for that fixed-width form and nothing else; a column stored as epoch seconds should be declared
`int64` and cursored as a number.

## Writing a container

```yaml
  entities:
    orders_out:
      write:
        container: orders_out                # optional; defaults to the entity name
        strategy: append | merge             # replace is refused (PZCS0301); use append or merge
        keys: [order_id, region]             # merge: the identity-forming columns (see Identity)
        id_from: [order_id, region]          # optional; columns whose values form `id` when the row has no `id` column
        concurrency: 32                      # optional; in-flight requests (1..256)
        rate_limit_retries: 9                # optional; SDK 429 backoff attempts (0..100) before the node fails transient
```

**Container contract.** The container must already exist — partition key and throughput are ops
decisions this connector does not make. A missing container is a fatal `PZCS0302` naming database
and container. The container's partition key paths (1–3; hierarchical keys are supported) are read
once per write session; every path must resolve to a column of the batch schema (dotted names
allowed), or the session is refused up front as `PZCS0303`, naming the missing path and the columns
that are present. A null in a partition-key column fails that row.

**Identity.** Cosmos identity is `(partition key, id)`, and every write targets that identity.

`id` resolves, in order:

1. the row's `id` column, if present — must be `string`, `int32` or `int64` (numbers render as
   invariant decimal text, since Cosmos `id` is always a string; a document written from an
   `int64` `id` reads back as a `string` `id` — this is documented behaviour, not a bug), non-empty,
   and free of `/`, `\`, `?`, `#`;
2. otherwise `id_from` — the named columns' values, rendered as text and **`|`-joined with no
   escaping**. A value containing `|`, `/`, `\`, `?` or `#` fails the row naming the column (`\`
   cannot appear in a Cosmos id at all, and `|` is the join separator, so neither can be escaped
   into a value); the composed id must be non-empty and at most 255 characters;
3. otherwise — **`append` only** — a fresh `Guid.NewGuid().ToString("N")`.

**merge** requires a resolvable `id` and a `keys` list that is exactly the id-forming columns:
`[id]` when the row supplies its own `id`, or exactly the `id_from` list (order-insensitive),
optionally with the partition-key columns appended. Any other `keys` is refused as `PZCS0304`,
whose message spells out the two accepted shapes — the engine's merge contract is "upsert on these
keys," and Cosmos can only upsert on its own `(partition key, id)` identity. A null in an
id-forming column fails the row.

**Row → document.** A dotted column name nests (`a.b` → `{"a":{"b":…}}`); a column that is both a
leaf and a prefix of another column is refused at `BeginWriteAsync`. A column name starting with
`_` is refused as a system property, except `_ts`, which is silently dropped (the server owns it).

Type spelling on write:

| pz type | Cosmos JSON |
|---|---|
| `int32` / `int64` | number; a magnitude over ±2^53 is refused as `PZCS0305` naming column and `id` — Cosmos stores doubles and would silently lose precision |
| `double` | number; `NaN`/±`Infinity` are refused (JSON has no spelling for them) |
| `decimal128` | **string** — lossless; declare `decimal` under a reader's `fields:` to get it back |
| `bool` | bool |
| `date32` | `yyyy-MM-dd` |
| `timestamp` | ISO-8601 UTC with microseconds |
| `string` | string |
| null (any type) | JSON `null` — the property is present, not omitted, so a `merge` can clear a previously-set value |

A serialised document over 2 MB is refused as `PZCS0306` naming `id`, before it is sent.

**Strategies.**

- **`append`**: `CreateItemStreamAsync` per row, bulk-executed, at most `concurrency` requests in
  flight (the SDK batches them per partition internally). A 409 on a user-supplied `id` fails that
  row non-transient — append is at-least-once by contract, and a retried run re-sending a row that
  already landed is exactly what a conflict reports. Generated ids never conflict.
- **`merge`**: `UpsertItemStreamAsync` per row, same bulk/concurrency shape. The last row wins for a
  duplicate identity within one write session. Effectively-once by construction.
- **`replace` is refused** at `BeginWriteAsync` as `PZCS0301`: Cosmos has no container rename and no
  atomic truncate, so there is no staging-and-swap to build on — the message names `append` and
  `merge` as the alternatives.
- An aborted append or merge cannot unsend the requests that already landed (`AbortSemantics.BestEffort`).

**Rate limiting.** The SDK's built-in 429 backoff is flow control, not a retry loop, and stays on,
bounded by `rate_limit_retries` (`MaxRetryAttemptsOnRateLimitedRequests`) and a 30 s max wait per
attempt. An exhausted budget surfaces as a transient error carrying the response's
`x-ms-retry-after-ms` as `RetryAfter`, so the engine's own retry policy owns the outer decision. The
SDK's other internal retries (region failover, connection re-establishment) are left at their
defaults.

## Errors and retry classification

`CosmosException` is classified by HTTP status (with the SDK's sub-status folded into the message):

| status | class | note |
|---|---|---|
| 429, 449, 503, 408 | transient, `RetryAfter` from the response header | throughput / retry-with / unavailable / timeout |
| 410 | transient | partition split or migration; feed ranges are re-read on the next attempt |
| 401, 403 | fatal, names `auth` | key or token rejected / RBAC denial |
| 404 on database or container | fatal, names the entity (`PZCS0102` / `PZCS0302`) | a 404 on an item never surfaces — this connector never does point reads |
| 400 | fatal, server message passed through | query syntax errors land here |
| 409 | fatal, names `id` | append conflict |
| 413 | fatal, `PZCS0306` | document too large (the server-side guard behind the client-side 2 MB check) |
| anything else | fatal, unmapped | |

`CosmosOperationCanceledException` (SDK-level timeout), `HttpRequestException`, `SocketException`
and `IOException` on the transport are all transient. Every error message passes through the
redactor before it reaches a log or an artifact.

## Limits and non-goals (v1)

Change feed / CDC; `replace` writes; container creation or throughput management;
transactional-batch grouping (bulk mode already covers throughput); stored procedures, triggers,
UDFs; Mongo, Table, Gremlin and Cassandra APIs; predicate pushdown from `ReadHints` beyond
projection; the analytical store / Synapse Link; point reads; vector and geospatial types (they
land as `json`); session-token continuity across nodes (each node reads with its own client
session — a batch ETL never needs to read back its own writes mid-run).

## Testing

**Emulator (Docker, no credentials).** The connector's own suite runs against
`mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-latest` (`PROTOCOL=http`, port 8081),
`connection_mode: gateway`, `auth: account_key` with the emulator's well-known key. Facts that need
it `SkippableFact`-skip cleanly without Docker. Not provable on the emulator (single feed range,
gateway only, no hierarchical partition keys in the acceptance containers): multi-partition
parallel reads, `partitions: N` folding across more than one feed range, `direct` connection mode,
end-to-end hierarchical partition-key extraction, and 410 handling.

**Live suite.** Those gaps are covered by an env-gated live suite, run on demand and never in CI:
set `PZ_COSMOS_ENDPOINT` and `PZ_COSMOS_KEY` against a real (or dev) Cosmos DB account; it creates a
throwaway database per run and drops it in teardown.

## Development

```bash
dotnet build Pz.Connector.CosmosDb.slnx -c Release
dotnet test Pz.Connector.CosmosDb.slnx -c Release --no-build        # emulator facts need docker; they SKIP without it
dotnet restore src/Pz.Connector.CosmosDb -r linux-x64               # once, on a cold cache
dotnet publish src/Pz.Connector.CosmosDb -c Release -r linux-x64 --no-restore
dotnet pack src/Pz.Connector.CosmosDb -c Release -o packages        # nupkg with pz.connector.json
```

`tests/e2e/` is the pz project CI packs and runs a connector against, backed by the same emulator
image. Releases are tag-triggered (`v*`) and publish to nuget.org through trusted publishing.
