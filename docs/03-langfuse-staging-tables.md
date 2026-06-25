# Langfuse Staging Tables & the v4 Direct-Write Path

Explains what the "staging tables" are in the context of *"Data goes through staging tables with up to 10 min of UI latency"* — the Langfuse server's ClickHouse write-path internals.

Verified against Langfuse server @ `216d422635f5634bfaa8a295041f92c81a1c2aed`.

---

## The staging table: `observations_batch_staging`

It's a **ClickHouse table** in the Langfuse worker's database, used by the **legacy/dual-write path** (i.e. any ingestion that does *not* carry the `x-langfuse-ingestion-version: 4` signal — which includes all REST ingestion and OTLP from old SDKs / raw collectors without the header).

### Key properties

- **Partitioned into 3-minute windows** — rows land in the partition corresponding to their event timestamp.
- The worker writes incoming events here *plus* to the legacy `observations` tables simultaneously (hence "dual write").
- A periodic **`handleEventPropagationJob`** (a BullMQ cron job in the worker container) scans completed 3-minute partitions and **merges/propagates them into the final `events` table** that the v4 UI reads from.

### Why "up to 10 min" UI latency

- You wait for the current 3-minute partition to close (up to ~3 min),
- then for the propagation job's next run cadence (it doesn't run continuously),
- plus S3 download + processing + the ClickHouse writer's own 1 s batch.
- Langfuse's official v4 docs round this to **"up to 10 minutes"** as the worst-case bound for non-v4-header data appearing in the Fast Preview UI. ([source: langfuse.com/docs/v4](https://langfuse.com/docs/v4))

---

## The v4 direct path skips staging entirely

When the worker detects v4 lineage (Python SDK ≥ 4.0 / JS SDK ≥ 5.0 via `x-langfuse-sdk-name`+`version`, OR an explicit `x-langfuse-ingestion-version: 4` header on OTLP), it routes to `ingestionService.writeEventRecord()` which writes straight to the **`events` / `events_full` table** — no staging, no propagation job. That cuts the floor to ~1 s (just the ClickhouseWriter batch).

```
Non-v4 (REST, old OTLP):  events → observations_batch_staging (3-min partition)
                                          ↓ handleEventPropagationJob (periodic)
                                     events table  →  UI   (~up to 10 min)

v4 direct (header/SDK):   events → events_full table  →  UI   (~1 s)
```

The write-path decision happens in [`worker/src/queues/otelIngestionQueue.ts:358-459`](https://github.com/langfuse/langfuse/blob/216d422635f5634bfaa8a295041f92c81a1c2aed/worker/src/queues/otelIngestionQueue.ts#L358-L459):

| Path | Trigger | Behavior | Latency |
|---|---|---|---|
| **`direct_header`** | `x-langfuse-ingestion-version: 4` header, OR Python SDK ≥ 4.0.0, OR JS SDK ≥ 5.0.0 | `writeEventRecord()` → ClickHouse `events_full` table directly | **~1s** |
| `direct_env` | `LANGFUSE_MIGRATION_V4_NATIVE_OTEL_BEHAVIOUR=direct` (deployment override) | Same as above | ~1s |
| `direct_scope` | Legacy fallback: scope name contains "langfuse" + sdk-experiment env + Python ≥ 3.9.0 / JS ≥ 4.4.0 | Same as above | ~1s |
| `dual` | None of the above (e.g. raw OTel collector without ingestion-version header) | Writes to `observations_batch_staging` (3-min partition) **+** legacy tables; periodic `handleEventPropagationJob` moves staging → events | **Up to ~10 minutes** |

```typescript
// otelIngestionQueue.ts L415-420 — V4 events_only mode skips legacy writes entirely
const skipLegacyWrites = !v4WritesToLegacyTables(env);
```

---

## Why this matters for the Dify case

Dify's native integration is doubly stuck on the slow path:

1. It uses **REST** (`/api/public/ingestion`) — REST has no `x-langfuse-ingestion-version` mechanism at all, so it *cannot* trigger the v4 direct-write path regardless of headers.
2. Even if Dify switched to OTLP, it would need to emit the `x-langfuse-ingestion-version: 4` header (or rely on SDK name+version auto-detection, which requires using the SDK's OTel span path — which Dify doesn't).

So Dify's traces currently flow through `observations_batch_staging` → propagation job → `events`, which is the structural source of the multi-minute UI latency on top of the transfer latency.

---

## Server-side queue delay (the other REST penalty)

Independent of staging: the REST ingestion endpoint incurs a server-side queue delay that OTLP does not. From [`packages/shared/src/server/ingestion/processEventBatch.ts:66-91`](https://github.com/langfuse/langfuse/blob/216d422635f5634bfaa8a295041f92c81a1c2aed/packages/shared/src/server/ingestion/processEventBatch.ts#L66-L91):

```typescript
const getDelay = (delay, source) => {
  if (delay !== null) return delay;            // explicit override wins
  const now = new Date();
  // Around UTC midnight: use full LANGFUSE_INGESTION_QUEUE_DELAY_MS to avoid out-of-order dups
  if ((hours === 23 && minutes >= 45) || (hours === 0 && minutes <= 15))
    return env.LANGFUSE_INGESTION_QUEUE_DELAY_MS;
  if (source === "otel") return 0;             // ← OTLP path: ZERO delay
  return Math.min(5000, env.LANGFUSE_INGESTION_QUEUE_DELAY_MS);  // REST: min(5s, env)
};
```

| Source | Queue delay |
|---|---|
| REST (`/api/public/ingestion`) | `min(5000, LANGFUSE_INGESTION_QUEUE_DELAY_MS)` — **5 s** with default config |
| OTLP (`/api/public/otel/v1/traces`) | **0 s** (hardcoded) |

**Net: OTLP path has a structural 5-second latency advantage over REST** when both run on default config. This is independent of any SDK tuning.

---

## Tunable server-side knobs (self-hosted Langfuse)

| Env var | Default | Effect |
|---|---|---|
| `LANGFUSE_INGESTION_QUEUE_DELAY_MS` | 15000 | REST queue delay (capped at 5s in code). OTLP is always 0 |
| `LANGFUSE_INGESTION_QUEUE_SHARD_COUNT` | 1 | REST queue sharding — increase for parallelism |
| `LANGFUSE_OTEL_INGESTION_QUEUE_SHARD_COUNT` | 1 | OTLP queue sharding |
| `LANGFUSE_INGESTION_QUEUE_PROCESSING_CONCURRENCY` | 20 | REST worker concurrency |
| `LANGFUSE_OTEL_INGESTION_QUEUE_PROCESSING_CONCURRENCY` | 20 | OTLP worker concurrency |
| `LANGFUSE_INGESTION_CLICKHOUSE_WRITE_BATCH_SIZE` | 1000 | ClickHouse INSERT batch size |
| `LANGFUSE_INGESTION_CLICKHOUSE_WRITE_INTERVAL_MS` | 1000 | ClickHouse flush interval — **drop to 250–500ms for lower latency** |
| `LANGFUSE_OTEL_MAX_SPAN_BYTES` | 9.5 MB | Per-span size warning threshold |
| `LANGFUSE_MIGRATION_V4_NATIVE_OTEL_BEHAVIOUR` | unset | Set to `direct` to force direct event writes for ALL OTLP batches regardless of SDK headers |
| `LANGFUSE_MIGRATION_V4_WRITE_MODE` | unset | `events_only` rejects trace/observation events at REST endpoint (only allows scores/logs) |
