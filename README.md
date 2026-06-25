# Dify → Langfuse Real-Time Tracing Research

Research into getting a self-hosted **Dify** LLM app to send its traces to a self-hosted **Langfuse** server in real-time (vs. batch processing), with the goal of minimizing trace transfer latency.

## TL;DR

Dify's current Langfuse integration is **structurally incompatible with "real-time."** It cannot be made low-latency by configuration alone. Three independent reasons stack:

1. **Dify uses the REST ingestion path** (`api.ingestion.batch()` → `POST /api/public/ingestion`), which on the Langfuse server incurs a **5 s queue delay**. The OTLP path gets **0 s**. Dify is on the wrong path.
2. **Dify adds its own 3-stage decoupling on top** — an in-process `threading.Timer` (default 5 s) → Celery queue → per-event synchronous HTTP. None of this is tunable to zero without code changes.
3. **Dify never sends `x-langfuse-ingestion-version: 4`** — and *cannot* via REST. That header is the only thing that routes to the "direct event write" path in the Langfuse worker. Without it, data goes through staging tables with **up to 10 min** of UI latency.

A widely-held misconception — that tuning `LANGFUSE_FLUSH_AT` / `LANGFUSE_FLUSH_INTERVAL` will help — **does not apply to Dify at all**. Those knobs govern the Langfuse SDK's OTel `BatchSpanProcessor`, which Dify does not use for its trace events (it calls the low-level REST client directly). Setting them changes nothing.

**Current floor (default config):** ~11 s + Celery wait + N×HTTP RTT.
**Achievable floor (OTLP + v4 header + tuned flush):** ~1.5 s.

---

## Repository structure

| File | Contents |
|---|---|
| [docs/01-research-report.md](docs/01-research-report.md) | Main synthesis: where latency lives, latency budget by approach, knobs that matter vs. don't, options to reduce latency, critical gotchas, evidence |
| [docs/02-dify-trace-flow.md](docs/02-dify-trace-flow.md) | Deep dive: Dify's complete trace emission flow (step-by-step with code quotes) + all 12 latency scenarios (A–L) |
| [docs/03-langfuse-staging-tables.md](docs/03-langfuse-staging-tables.md) | Langfuse server internals: the `observations_batch_staging` table, `handleEventPropagationJob`, and the v4 direct-write path |

## Evidence base

Investigated against:
- **Dify** `main` @ `b33e8f0ddb1189427548b0e1206cedcdc17d9bb6`
- **Langfuse server** @ `216d422635f5634bfaa8a295041f92c81a1c2aed`
- **Langfuse Python SDK** @ `7074373c74493f28a1f0eefad303499f0d06f9f8`

All claims are source-cited with permalinks in the docs.

## License

MIT — see [LICENSE](LICENSE).
