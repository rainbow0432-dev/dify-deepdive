# Dify App Trace Reference Catalog

A collection of 13 reference Dify app traces, captured as Langfuse wire events.

## Structure

Each scenario directory (`NN-slug/`) contains:
- `events.jsonl` — wire events, one per line, in emission order
- `meta.json` — scenario metadata

Root files:
- `catalog.json` — machine-readable index of all scenarios
- `schema.md` — wire event field reference

## Wire event format

Each line in `events.jsonl` is a JSON object:
```json
{"id": "<uuid>", "timestamp": "<ISO8601>", "type": "trace-create|span-create|generation-create", "body": {...}}
```

See `schema.md` for the full field reference.

## Scenarios

| # | Directory | App Type | Events | Spans | Pattern |
|---|---|---|---|---|---|
| 01 | `01-linear-llm-chain` | workflow | 14 | 13 | linear |
| 02 | `02-parallel-branches` | workflow | 12 | 11 | parallel-fork-join |
| 03 | `03-agent-react-loop` | agent-chat | 12 | 11 | react-loop |
| 04 | `04-multi-tool-chain` | agent-chat | 12 | 10 | sequential-chain |
| 05 | `05-rag-multi-hop` | chat | 12 | 10 | multi-hop-retrieval |
| 06 | `06-moderation-rag-tool-combo` | chat | 11 | 10 | feature-combination |
| 07 | `07-workflow-conditional` | workflow | 12 | 11 | conditional-branch |
| 08 | `08-error-recovery-agent` | agent-chat | 12 | 10 | error-recovery |
| 09 | `09-nested-workflow` | advanced-chat | 14 | 12 | nested-workflow |
| 10 | `10-workflow-error-propagation` | workflow | 11 | 10 | error-propagation |
| 11 | `11-streaming-chatflow` | advanced-chat | 13 | 11 | streaming |
| 12 | `12-multi-model-pipeline` | workflow | 12 | 11 | multi-model |
| 13 | `13-completion-multi-feature` | completion | 12 | 10 | feature-combination |

**Total**: 159 events, 140 spans across 13 scenarios.

## Provenance

- Dify commit: `b33e8f0ddb1189427548b0e1206cedcdc17d9bb6`
- Langfuse SDK: `>=4.2.0,<5.0.0`
