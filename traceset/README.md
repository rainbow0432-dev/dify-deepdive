# Dify App Trace Reference Catalog

A collection of 14 reference Dify app traces, captured as Langfuse wire events.

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

| # | Directory | App Type | Events | Edge? |
|---|---|---|---|---|
| 01 | `01-chat-basic` | chatbot | 4 |  |
| 02 | `02-chat-rag` | chatbot | 6 |  |
| 03 | `03-completion-basic` | completion | 4 |  |
| 04 | `04-agent-single-tool` | agent | 5 |  |
| 05 | `05-agent-multi-tool` | agent | 7 |  |
| 06 | `06-workflow-5node` | workflow | 7 |  |
| 07 | `07-workflow-15node` | workflow | 17 | high-n |
| 08 | `08-chatflow-basic` | chatflow | 11 |  |
| 09 | `09-moderation-blocked` | chatbot | 3 | moderation-blocked |
| 10 | `10-moderation-pass-through` | chatbot | 5 |  |
| 11 | `11-rag-empty-results` | chatbot | 5 | empty-rag |
| 12 | `12-tool-failure` | agent | 5 | tool-error |
| 13 | `13-suggested-questions-error` | chatbot | 5 | suggested-questions-error |
| 14 | `14-message-streaming` | chatbot | 4 | streaming |

**Total**: 88 events across 14 scenarios.

## Provenance

- Dify commit: `b33e8f0ddb1189427548b0e1206cedcdc17d9bb6`
- Langfuse SDK: `>=4.2.0,<5.0.0`
