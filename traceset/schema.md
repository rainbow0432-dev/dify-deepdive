# Langfuse Wire Event Schema

## Event Envelope

```json
{
  "id": "<uuid v5>",
  "timestamp": "<ISO 8601 UTC>",
  "type": "trace-create" | "span-create" | "generation-create",
  "body": { ... }
}
```

## trace-create body

```json
{
  "id": "<trace_id>",
  "name": "<string>",
  "userId": "<string>",
  "input": "<any>",
  "output": "<any>",
  "sessionId": "<string>",
  "metadata": "<any>"
}
```

## span-create body

```json
{
  "id": "<span_id>",
  "traceId": "<trace_id>",
  "name": "<string>",
  "startTime": "<ISO 8601>",
  "endTime": "<ISO 8601>",
  "input": "<any>",
  "output": "<any>",
  "metadata": "<any>",
  "level": "DEBUG" | "DEFAULT" | "WARNING" | "ERROR",
  "statusMessage": "<string>",
  "parentObservationId": "<string>"
}
```

## generation-create body

Extends span-create with:

```json
{
  "completionStartTime": "<ISO 8601>",
  "model": "<string>",
  "modelParameters": { "<key>": "<value>" },
  "usageDetails": {
    "input": "<int>",
    "output": "<int>",
    "total": "<int>",
    "unit": "TOKENS",
    "inputCost": "<float>",
    "outputCost": "<float>",
    "totalCost": "<float>"
  }
}
```

## Serialization rules

- **camelCase**: all body field names are camelCase on the wire.
- **exclude_unset + exclude_none**: only fields with non-None values appear.

## Dify trace type to wire event mapping

| Dify TraceInfo type | Wire events | Handler |
|---|---|---|
| MessageTraceInfo | 1 trace-create + 1 generation-create | `LangFuseDataTrace.message_trace` |
| WorkflowTraceInfo | 1 trace-create + 1 span-create + K node events | `LangFuseDataTrace.workflow_trace` |
| ModerationTraceInfo | 1 span-create | `LangFuseDataTrace.moderation_trace` |
| DatasetRetrievalTraceInfo | 1 span-create | `LangFuseDataTrace.dataset_retrieval_trace` |
| ToolTraceInfo | 1 span-create per tool call | `LangFuseDataTrace.tool_trace` |
| GenerateNameTraceInfo | 1 trace-create + 1 span-create | `LangFuseDataTrace.generate_name_trace` |
| SuggestedQuestionTraceInfo | 1 generation-create | `LangFuseDataTrace.suggested_question_trace` |
