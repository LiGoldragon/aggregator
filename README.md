# aggregator

Runtime component for collecting and normalizing recent work evidence from configured transcript and repository sources. Review and synthesis remain agent work outside this binary.

## Examples

- `examples/collect.nota` is the coarse evidence collection request.
- `examples/configuration.nota` shows the current meta configuration shape, including configured source roots, the daemon-local fragile output index policy, and read/preview/page limits. Replace the sample `/srv/aggregator/...` paths with local readable roots before validation.
- `examples/output-interface-requests.nota` is a signal/client sequence for metadata-first discovery followed by explicit bounded output reads. Submit one NOTA form at a time, replacing each `fragile-*` placeholder with the opaque reference returned by the previous listing.
- `examples/output-interface-replies.nota` shows schema-faithful reply and rejection shapes a UI or agent should handle.

After substituting real references from earlier replies, run one request per CLI invocation:

```sh
while IFS= read -r request; do
  [ -z "$request" ] && continue
  cargo run --bin aggregator -- --configuration /path/to/configuration.nota --request "$request"
done < examples/output-interface-requests.nota
```

## Metadata-first bounded output workflow

A UI or agent should discover cards before reading text:

1. `ListSessions` with source and time filters, a page limit, and a deterministic order.
2. `ListSubagents` for a selected session when a subagent drill-down is needed.
3. `ListOutputs` with session/subagent/authorship filters and `MetadataOnly` or a small `BoundedPreview`.
4. `ListOutputSegments` for a selected output when byte or line ranges are easier to choose from segment cards.
5. `EstimateOutput` for the chosen range.
6. `ReadOutput` only with an explicit `maximum_bytes` no larger than the configured read cap.

Fragile references are daemon-local opaque sidecar entries. They are durable enough to carry between calls, but the daemon may reject them as stale or broken when backing transcript or artifact files change. Agent-authored output cards are artifact provenance, not psyche-authorized design authority.

Byte ranges are half-open zero-based `[start, end)` intervals. Line ranges are half-open one-based `[start, end)` intervals. `OldestFirst` and `NewestFirst` are deterministic and break ties by fragile reference; cursors are bound to the collection, filters, order, page limit, canonical query material, item count, and full sorted filtered reference list.

Handle rejections by returning to metadata discovery instead of fetching more text: drop stale cursors or references and relist, mark broken references unavailable, lower page/preview/read caps after `Oversized`, and repair reversed or out-of-bounds half-open ranges after `InvalidRange`.

Optional read-only legacy recovery roots can be configured and validated, but they are not the primary example path and should not be treated as live authoritative sources.
