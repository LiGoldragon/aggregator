# aggregator

Runtime component for collecting and normalizing recent work evidence from configured transcript and repository sources. Review and synthesis remain agent work outside this binary.

## Text and wire

Every query a person writes and every response printed is Datom text over the
generated `signal-aggregator` and `meta-signal-aggregator` contracts. Between
the CLI and the daemon each carries as one `signal` frame: a four-byte
big-endian length prefix and the rkyv archive of one `Query` or one `Response`.
Nothing sits above the archive: no exchange identifiers, no lanes, no
sub-replies.

## Examples

- `examples/collect.datom` is the coarse evidence collection query.
- `examples/configuration.datom` is the current configuration shape, including configured source roots, the daemon-local fragile output index policy, and read/preview/page limits. Replace the sample `/srv/aggregator/...` paths with local readable roots before validation.
- `examples/transcript-block-search.datom` is a transcript block search carrying its text query as a flat node arena.
- `examples/transcript-block-read.datom` is a bounded transcript block read response.

One query per CLI invocation:

```sh
cargo run --bin aggregator -- \
  --configuration /path/to/configuration.datom \
  --request "$(cat examples/collect.datom)"
```

Replace each `fragile-*` placeholder with the opaque reference an earlier
response returned.

## Metadata-first bounded output workflow

A UI or agent should discover cards before reading text:

1. `ListSessions` with source and time filters, a page limit, and a deterministic order.
2. `ListSubagents` for a selected session when a subagent drill-down is needed.
3. `ListOutputs` with session/subagent/authorship filters and `MetadataOnly` or a small `BoundedPreview`.
4. `ListOutputSegments` for a selected output when byte or line ranges are easier to choose from segment cards.
5. `EstimateOutput` for the chosen range.
6. `ReadOutput` only with an explicit `maximum_bytes` no larger than the configured read cap.

Fragile references are daemon-local opaque sidecar entries. They are durable enough to carry between calls, but the daemon may reject them as stale or broken when backing runtime evidence changes. Agent-authored output cards are provenance and evidence, not psyche-authorized design authority.

Byte ranges are half-open zero-based `[start, end)` intervals. Line ranges are half-open one-based `[start, end)` intervals. `OldestFirst` and `NewestFirst` are deterministic and break ties by fragile reference; cursors are bound to the collection, filters, order, page limit, canonical query material, item count, and full sorted filtered reference list.

Handle rejections by returning to metadata discovery instead of fetching more text: drop stale cursors or references and relist, mark broken references unavailable, lower page/preview/read caps after `Oversized`, and repair reversed or out-of-bounds half-open ranges after `InvalidRange`.

Optional read-only legacy recovery roots are for one-time migration or recovery only; do not treat `reports/` or `agent-outputs/` as live authoritative sources or a recommended workflow.

## Local transcript block search workflow

Transcript block search is for scraping configured local harness, session, and subagent transcript roots. It is not a harness integration API and is not a reason to create or keep markdown report files alive. Configure local readable Claude JSONL, Claude subagent `.output` JSONL, Codex session, or Pi run-history roots, then call `ObserveHealth` and discover cards before reading text:

1. `ObserveHealth` to see configured source/index status (`ReadableEmpty`, `ReadableIndexed`, `UnreadableRoot`, `DiscoveryTruncated`, `MalformedRecords`, or `IndexStoreUnreadable`).
2. `ListSessions` with source and time filters.
2. `ListSubagents` for the selected session when subagent drill-down matters.
3. `ListTranscriptBlocks` with `MetadataOnly` or a small `BoundedPreview` and a grounded kind filter.
4. `SearchTranscriptBlocks` with a flat text-query arena: a vector of `TextQueryNode` values and the index of its root. A node is `Contains.Word.quota`, `Contains.Phrase.{ [ rate limit ] }`, `Near.{ Word.quota Word.reset 6 }`, or an `AllOf`, `AnyOf`, or `Not` naming its children by index into the same vector. An index outside the vector, or a node that reaches itself, is rejected as `InvalidQuery`.
5. `EstimateTranscriptBlock` for the selected fragile block reference.
6. `ReadTranscriptBlock` only with an explicit `maximum_bytes`; there is no unbounded whole-block text fetch.

The `TranscriptBlockKind` vocabulary is: `UserPrompt`, `AgentResponse`, `ToolCall`, `ToolResult`, `Inference`, `SystemInstruction`, `Attachment`, `SessionEvent`, and `Unclassified`. Do not invent a generic final-response classification; use `AgentResponse` only when the source mapping grounds it. Agent-authored text returned by these calls is evidence and provenance, not authority.

For quota or usage recovery, search terms like rate, usage, quota, limit, reset, or cooldown; filter by session, subagent, and kind; then read only the matching blocks with a bounded `maximum_bytes`. If a cursor or block reference is stale, relist and search again instead of broadening the read.

Current adapter caveats are visible in the kind vocabulary: some additional Codex payload categories can fall back to `SessionEvent`, and Codex developer-role messages may currently be `Unclassified`.
