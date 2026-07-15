# aggregator — architecture

`aggregator` is a typed observation and projection layer over configured work
evidence. It exists to make harness, session, subagent transcript, and event
evidence navigable without asking agents to write routine handoff reports.
`reports/` and `agent-outputs/` are not normal source roots, and the component
is not a markdown report archive.

## Role and authority

The component owns runtime observation, indexing, and bounded projection only.
It returns evidence packages, metadata cards, fragile references, size facts,
bounded text excerpts, truncation facts, and typed read or reference failures.
It does not produce summaries, reviews, recommendations, scores, or judgments.

Agent output observed through this component is provenance and evidence, not
authority. Accepted decisions still land in their owning durable surfaces: code,
schema, architecture docs, README content, tracker items, Spirit records, or
other project-specific state.

Routine "write a report so another agent can read it" handoff should disappear.
The pickup object is an aggregator reference plus an explicit bounded read, not
a filesystem path to a newly authored markdown artifact.

## Runtime planes

The daemon follows the standard runtime split:

- **Signal** admits framed ordinary and meta requests, validates request shape,
  and emits typed replies and typed rejections.
- **Nexus** owns collection orchestration, adapter calls, output-interface
  operations, time-window lowering, limits, pagination, truncation accounting,
  and effect failures.
- **SEMA** owns active configuration state and persisted configuration through
  `ConfigurationStore`.

The request flow is `Signal -> Nexus -> SEMA when state is needed -> Nexus ->
Signal -> client`. The ordinary CLI `aggregator` and the meta CLI
`meta-aggregator` are thin Unix-socket clients. Configuration is changed through
the `meta-signal-aggregator` contract; ordinary collection and output reads
cannot mutate configuration.

## Source boundaries

The source of truth is underlying runtime evidence: harness/session/subagent
transcripts and event evidence, plus configured repository evidence for the
collection surface. Current adapters read explicitly configured Claude JSONL, Claude subagent
`.output` JSONL, Codex session, Pi run-history, and optional repository roots.
Transcript-only configuration is supported for recovery; transcript locations
and formats are adapter records, not hard-coded private paths.

The output-interface index is derived from configured transcript evidence. It is
not derived from agent-written reports as a normal workflow.

Optional legacy recovery roots for old `reports/` or `agent-outputs/` material
are read-only, opt-in recovery or migration inputs. They are not recommended as
future normal architecture, are not authoritative live sources, and must not own
the daemon-local fragile index. Remove them after the recovery or migration they
serve.

Long-term integration should prefer pushed transcript/event updates from the
producer over polling or scanning roots. The current configured-root readers are
an implementation bridge, not the desired final coupling.

## Output interfaces and fragile references

The ordinary contract exposes metadata-first output operations:

- `ListSessions` lists paged session cards.
- `InventorySessions` lists metadata-only session inventory cards with per-source scan completeness.
- `LookupSession` resolves sessions by fragile reference, producer session identifier, or source locator.
- `WriteSessionArchive`, `QuerySessionArchive`, and `ReadSessionArchive` store and read agent-authored summaries in an explicit local rkyv archive path.
- `ListSubagents` lists subagent cards for a selected session.
- `ListOutputs` lists output cards with `MetadataOnly` or bounded-preview
  projection.
- `ListOutputSegments` lists segment cards for a selected output.
- `ListTranscriptBlocks` lists whole logical transcript-block cards with grounded kind selection and optional bounded previews.
- `SearchTranscriptBlocks` applies `nota-text-query` over readable transcript blocks and returns query evidence with matching cards.
- `ObserveHealth` reports metadata-first runtime capabilities, configured source health, and fragile-index counts without transcript text.
- `EstimateTranscriptBlock` estimates a selected block before text projection.
- `ReadTranscriptBlock` reads a selected whole block only with an explicit `maximum_bytes` bounded by the configured read cap.
- `EstimateOutput` estimates an explicit output range.
- `ReadOutput` reads only an explicit range bounded by the configured read cap.

UIs and agents should consume cards first, then request bounded reads only for
selected references. V3 page cursors are size-capped keyset continuations bound
to the snapshot identity, collection, filters, order, and page limit. They hold
the last emitted candidate and a sort-tuple digest, never an offset or a
corpus-sized reference signature. Changing evidence, configuration, coverage,
or listing shape makes a cursor stale; a legacy v2 cursor is rejected as stale.

The grounded `TranscriptBlockKind` vocabulary is `UserPrompt`, `AgentResponse`,
`ToolCall`, `ToolResult`, `Inference`, `SystemInstruction`, `Attachment`,
`SessionEvent`, and `Unclassified`. The runtime does not infer a generic final
response kind; current Codex caveats are represented as data by falling back to
`SessionEvent` for some additional payload categories and `Unclassified` for
current developer-role messages.

Fragile references are daemon-local opaque identifiers into backing runtime
evidence. The durable sidecar index stores references, metadata, fingerprints,
segment spans, and bounded card material needed for navigation. It is not
canonical content storage and must not become a report archive. The established
`.output-index.json` path is a small v3 compatibility pointer; immutable typed
chunks, manifests, checkpoints, migration backups, and best-effort garbage
collection live in its adjacent `.output-index.json.d/` directory. Each chunk
has fixed logical, serialized, record, and query-work limits and is validated
for kind, checksum, and size before decoding.

Refreshes publish snapshot-bound v3 state atomically. Complete sources advance
independently; incomplete sources retain their last-complete view and report
provisional/resumable coverage rather than being mistaken for complete data.
A v2 document is migration-only: it is boundedly imported and copied once to an
immutable rollback backup before the pointer is replaced. Version-1 data is
obsolete and is discarded without decoding. Backing evidence remains the read
source, so references can become stale, missing, or broken when those files
change; operations reject those cases with typed `OperationRejected` replies
instead of guessing.

## Privacy and projection

Raw transcript text can be private. Metadata-only cards and identifiers-only
navigation are first-class. Text projection is always bounded by configured
limits, and segment or output reads report truncation facts explicitly. An
unreadable or truncated source is data in the reply; it is not hidden behind
prose.

## Code map

```text
src/bin/aggregator-daemon.rs              daemon entrypoint for ordinary and meta sockets
src/bin/aggregator.rs                     ordinary socket client CLI
src/bin/meta-aggregator.rs                meta socket client CLI
src/bin/aggregator-write-configuration.rs configuration file writer CLI
src/client.rs                             Unix-socket client exchange helpers
src/daemon.rs                             prototype Unix-socket daemon services and frame routing
src/signal.rs                             Signal validation, version, and rejection helpers
src/nexus.rs                              collection orchestration and output-interface routing
src/sema.rs                               configuration state and meta operations
src/output_index.rs                       durable fragile index, inventory cards, cursors, estimates, reads, rejections
src/archive.rs                            local rkyv session archive read/write/query store
src/configuration.rs                      configuration storage, validation, limits, legacy recovery boundaries
src/adapter/claude.rs                     Claude JSONL transcript adapter
src/adapter/codex.rs                      Codex session transcript adapter
src/adapter/pi.rs                         Pi run-history transcript adapter
src/adapter/repository.rs                 repository evidence adapter
src/clock.rs                              collection reference time handling
src/time_model.rs                         timestamp parsing and comparison
src/error.rs                              typed crate error boundary
schema/runtime.schema                     runtime triad schema sketch
generated/README.md                       schema-generation placeholder
tests/boundary.rs                         contract, daemon, adapter, and output-interface witnesses
examples/collect.nota                     coarse evidence collection request example
examples/configuration.nota               current configuration example
examples/output-interface-requests.nota   metadata-first output operation request examples
examples/session-inventory-archive-requests.nota inventory and local archive request examples
examples/output-interface-replies.nota    output operation reply and rejection examples
examples/transcript-block-search-requests.nota  transcript block scrape/search/read request examples
examples/transcript-block-search-replies.nota   transcript block reply, evidence, and rejection examples
```

## Current status

The configured runtime path implements collection over configured transcript and
repository evidence, and the daemon serves ordinary and meta frame requests over
Unix sockets. The output interface implementation is present: session,
subagent, output, segment, and transcript-block listings; complete metadata-first session inventory and lookup; aggregator-local rkyv session archive write/query/read with explicit archive paths; transcript-block
search with `nota-text-query` evidence; size estimates; bounded reads; durable
store-derived fragile index; metadata-first cards; typed stale, missing, broken,
oversized, invalid-range, invalid-query, and invalid-request rejections; and
query-bound page cursors.

The legacy no-runtime-configuration Nexus constructor still returns typed
not-implemented errors and exists only for scaffold-era boundary coverage. The
schema sketch under `schema/` remains a sketch; the Rust implementation and the
`signal-aggregator` and `meta-signal-aggregator` contracts are the active
runtime surfaces.
