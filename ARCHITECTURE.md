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

- **Signal** admits framed ordinary and meta queries, validates query shape,
  and emits typed responses and typed rejections.
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

## Wire and text

The wire is `signal` 3.0.2 and nothing above it. One request is one frame
carrying the rkyv archive of the contract's `Query`; one reply is one frame
carrying its `Response`. A frame is a four-byte big-endian length prefix and a
body, bounded by `FrameCapacity`. There are no exchange identifiers, no lanes,
no sequence numbers, and no sub-replies: `signal` owns no protocol above the
archive, and the living has not decided one. When a protocol above the archive
is wanted, it is decided and then written here; nothing in this component
anticipates it.

The text surface is Datom. Every request a person or a CLI writes, every reply
printed, and the stored configuration file are Datom text actualized into and
projected out of the same generated contract types. The stored configuration
lives at `configuration.datom`. There is no second notation and no second
decode path.

The whole wire vocabulary is one Ethos Signal declaration in each contract
crate, generated into Rust. The runtime holds no hand-written wire type.

## The text-query tree

The contract carries a transcript-block text query as the recursive
`TextQuery` and matching evidence as the recursive `MatchEvidence`, each
declared in `signal-aggregator` (1.0.0 and later); a child sits in place inside
its parent.

`dotos-text-query` is the matching engine and nothing else. It is depended on
with default features off, so it carries no Dotos dependency. `src/text_query/`
is the only place the contract tree and the engine tree meet: it projects the
contract tree into the engine's tree to run a match, and the engine's evidence
back into a contract tree. The engine's types never reach the wire.

The tree is peer input, and its depth is bounded only by the frame that carried
it, so the projection is bounded as it recurses. A tree past its node bound, a
tree past its depth bound, and a distance or position the engine cannot hold
are each an `OperationRejected` with `OperationRejectionReason::InvalidQuery`.

A received frame's nesting is bounded before it is decoded, with the same
limits (depth 32, 256 nodes). rkyv validation recurses once per nested pointer,
so `wire::ReceivedFrame` validates within `MAXIMUM_FRAME_NESTING` (the tree
depth plus a fixed envelope allowance of 16); a frame past that ceiling is
refused as `FrameRefusal::Unvalidated`. Inside the ceiling every `TextQuery`
and `MatchEvidence` tree is measured still archived against the projection
budget, and a tree past either bound is refused as
`FrameRefusal::TreeOutsideBound` before any of it is allocated. On the ordinary
socket that refusal is answered with the same `InvalidQuery` rejection, naming
the request; a frame that does not validate gets no reply.

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
- `SearchTranscriptBlocks` applies the `dotos-text-query` matching engine over readable transcript blocks and returns query evidence, as a `MatchEvidence` tree, with matching cards.
- `ObserveHealth` reports metadata-first runtime capabilities, configured source health, and fragile-index counts without transcript text.
- `EstimateTranscriptBlock` estimates a selected block before text projection.
- `ReadTranscriptBlock` reads a selected whole block only with an explicit `maximum_bytes` bounded by the configured read cap.
- `EstimateOutput` estimates an explicit output range.
- `ReadOutput` reads only an explicit range bounded by the configured read cap.

UIs and agents should consume cards first, then request bounded reads only for
selected references. V3 page cursors are size-capped continuations bound to
the snapshot identity, collection, filters, order, and page limit. They hold
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
evidence. References rooted in producer sessions include configured source kind,
source identifier, and configured occurrence, so equal producer identifiers
cannot cross source/privacy boundaries. The durable sidecar index stores
references, metadata, fingerprints,
segment spans, and bounded card material needed for navigation. It is not
canonical content storage and must not become a report archive. The established
`.output-index.json` path is a small v3 compatibility pointer; immutable typed
chunks, manifests, checkpoints, migration backups, and best-effort garbage
collection live in its adjacent `.output-index.json.d/` directory. Each chunk
has fixed logical, serialized, record, and query-work limits and is validated
for kind, checksum, and size before decoding.

The live output interface refreshes through the v3 typed generation writer and
navigates only published fixed-fanout reference roots and decodes selected rkyv projection leaves. A refresh scans each configured
source once; incomplete scans retain the last complete publication. Published
pointers and chunks
are typed binary records; JSON remains confined to the bounded v2 migration
adapter. Backing evidence remains the read source, so references can become
stale, missing, or broken when those files change; operations reject those
cases with typed `OperationRejected` replies instead of guessing.

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
src/client.rs                             CLI argument reading and client commands
src/wire.rs                               Datom text and the Signal frame
src/daemon.rs                             prototype Unix-socket daemon services and frame routing
src/counting.rs                           measured counts and contract counts
src/text_query.rs                         text-query tree projection and its faults
src/text_query/query.rs                   text query in both shapes
src/text_query/evidence.rs                matching evidence in both shapes
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
tests/boundary.rs                         contract, daemon, adapter, and output-interface witnesses
tests/text_query_projection.rs            contract tree and engine tree witnesses
examples/collect.datom                    coarse evidence collection query
examples/configuration.datom              current configuration shape
examples/transcript-block-search.datom    transcript block search query with a text-query tree
examples/transcript-block-read.datom      bounded transcript block read response
```

## Current status

The configured runtime path implements collection over configured transcript and
repository evidence, and the daemon serves ordinary and meta frame requests over
Unix sockets. The output interface implementation is present: session,
subagent, output, segment, and transcript-block listings; complete metadata-first session inventory and lookup; aggregator-local rkyv session archive write/query/read with explicit archive paths; transcript-block
search with matching evidence carried as a tree; size estimates; bounded reads; durable
store-derived fragile index; metadata-first cards; typed stale, missing, broken,
oversized, invalid-range, invalid-query, and invalid-request rejections; and
query-bound page cursors.

The legacy no-runtime-configuration Nexus constructor still returns typed
not-implemented errors and exists only for scaffold-era boundary coverage. The
Rust implementation and the `signal-aggregator` and `meta-signal-aggregator`
Ethos declarations are the active runtime surfaces.
