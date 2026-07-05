# aggregator — architecture

`aggregator` is the runtime component for collecting recent work evidence over
requested time windows. It reads configured Claude, Codex, Pi, and repository
sources, normalizes what it observes, and returns an evidence package through
`signal-aggregator`.

## Role

The component owns runtime collection and normalization only. It reports source
volumes, timestamps, paths or identifiers, repository changes and commits,
transcript segment locators, bounded projected excerpts when requested,
truncation facts, and read-failure facts. It does not produce summaries,
reviews, recommendations, scores, or judgments. Agents synthesize from the
returned evidence package.

## Triad runtime shape

The daemon is split into the standard runtime planes:

- **Signal** admits framed ordinary and meta requests, authenticates the caller
  shape when the real daemon lands, and emits typed replies.
- **Nexus** owns collection orchestration, adapter calls, time-window lowering,
  limits, truncation accounting, and effect failures.
- **SEMA** owns durable configuration and collection state once persistence
  lands.

The request flow is `Signal -> Nexus -> SEMA when state is needed -> Nexus ->
Signal -> client`. The ordinary CLI `aggregator` is a client of the daemon. The
meta CLI `meta-aggregator` is a client of the meta socket. Configuration is
changed through `meta-signal-aggregator`; ordinary collection cannot mutate
configuration.

## Sources and adapters

Active repositories come from configuration. Transcript locations and formats
are adapter-specific records, not hard-coded daemon paths. The first adapter
modules can read fixture/configured roots for Claude JSONL transcripts, Codex
session roots and indexes, Pi run-history roots, and fixture or policy-backed
repository evidence. They return normalized evidence plus typed read failures;
daemon orchestration and durable persistence remain separate work.

## Time windows

The ordinary contract carries bounded windows as `Recent(RelativeDuration)`,
`Range(TimeRange)`, and `Since(Timestamp)`. The runtime lowers these into
adapter-specific reads and records read failures instead of synthesizing around
missing data.

## Privacy and projection

Raw transcript text can be private. Metadata-only and identifiers-only
projections are first-class. Text projection is bounded by `LimitPolicy` and the
segment-level projection records truncation facts. A truncated or unreadable
source is reported as data in the package; it is not hidden behind prose.

## Code map

```text
src/bin/aggregator-daemon.rs              daemon entrypoint scaffold
src/bin/aggregator.rs                     ordinary CLI scaffold
src/bin/meta-aggregator.rs                meta CLI scaffold
src/bin/aggregator-write-configuration.rs configuration writer scaffold
src/signal.rs                             Signal plane boundary helpers
src/nexus.rs                              Nexus collection orchestration scaffold
src/sema.rs                               SEMA configuration/state scaffold
src/adapter/claude.rs                     Claude transcript adapter scaffold
src/adapter/codex.rs                      Codex transcript adapter scaffold
src/adapter/pi.rs                         Pi transcript adapter scaffold
src/adapter/repository.rs                 repository adapter scaffold
src/configuration.rs                      configuration fixture/storage skeleton
schema/runtime.schema                     runtime triad schema sketch
generated/README.md                       schema-generation placeholder
tests/boundary.rs                         scaffold boundary witnesses
```

## Status

This is a foundation slice. It compiles and exposes the repo shape, current
contract dependencies, binaries, configuration validation, adapter modules, and
boundary tests. The adapters only read explicitly configured or fixture roots;
the Nexus/daemon path still deliberately returns typed not-implemented errors
rather than scanning local private sources.
