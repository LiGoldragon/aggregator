use std::path::PathBuf;

use aggregator::{
    adapter::{TranscriptRecord, TranscriptRecordSink},
    output_index::{
        build::{BoundedGenerationBuilder, SourceKey},
        instrumentation::IndexResourceMeter,
        limits::IndexStoreLimits,
        store::IndexStore,
    },
};
use signal_aggregator::{SourceIdentifier, SourceKind};
use tempfile::TempDir;

fn record(line_number: u64) -> TranscriptRecord {
    TranscriptRecord::new(
        SourceKind::Claude,
        SourceIdentifier::new("claude:fixture"),
        PathBuf::from("/private/fixture/session.jsonl"),
        line_number,
        None,
        "bounded transcript line".to_owned(),
    )
}

#[test]
fn source_occurrence_prevents_cross_root_or_duplicate_configuration_merges() {
    let source = SourceIdentifier::new("claude:same-root");
    let first = SourceKey::new(SourceKind::Claude, source.clone(), 0);
    let second = SourceKey::new(SourceKind::Claude, source, 1);
    let other_root = SourceKey::new(
        SourceKind::Claude,
        SourceIdentifier::new("claude:other-root"),
        0,
    );

    assert_ne!(first, second);
    assert_ne!(first.signature(), second.signature());
    assert_ne!(first, other_root);
}

#[test]
fn actual_builder_high_water_is_constant_across_substantially_different_corpora() {
    let small = build_corpus(64);
    let large = build_corpus(4_096);
    let limits = tiny_limits();

    assert_eq!(small.high_water_bytes, large.high_water_bytes);
    assert!(
        large.high_water_bytes <= limits.maximum_logical_chunk_bytes,
        "the live maximum follows the configured logical-chunk formula, not corpus cardinality"
    );
    assert_eq!(small.live_bytes, 0);
    assert_eq!(large.live_bytes, 0);
}

fn build_corpus(records: u64) -> aggregator::output_index::instrumentation::IndexResourceCounters {
    let root = TempDir::new().expect("temporary index root");
    let store = IndexStore::new(root.path().join("store.output-index.json"), tiny_limits());
    let staging = store.create_staging("builder-test").expect("staging");
    let meter = IndexResourceMeter::default();
    let source_key = SourceKey::new(
        SourceKind::Claude,
        SourceIdentifier::new("claude:fixture"),
        0,
    );
    let mut builder =
        BoundedGenerationBuilder::new(staging, source_key, tiny_limits(), meter.clone());

    for line_number in 1..=records {
        builder.observe_record(record(line_number));
    }
    let run = builder.finish().expect("bounded source run");
    assert_eq!(run.record_count, records);
    assert!(
        run.chunk_count > 1,
        "corpus spills through immutable chunks"
    );
    meter.snapshot()
}

fn tiny_limits() -> IndexStoreLimits {
    IndexStoreLimits {
        maximum_logical_chunk_bytes: 1024,
        maximum_serialized_chunk_bytes: 4096,
        maximum_records_per_chunk: 3,
        maximum_manifest_bytes: 4096,
        maximum_checkpoint_bytes: 4096,
        maximum_cursor_bytes: 4096,
        maximum_record_bytes: 1024,
        maximum_string_bytes: 1024,
        maximum_merge_fan_in: 2,
        maximum_query_candidates: 16,
        staging_generations_retained: 2,
    }
}
