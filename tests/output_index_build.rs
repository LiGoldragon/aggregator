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
fn large_logical_session_spills_capped_immutable_observation_chunks() {
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

    for line_number in 1..=64 {
        builder.observe_record(record(line_number));
    }
    let run = builder.finish().expect("bounded source run");
    let counters = meter.snapshot();

    assert_eq!(run.record_count, 64);
    assert!(
        run.chunk_count > 1,
        "a logical session must cross chunk boundaries"
    );
    assert!(
        run.chunk_count >= 22,
        "three-record chunks bound one large session"
    );
    assert!(
        counters.high_water_bytes <= tiny_limits().maximum_logical_chunk_bytes,
        "live builder reservations must be independent of corpus cardinality"
    );
    assert_eq!(
        counters.live_bytes, 0,
        "flushed chunks release their reservation"
    );
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
