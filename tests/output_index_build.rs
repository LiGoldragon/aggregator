use std::{fs, path::PathBuf};

use aggregator::{
    adapter::{TranscriptRecord, TranscriptRecordSink},
    output_index::{
        build::{BoundedGenerationBuilder, SourceKey},
        instrumentation::IndexResourceMeter,
        limits::IndexStoreLimits,
        schema::{IndexFileKind, ProjectionRecordDto},
        store::{ChunkReader, IndexStore},
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
    assert_ne!(
        first.scoped_reference_material("producer-session", "shared-session"),
        second.scoped_reference_material("producer-session", "shared-session"),
        "an equal producer session identifier must not merge two configured sources"
    );
    assert_ne!(
        first.scoped_reference_material("producer-session", "shared-session"),
        other_root.scoped_reference_material("producer-session", "shared-session"),
        "source-root identity is part of descendant reference material"
    );
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

#[test]
fn builder_emits_every_projection_kind_and_scalar_reference_edges() {
    let root = TempDir::new().expect("temporary index root");
    let store = IndexStore::new(root.path().join("store.output-index.json"), tiny_limits());
    let staging = store.create_staging("typed-projection").expect("staging");
    let source_key = SourceKey::new(
        SourceKind::Claude,
        SourceIdentifier::new("claude:fixture"),
        0,
    );
    let mut builder = BoundedGenerationBuilder::new(
        staging.clone(),
        source_key,
        tiny_limits(),
        IndexResourceMeter::default(),
    );
    for line_number in 1..=4 {
        let record = record(line_number)
            .with_subagent_name(Some(signal_aggregator::SubagentName::new("worker")))
            .with_blocks(vec![
                aggregator::adapter::TranscriptBlockSourceContext::new(
                    SourceKind::Claude,
                    SourceIdentifier::new("claude:fixture"),
                    PathBuf::from("/private/fixture/session.jsonl"),
                    line_number,
                    None,
                )
                .readable_block(
                    0,
                    signal_aggregator::TranscriptBlockKind::AgentResponse,
                    "block text".to_owned(),
                ),
            ]);
        builder.observe_record(record);
    }
    let run = builder.finish().expect("typed source run");
    assert!(run.chunk_count > 5, "projection leaves spill independently");

    let mut kinds = std::collections::BTreeSet::new();
    let mut reference_edges = 0_u64;
    for entry in fs::read_dir(staging.path()).expect("staging entries") {
        let path = entry.expect("entry").path();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        if name.contains("projection") {
            let chunk = ChunkReader::new(path, IndexFileKind::Projection, tiny_limits())
                .read()
                .expect("typed projection chunk");
            match chunk.projection.expect("projection payload") {
                ProjectionRecordDto::Session(_) => {
                    kinds.insert("session");
                }
                ProjectionRecordDto::Subagent(_) => {
                    kinds.insert("subagent");
                }
                ProjectionRecordDto::Output(_) => {
                    kinds.insert("output");
                }
                ProjectionRecordDto::Segment(_) => {
                    kinds.insert("segment");
                }
                ProjectionRecordDto::TranscriptBlock(_) => {
                    kinds.insert("block");
                }
            }
        } else if name.contains("index") {
            let chunk = ChunkReader::new(path, IndexFileKind::ReferenceIndex, tiny_limits())
                .read()
                .expect("reference edge chunk");
            reference_edges += chunk.records.len() as u64;
            assert!(
                chunk
                    .records
                    .iter()
                    .all(|record| { record.fields.iter().all(|field| field.name != "children") })
            );
        }
    }
    assert_eq!(
        kinds,
        std::collections::BTreeSet::from(["session", "subagent", "output", "segment", "block"])
    );
    assert!(
        reference_edges >= 12,
        "each projection is discoverable without parent child arrays"
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
