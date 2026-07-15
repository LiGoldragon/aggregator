use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
};

use aggregator::{
    Error, IndexStoreError,
    output_index::{
        limits::IndexStoreLimits,
        migration_v2::{IndexFormat, IndexFormatProbe, MigrationSource, V2Migration},
        reconciliation::{
            ChildContribution, ParentSummaryCompactor, ParentSummarySink, SourceGeneration,
            SourceReconciler, SourceRefreshFact, SourceSlotState,
        },
        schema::{IndexFileKind, SourceCoverageStatus},
        store::{ChunkReader, IndexStore},
    },
};
use tempfile::TempDir;

fn source(occurrence: u64) -> MigrationSource {
    MigrationSource::new("Claude".to_owned(), "claude:fixture".to_owned(), occurrence)
}

fn v2_fixture(preview: &str) -> String {
    format!(
        r#"{{"version":2,"sessions":[{{"reference":"session:one","source":"Claude","source_identifier":"claude:fixture","path":"/synthetic/session.jsonl","fingerprint":{{"byte_count":1}},"size":{{"byte_count":1}},"subagent_references":["subagent:one"],"output_references":["output:one"]}}],"subagents":[{{"reference":"subagent:one","session_reference":"session:one","name":"worker","authored_status":"UnknownAuthorship","size":{{"byte_count":1}},"output_references":["output:one"]}}],"outputs":[{{"reference":"output:one","session_reference":"session:one","source":"Claude","source_identifier":"claude:fixture","authored_status":"UnknownAuthorship","path":"/synthetic/session.jsonl","fingerprint":{{"byte_count":1}},"source_line_number":1,"text_hash":"hash","size":{{"byte_count":1}},"preview_text":"{preview}","preview_original_bytes":1}}],"segments":[{{"reference":"segment:one","output_reference":"output:one","segment_index":0,"size":{{"byte_count":1}},"preview_text":"{preview}","preview_original_bytes":1,"source":"Claude","path":"/synthetic/session.jsonl"}}],"transcript_blocks":[{{"reference":"block:one","session_reference":"session:one","kind":"AgentResponse","block_index":0,"source":"Claude","source_identifier":"claude:fixture","authored_status":"UnknownAuthorship","path":"/synthetic/session.jsonl","fingerprint":{{"byte_count":1}},"source_line_number":1,"text_hash":"hash","size":{{"byte_count":1}},"text_availability":"ReadableText","preview_text":"{preview}","preview_original_bytes":1}}]}}"#
    )
}

fn store(root: &TempDir) -> IndexStore {
    IndexStore::new(root.path().join("output-index.json"), tiny_limits())
}

fn tiny_limits() -> IndexStoreLimits {
    IndexStoreLimits {
        maximum_logical_chunk_bytes: 4096,
        maximum_serialized_chunk_bytes: 8192,
        maximum_records_per_chunk: 2,
        maximum_manifest_bytes: 8192,
        maximum_checkpoint_bytes: 8192,
        maximum_cursor_bytes: 4096,
        maximum_record_bytes: 2048,
        maximum_string_bytes: 128,
        maximum_merge_fan_in: 2,
        maximum_query_candidates: 8,
        staging_generations_retained: 2,
    }
}

#[test]
fn streaming_v2_import_preserves_references_and_counts_without_child_arrays() {
    let root = TempDir::new().expect("temporary root");
    let input = root.path().join("legacy-v2.json");
    fs::write(&input, v2_fixture("preview")).expect("fixture");
    let store = store(&root);
    let staging = store.create_staging("migration").expect("staging");
    let result = V2Migration::new(tiny_limits(), vec![source(0)])
        .import_into_staging(&input, &staging)
        .expect("stream v2 fixture");

    assert_eq!(result.collection_counts.values().sum::<u64>(), 5);
    assert_eq!(result.source_runs[0].record_count, 5);
    assert!(
        result.source_runs[0].chunk_count > 1,
        "records spill through bounded chunks"
    );
    let first = ChunkReader::new(
        staging
            .path()
            .join(&result.source_runs[0].chunk_locators[0]),
        IndexFileKind::Chunk,
        tiny_limits(),
    )
    .read()
    .expect("read migrated chunk");
    let session = first
        .records
        .iter()
        .find(|record| record.record_kind == 10)
        .expect("session record");
    assert!(
        session
            .fields
            .iter()
            .any(|field| field.name == "reference" && field.bytes == b"\"session:one\"")
    );
    assert!(
        session
            .fields
            .iter()
            .any(|field| field.name == "subagent_references-count" && field.bytes == b"1")
    );
    assert!(
        !session
            .fields
            .iter()
            .any(|field| field.name == "subagent_references"),
        "child references are scalar-only during import"
    );
}

#[test]
fn malformed_or_oversized_v2_keeps_the_only_legacy_evidence_untouched() {
    let root = TempDir::new().expect("temporary root");
    let input = root.path().join("legacy-v2.json");
    let store = store(&root);
    for fixture in [
        b"{\"version\":2,\"sessions\":[".as_slice(),
        v2_fixture(&"x".repeat(129)).as_bytes(),
    ] {
        fs::write(&input, fixture).expect("fixture");
        let original = fs::read(&input).expect("original bytes");
        let staging = store.create_staging("rejected").expect("staging");
        let error = V2Migration::new(tiny_limits(), vec![source(0)])
            .import_into_staging(&input, &staging)
            .expect_err("invalid v2 rejects");
        assert!(matches!(
            error,
            Error::IndexStore {
                source: IndexStoreError::MigrationFailure { .. }
            }
        ));
        assert_eq!(fs::read(&input).expect("bytes after failure"), original);
    }
}

#[test]
fn duplicate_reference_is_idempotent_but_divergence_is_corruption() {
    let root = TempDir::new().expect("temporary root");
    let input = root.path().join("legacy-v2.json");
    let fixture = v2_fixture("preview");
    let session_start = fixture.find("[{").expect("session array") + 1;
    let session_end = fixture.find("],\"subagents\"").expect("session array end");
    let session = &fixture[session_start..session_end];
    let duplicate = fixture.replacen("],\"subagents\"", &format!(",{session}],\"subagents\""), 1);
    fs::write(&input, duplicate).expect("duplicate fixture");
    let store = store(&root);
    let staging = store.create_staging("duplicate").expect("staging");
    let idempotent = V2Migration::new(tiny_limits(), vec![source(0)])
        .import_into_staging(&input, &staging)
        .expect("identical reference deduplicates");
    assert_eq!(idempotent.source_runs[0].record_count, 5);

    let divergent_session = session.replace("/synthetic/session.jsonl", "/synthetic/changed.jsonl");
    let divergent = fixture.replacen(
        "],\"subagents\"",
        &format!(",{divergent_session}],\"subagents\""),
        1,
    );
    fs::write(&input, divergent).expect("divergent fixture");
    let staging = store.create_staging("divergent").expect("staging");
    let error = V2Migration::new(tiny_limits(), vec![source(0)])
        .import_into_staging(&input, &staging)
        .expect_err("same fragile reference with changed evidence is corruption");
    assert!(matches!(
        error,
        Error::IndexStore {
            source: IndexStoreError::ReferenceCollision
        }
    ));
}

#[test]
fn immutable_backup_is_streamed_once_and_never_becomes_an_archive() {
    let root = TempDir::new().expect("temporary root");
    let store = store(&root);
    let input = root.path().join("legacy-v2.json");
    fs::write(&input, v2_fixture("first")).expect("first evidence");
    let backup = store.retain_v2_backup(&input).expect("backup");
    let first = fs::read(&backup).expect("backup bytes");
    fs::write(&input, v2_fixture("second")).expect("changed evidence");
    assert_eq!(
        store.retain_v2_backup(&input).expect("existing backup"),
        backup
    );
    assert_eq!(fs::read(&backup).expect("immutable backup"), first);
    assert_eq!(
        fs::read_dir(backup.parent().expect("migration directory"))
            .expect("backup directory")
            .count(),
        1
    );
}

#[test]
fn probe_distinguishes_v2_v3_obsolete_and_unknown_formats() {
    assert_eq!(IndexFormatProbe::new(b" \n").format(), IndexFormat::Missing);
    assert_eq!(
        IndexFormatProbe::new(b"{\"version\":1,").format(),
        IndexFormat::ObsoleteV1
    );
    assert_eq!(
        IndexFormatProbe::new(b"{\"version\":2,").format(),
        IndexFormat::MigratableV2
    );
    assert_eq!(
        IndexFormatProbe::new(b"AGGIDX03").format(),
        IndexFormat::CurrentV3
    );
    assert_eq!(
        IndexFormatProbe::new(b"nope").format(),
        IndexFormat::Unsupported
    );
}

#[test]
fn complete_sources_replace_independently_and_removed_or_empty_sources_disappear() {
    let reconciler = SourceReconciler;
    let prior = vec![
        SourceSlotState {
            source: source(0),
            last_complete: Some(SourceGeneration::new(
                "old-a".to_owned(),
                BTreeSet::from(["a/file".to_owned()]),
                1,
            )),
            provisional_visible: None,
            checkpoint: None,
            coverage: SourceCoverageStatus::Complete,
        },
        SourceSlotState {
            source: source(1),
            last_complete: Some(SourceGeneration::new(
                "old-b".to_owned(),
                BTreeSet::from(["b/file".to_owned()]),
                1,
            )),
            provisional_visible: None,
            checkpoint: None,
            coverage: SourceCoverageStatus::Complete,
        },
    ];
    let facts = BTreeMap::from([
        (
            0,
            SourceRefreshFact::Incomplete {
                generation: SourceGeneration::new(
                    "partial-a".to_owned(),
                    BTreeSet::from(["a/finished-file".to_owned()]),
                    1,
                ),
                checkpoint: "cursor-a".to_owned(),
            },
        ),
        (
            1,
            SourceRefreshFact::Complete {
                generation: SourceGeneration::complete_empty("empty-b".to_owned()),
            },
        ),
    ]);
    let result = reconciler.reconcile(prior, vec![source(0), source(1)], facts);
    assert_eq!(
        result.slots[0]
            .last_complete
            .as_ref()
            .expect("old complete")
            .locator,
        "old-a"
    );
    assert_eq!(
        result.slots[0]
            .visible_generation()
            .expect("provisional")
            .locator,
        "partial-a"
    );
    assert_eq!(result.slots[0].coverage, SourceCoverageStatus::Incomplete);
    let persisted = result.slots[0].disk_slot();
    assert_eq!(persisted.last_complete.as_deref(), Some("old-a"));
    assert_eq!(persisted.visible_generation.as_deref(), Some("partial-a"));
    assert_eq!(
        persisted.provisional_checkpoint.as_deref(),
        Some("cursor-a")
    );
    assert_eq!(
        SourceCoverageStatus::from_u8(persisted.coverage_status),
        Some(SourceCoverageStatus::Incomplete)
    );
    assert_eq!(
        result.slots[1]
            .last_complete
            .as_ref()
            .expect("empty complete")
            .locator,
        "empty-b"
    );
    assert_eq!(
        result.slots[1]
            .last_complete
            .as_ref()
            .expect("empty complete")
            .record_count,
        0
    );
    assert!(
        result
            .tombstones
            .iter()
            .any(|tombstone| tombstone.source_occurrence == 0
                && tombstone.scope == "a/finished-file")
    );
    assert!(
        !result
            .tombstones
            .iter()
            .any(|tombstone| tombstone.scope == "a/file"),
        "unvisited scope cannot delete"
    );

    let removed = reconciler.reconcile(result.slots, vec![source(0)], BTreeMap::new());
    assert_eq!(
        removed.slots.len(),
        1,
        "removed configuration omits its entire slot"
    );
}

#[test]
fn restart_failure_keeps_checkpoint_and_parent_compaction_deduplicates_provisional_children() {
    let reconciler = SourceReconciler;
    let partial = SourceRefreshFact::Incomplete {
        generation: SourceGeneration::new("partial".to_owned(), BTreeSet::new(), 2),
        checkpoint: "checkpoint".to_owned(),
    };
    let first = reconciler.reconcile(
        Vec::new(),
        vec![source(0)],
        BTreeMap::from([(0, partial.clone())]),
    );
    let restarted = reconciler.reconcile(
        first.slots.clone(),
        vec![source(0)],
        BTreeMap::from([(0, SourceRefreshFact::Failed)]),
    );
    assert_eq!(restarted.slots[0].checkpoint.as_deref(), Some("checkpoint"));
    assert_eq!(restarted.slots[0].coverage, SourceCoverageStatus::Failed);
    let mut summaries = TestSummarySink::default();
    ParentSummaryCompactor.compact_sorted(
        [
            ChildContribution {
                reference: "child-a".to_owned(),
                parent_reference: "parent".to_owned(),
                source_occurrence: 0,
            },
            ChildContribution {
                reference: "child-a".to_owned(),
                parent_reference: "parent".to_owned(),
                source_occurrence: 0,
            },
            ChildContribution {
                reference: "child-b".to_owned(),
                parent_reference: "parent".to_owned(),
                source_occurrence: 0,
            },
        ],
        &mut summaries,
    );
    assert_eq!(
        summaries.entries,
        vec![("parent".to_owned(), 2)],
        "restart never double-counts a child"
    );
}

#[derive(Default)]
struct TestSummarySink {
    entries: Vec<(String, u64)>,
}
impl ParentSummarySink for TestSummarySink {
    fn observe_parent_summary(&mut self, parent_reference: String, child_count: u64) {
        self.entries.push((parent_reference, child_count));
    }
}

#[test]
fn v2_pointer_is_not_misrepresented_as_an_empty_v3_index() {
    use aggregator::output_index::{limits::IndexStoreLimits, store::IndexStore};

    let root = TempDir::new().expect("temporary root");
    let index = IndexStore::new(
        root.path().join("store.output-index.json"),
        IndexStoreLimits::default(),
    );
    fs::write(index.pointer_path(), v2_fixture("synthetic-preview")).expect("v2 evidence");

    let error = index
        .read_current_pointer()
        .expect_err("v2 must remain explicit migration evidence");
    assert!(matches!(error, Error::IndexStore { .. }));
    assert_eq!(
        fs::read(index.pointer_path()).expect("unchanged v2 pointer"),
        v2_fixture("synthetic-preview").as_bytes(),
        "probing does not destroy the only complete legacy evidence"
    );
}
