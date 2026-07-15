use std::{fs, path::PathBuf};

use aggregator::adapter::{
    TranscriptFileAction, TranscriptFileCoverage, TranscriptFileDescriptor, TranscriptFileSync,
    TranscriptRecord, TranscriptRecordSink, TranscriptScanRequest, claude::ClaudeJsonlRootReader,
};
use signal_aggregator::{SourceIdentifier, SourceKind};
use tempfile::TempDir;

#[derive(Debug, Default)]
struct SyncedFileSink {
    records: u64,
    begins: Vec<u64>,
    completed: Vec<u64>,
}

impl TranscriptRecordSink for SyncedFileSink {
    fn observe_record(&mut self, _record: TranscriptRecord) {
        self.records += 1;
    }

    fn begin_file(&mut self, descriptor: &TranscriptFileDescriptor) -> TranscriptFileAction {
        self.begins.push(descriptor.discovery_ordinal);
        TranscriptFileAction::Read
    }

    fn complete_file(&mut self, coverage: &TranscriptFileCoverage) -> TranscriptFileSync {
        assert!(
            coverage.completed,
            "only unchanged EOF files may checkpoint"
        );
        self.completed.push(coverage.descriptor.discovery_ordinal);
        TranscriptFileSync::Synced
    }
}

#[test]
fn claude_resumable_scan_advances_only_after_synced_files() {
    let root = TempDir::new().expect("temporary transcript root");
    fs::write(
        root.path().join("a.jsonl"),
        "{\"timestamp\":\"2026-01-02T00:00:00Z\",\"text\":\"first\"}\n",
    )
    .expect("first fixture");
    fs::write(
        root.path().join("b.jsonl"),
        "{\"timestamp\":\"2026-01-02T00:00:01Z\",\"text\":\"second\"}\n",
    )
    .expect("second fixture");
    let reader = ClaudeJsonlRootReader::new(root.path().to_path_buf());
    let request = TranscriptScanRequest::new(0, *blake3::hash(b"configuration").as_bytes());
    let mut sink = SyncedFileSink::default();

    let first = reader.scan_records_resumable(&request, &mut sink);

    assert_eq!(sink.records, 2);
    assert_eq!(sink.begins, vec![0, 1]);
    assert_eq!(sink.completed, vec![0, 1]);
    assert_eq!(first.cursor.next_discovery_ordinal, 2);
    assert_eq!(first.completed_files, 2);

    let mut resumed_sink = SyncedFileSink::default();
    let second = reader.scan_records_resumable(
        &request.with_resume_cursor(Some(first.cursor)),
        &mut resumed_sink,
    );
    assert!(second.resumed);
    assert_eq!(
        resumed_sink.records, 0,
        "synced prefix must not be emitted twice"
    );
    assert_eq!(second.cursor.next_discovery_ordinal, 2);
}

#[test]
fn changed_completed_prefix_restarts_the_claude_scan() {
    let root = TempDir::new().expect("temporary transcript root");
    let path: PathBuf = root.path().join("a.jsonl");
    fs::write(
        &path,
        "{\"timestamp\":\"2026-01-02T00:00:00Z\",\"text\":\"first\"}\n",
    )
    .expect("fixture");
    let reader = ClaudeJsonlRootReader::new(root.path().to_path_buf());
    let request = TranscriptScanRequest::new(0, *blake3::hash(b"configuration").as_bytes());
    let mut first_sink = SyncedFileSink::default();
    let first = reader.scan_records_resumable(&request, &mut first_sink);

    fs::write(
        path,
        "{\"timestamp\":\"2026-01-02T00:00:00Z\",\"text\":\"changed evidence\"}\n",
    )
    .expect("mutated fixture");
    let mut restart_sink = SyncedFileSink::default();
    let restarted = reader.scan_records_resumable(
        &request.with_resume_cursor(Some(first.cursor)),
        &mut restart_sink,
    );

    assert!(!restarted.resumed);
    assert_eq!(restart_sink.records, 1);
    assert_eq!(restarted.cursor.source, SourceKind::Claude);
    assert_eq!(
        restarted.cursor.source_identifier,
        SourceIdentifier::new(format!("claude:{}", root.path().display()))
    );
}
