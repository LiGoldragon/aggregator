use aggregator::output_index::{
    limits::IndexStoreLimits,
    schema::*,
    store::{ChunkReader, ChunkWriter},
};
use tempfile::TempDir;

fn size() -> ProjectionSizeDto {
    ProjectionSizeDto {
        byte_count: Some(7),
        line_count: Some(1),
        segment_count: Some(1),
        certainty: 1,
    }
}
fn path() -> DiskPath {
    DiskPath::new(
        b"/synthetic/session".to_vec(),
        "/synthetic/session".to_owned(),
    )
}
fn records() -> Vec<ProjectionRecordDto> {
    vec![
        ProjectionRecordDto::Session(ProjectionSessionDto {
            reference: "session:v1:a".into(),
            source: 1,
            source_identifier: "synthetic".into(),
            path: path(),
            fingerprint_bytes: 7,
            fingerprint_seconds: 1,
            fingerprint_nanoseconds: 0,
            started_at: Some("2025-01-01T00:00:00Z".into()),
            last_observed_at: None,
            producer_session_identifier: None,
            subagent_count: 1,
            output_count: 1,
            size: size(),
        }),
        ProjectionRecordDto::Subagent(ProjectionSubagentDto {
            reference: "subagent:v1:a".into(),
            session_reference: "session:v1:a".into(),
            name: "synthetic".into(),
            authored_status: 1,
            task: Some(ProjectionTaskDto {
                task_identifier: "task".into(),
            }),
            output_count: 1,
            size: size(),
            first_observed_at: None,
            last_observed_at: None,
        }),
        ProjectionRecordDto::Output(ProjectionOutputDto {
            reference: "output:v1:a".into(),
            session_reference: "session:v1:a".into(),
            subagent_reference: Some("subagent:v1:a".into()),
            title: Some("synthetic".into()),
            task: None,
            source: 1,
            source_identifier: "synthetic".into(),
            authored_status: 1,
            produced_at: None,
            path: path(),
            fingerprint_bytes: 7,
            fingerprint_seconds: 1,
            fingerprint_nanoseconds: 0,
            source_line_number: 1,
            text_hash: "hash".into(),
            size: size(),
            preview_text: "bounded".into(),
            preview_original_bytes: 7,
        }),
        ProjectionRecordDto::Segment(ProjectionSegmentDto {
            reference: "segment:v1:a".into(),
            output_reference: "output:v1:a".into(),
            segment_index: 0,
            byte_range: Some((0, 7)),
            line_range: Some((1, 2)),
            size: size(),
            preview_text: "bounded".into(),
            preview_original_bytes: 7,
            source: 1,
            path: path(),
        }),
        ProjectionRecordDto::TranscriptBlock(ProjectionTranscriptBlockDto {
            reference: "block:v1:a".into(),
            session_reference: "session:v1:a".into(),
            subagent_reference: None,
            kind: 1,
            block_index: 0,
            task: None,
            source: 1,
            source_identifier: "synthetic".into(),
            authored_status: 1,
            observed_at: None,
            path: path(),
            fingerprint_bytes: 7,
            fingerprint_seconds: 1,
            fingerprint_nanoseconds: 0,
            source_line_number: 1,
            text_hash: "hash".into(),
            size: size(),
            text_availability: 1,
            preview_text: "bounded".into(),
            preview_original_bytes: 7,
        }),
    ]
}
#[test]
fn every_projection_card_kind_round_trips_as_rkyv_without_transcript_corpus() {
    let dir = TempDir::new().unwrap();
    for (ordinal, projection) in records().into_iter().enumerate() {
        let file = dir.path().join(ordinal.to_string());
        let chunk = IndexChunk {
            schema_version: TYPED_PROJECTION_DTO_VERSION,
            records: vec![],
            projection: Some(projection),
        };
        ChunkWriter::new(
            file.clone(),
            IndexFileKind::Projection,
            IndexStoreLimits::default(),
        )
        .write(&chunk)
        .unwrap();
        let read = ChunkReader::new(file, IndexFileKind::Projection, IndexStoreLimits::default())
            .read()
            .unwrap();
        assert_eq!(read, chunk);
    }
}
