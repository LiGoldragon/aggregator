use std::fs;

use aggregator::{
    Error,
    output_index::{
        limits::IndexStoreLimits,
        schema::{IndexChunk, IndexFieldDto, IndexFileKind, IndexRecordDto},
        store::{ChunkReader, ChunkWriter},
    },
};
use tempfile::TempDir;

fn test_chunk() -> IndexChunk {
    IndexChunk {
        schema_version: 1,
        records: vec![IndexRecordDto {
            schema_version: 1,
            record_kind: 1,
            fields: vec![IndexFieldDto {
                name: "reference".to_string(),
                bytes: b"opaque-reference".to_vec(),
            }],
        }],
    }
}

#[test]
fn typed_chunk_rejects_corruption_before_archive_decode() {
    let directory = TempDir::new().expect("temporary directory");
    let path = directory.path().join("chunk");
    ChunkWriter::new(
        path.clone(),
        IndexFileKind::Chunk,
        IndexStoreLimits::default(),
    )
    .write(&test_chunk())
    .expect("write typed chunk");
    let mut bytes = fs::read(&path).expect("read typed chunk");
    *bytes.last_mut().expect("payload byte") ^= 0xff;
    fs::write(&path, bytes).expect("corrupt typed chunk");
    let error = ChunkReader::new(path, IndexFileKind::Chunk, IndexStoreLimits::default())
        .read()
        .expect_err("checksum must reject before archive decode");
    assert!(matches!(error, Error::IndexStore { .. }));
}

#[test]
fn typed_chunk_round_trips_one_bounded_chunk() {
    let directory = TempDir::new().expect("temporary directory");
    let path = directory.path().join("chunk");
    let chunk = test_chunk();
    ChunkWriter::new(
        path.clone(),
        IndexFileKind::Chunk,
        IndexStoreLimits::default(),
    )
    .write(&chunk)
    .expect("write typed chunk");
    assert_eq!(
        ChunkReader::new(path, IndexFileKind::Chunk, IndexStoreLimits::default())
            .read()
            .expect("read typed chunk"),
        chunk
    );
}
