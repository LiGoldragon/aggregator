use std::fs;

use aggregator::{
    Error, IndexStoreError,
    output_index::{
        limits::IndexStoreLimits,
        schema::{IndexChunk, IndexFieldDto, IndexFileKind, IndexRecordDto},
        store::{ChunkReader, ChunkWriter, IndexLocator, IndexStore},
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

fn test_store(directory: &TempDir) -> IndexStore {
    IndexStore::new(
        directory.path().join("store.output-index.json"),
        IndexStoreLimits::default(),
    )
}

fn write_manifest(
    store: &IndexStore,
    name: &str,
) -> (aggregator::output_index::store::IndexStaging, IndexLocator) {
    let staging = store
        .create_staging(name)
        .expect("create staging generation");
    let manifest = IndexLocator::new("manifest");
    staging
        .write_chunk(&manifest, IndexFileKind::Manifest, &test_chunk())
        .expect("sync manifest");
    (staging, manifest)
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
    assert!(matches!(
        error,
        Error::IndexStore {
            source: IndexStoreError::InvalidChecksum
        }
    ));
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

#[test]
fn typed_chunk_rejects_oversize_before_reading_archive() {
    let directory = TempDir::new().expect("temporary directory");
    let path = directory.path().join("oversized");
    let limit = IndexStoreLimits::default().maximum_serialized_chunk_bytes;
    fs::write(&path, vec![0_u8; (limit + 55) as usize]).expect("write oversized fixture");
    let error = ChunkReader::new(path, IndexFileKind::Chunk, IndexStoreLimits::default())
        .read()
        .expect_err("metadata cap rejects oversized file");
    assert!(matches!(
        error,
        Error::IndexStore {
            source: IndexStoreError::OversizedEnvelope { .. }
        }
    ));
}

#[test]
fn typed_chunk_rejects_wrong_kind_and_version_before_decode() {
    let directory = TempDir::new().expect("temporary directory");
    let path = directory.path().join("chunk");
    ChunkWriter::new(
        path.clone(),
        IndexFileKind::Manifest,
        IndexStoreLimits::default(),
    )
    .write(&test_chunk())
    .expect("write manifest envelope");
    let wrong_kind = ChunkReader::new(
        path.clone(),
        IndexFileKind::Chunk,
        IndexStoreLimits::default(),
    )
    .read()
    .expect_err("wrong kind rejects before archive decode");
    assert!(matches!(
        wrong_kind,
        Error::IndexStore {
            source: IndexStoreError::UnexpectedFileKind { .. }
        }
    ));

    let mut bytes = fs::read(&path).expect("read envelope");
    bytes[9] = 99;
    fs::write(&path, bytes).expect("write unsupported version");
    let unsupported = ChunkReader::new(path, IndexFileKind::Manifest, IndexStoreLimits::default())
        .read()
        .expect_err("unsupported version rejects before archive decode");
    assert!(matches!(
        unsupported,
        Error::IndexStore {
            source: IndexStoreError::UnsupportedVersion { .. }
        }
    ));
}

#[test]
fn interrupted_generation_keeps_prior_pointer_and_manifest_readable() {
    let directory = TempDir::new().expect("temporary directory");
    let store = test_store(&directory);
    let (first, manifest) = write_manifest(&store, "first");
    let published = store
        .publish(&first, &manifest, [1; 32])
        .expect("publish first generation");

    let incomplete = store
        .create_staging("incomplete")
        .expect("stage interrupted generation");
    incomplete
        .write_chunk(
            &IndexLocator::new("chunk"),
            IndexFileKind::Chunk,
            &test_chunk(),
        )
        .expect("sync unrelated chunk");
    let interrupted = store
        .publish(&incomplete, &IndexLocator::new("manifest"), [2; 32])
        .expect_err("missing staged manifest is an interrupted publication");
    assert!(matches!(
        interrupted,
        Error::IndexStore {
            source: IndexStoreError::InterruptedPublication
        }
    ));

    assert_eq!(
        store.read_current_pointer().expect("read pointer"),
        Some(published.clone())
    );
    assert_eq!(
        store
            .open_reader(
                &IndexLocator::new(published.manifest_locator),
                IndexFileKind::Manifest,
            )
            .expect("open committed manifest")
            .read()
            .expect("read committed manifest"),
        test_chunk()
    );
}

#[test]
fn stale_concurrent_builder_cannot_replace_newer_pointer() {
    let directory = TempDir::new().expect("temporary directory");
    let store = test_store(&directory);
    let (first, first_manifest) = write_manifest(&store, "first");
    let (stale, stale_manifest) = write_manifest(&store, "stale");
    store
        .publish(&first, &first_manifest, [1; 32])
        .expect("publish first builder");
    let error = store
        .publish(&stale, &stale_manifest, [2; 32])
        .expect_err("stale builder must fail compare-and-swap");
    assert!(matches!(
        error,
        Error::IndexStore {
            source: IndexStoreError::WriterConflict
        }
    ));
    assert_eq!(
        store
            .read_current_pointer()
            .expect("read current pointer")
            .expect("current pointer")
            .snapshot_identity,
        [1; 32]
    );
}

#[test]
fn stale_pointer_temporary_does_not_block_unique_publication() {
    let directory = TempDir::new().expect("temporary directory");
    let store = test_store(&directory);
    fs::write(
        directory.path().join(".store.output-index.json.stale.tmp"),
        b"interrupted",
    )
    .expect("create stale temporary");
    let (staging, manifest) = write_manifest(&store, "new");
    store
        .publish(&staging, &manifest, [7; 32])
        .expect("unique temporary permits publication");
    assert!(store.pointer_path().is_file());
}

#[cfg(unix)]
#[test]
fn store_refuses_symlinked_parent_before_creating_typed_data() {
    use std::os::unix::fs::symlink;

    let directory = TempDir::new().expect("temporary directory");
    let outside = TempDir::new().expect("outside directory");
    let link = directory.path().join("redirect");
    symlink(outside.path(), &link).expect("create hostile parent symlink");
    let store = IndexStore::new(
        link.join("nested").join("store.output-index.json"),
        IndexStoreLimits::default(),
    );

    let error = store
        .create_staging("blocked")
        .expect_err("store must reject a symlinked parent before creation");
    assert!(matches!(
        error,
        Error::IndexStore {
            source: IndexStoreError::UnsafePath
        }
    ));
    assert!(!outside.path().join("nested").exists());
}

#[cfg(unix)]
#[test]
fn orphan_cleanup_rejects_symlinks_without_following_them() {
    use std::os::unix::fs::symlink;

    let directory = TempDir::new().expect("temporary directory");
    let store = test_store(&directory);
    let staging = store.create_staging("live").expect("create staging root");
    let outside = TempDir::new().expect("outside directory");
    let link = store.data_root().join("staging").join("escaped");
    symlink(outside.path(), &link).expect("create hostile symlink");

    let error = store
        .cleanup_orphans(&[staging.generation().clone()])
        .expect_err("cleanup must refuse hostile symlink");
    assert!(matches!(
        error,
        Error::IndexStore {
            source: IndexStoreError::UnsafePath
        }
    ));
    assert!(outside.path().is_dir());
    assert!(link.is_symlink());
}
