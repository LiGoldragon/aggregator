//! Crash-safe, bounded typed chunk persistence.

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
};

use fs2::FileExt;

use crate::{Error, Result, error::IndexStoreError};

use super::{
    limits::IndexStoreLimits,
    schema::{IndexChunk, IndexFileKind, IndexStoreFormatVersion},
};

const ENVELOPE_MAGIC: [u8; 8] = *b"AGGIDX03";
const ENVELOPE_BYTES: usize = 54;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexStore {
    pointer_path: PathBuf,
    data_root: PathBuf,
    limits: IndexStoreLimits,
}

impl IndexStore {
    pub fn new(pointer_path: PathBuf, limits: IndexStoreLimits) -> Self {
        let data_root = PathBuf::from(format!("{}.d", pointer_path.display()));
        Self {
            pointer_path,
            data_root,
            limits,
        }
    }

    pub fn pointer_path(&self) -> &Path {
        &self.pointer_path
    }

    pub fn data_root(&self) -> &Path {
        &self.data_root
    }

    pub fn create_staging(&self, nonce: &str) -> Result<IndexStaging> {
        let locator = IndexLocator::new(nonce);
        locator.validate()?;
        self.ensure_root()?;
        let path = self.data_root.join("staging").join(locator.as_str());
        fs::create_dir_all(self.data_root.join("staging")).map_err(|error| {
            Error::index_store(IndexStoreError::io("creating index staging root", error))
        })?;
        fs::create_dir(&path).map_err(|error| {
            Error::index_store(IndexStoreError::io("creating index staging", error))
        })?;
        Ok(IndexStaging {
            store: self.clone(),
            path,
        })
    }

    pub fn open_reader(&self, locator: &IndexLocator, kind: IndexFileKind) -> Result<ChunkReader> {
        let path = self.chunk_path(locator)?;
        Ok(ChunkReader::new(path, kind, self.limits))
    }

    pub fn publish_pointer(&self, staging: &IndexStaging, manifest: &IndexLocator) -> Result<()> {
        manifest.validate()?;
        let lock = self.lock()?;
        let manifest_path = staging.path.join(manifest.as_str());
        if !manifest_path.is_file() {
            return Err(Error::index_store(IndexStoreError::InterruptedPublication));
        }
        self.ensure_root()?;
        let temporary = self.pointer_path.with_extension("output-index.pointer.tmp");
        let mut temporary_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|error| {
                Error::index_store(IndexStoreError::io("creating typed pointer", error))
            })?;
        temporary_file
            .write_all(manifest.as_str().as_bytes())
            .and_then(|_| temporary_file.sync_all())
            .map_err(|error| {
                Error::index_store(IndexStoreError::io("syncing typed pointer", error))
            })?;
        fs::rename(&temporary, &self.pointer_path).map_err(|error| {
            Error::index_store(IndexStoreError::io("publishing typed pointer", error))
        })?;
        self.sync_directory(self.pointer_path.parent())?;
        lock.unlock().map_err(|error| {
            Error::index_store(IndexStoreError::io("unlocking typed index", error))
        })?;
        Ok(())
    }

    pub fn lock(&self) -> Result<File> {
        self.ensure_root()?;
        let path = self.data_root.join("lock");
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|error| {
                Error::index_store(IndexStoreError::io("opening typed index lock", error))
            })?;
        file.try_lock_exclusive()
            .map_err(|_| Error::index_store(IndexStoreError::WriterConflict))?;
        Ok(file)
    }

    fn ensure_root(&self) -> Result<()> {
        fs::create_dir_all(&self.data_root).map_err(|error| {
            Error::index_store(IndexStoreError::io("creating typed index root", error))
        })
    }

    fn chunk_path(&self, locator: &IndexLocator) -> Result<PathBuf> {
        locator.validate()?;
        let path = self.data_root.join(locator.as_str());
        self.verify_containment(&path)?;
        Ok(path)
    }

    fn verify_containment(&self, candidate: &Path) -> Result<()> {
        let root = self.data_root.canonicalize().map_err(|error| {
            Error::index_store(IndexStoreError::io(
                "canonicalizing typed index root",
                error,
            ))
        })?;
        let parent = candidate
            .parent()
            .ok_or_else(|| Error::index_store(IndexStoreError::UnsafePath))?;
        let parent = parent.canonicalize().map_err(|error| {
            Error::index_store(IndexStoreError::io(
                "canonicalizing typed index parent",
                error,
            ))
        })?;
        if !parent.starts_with(root) {
            return Err(Error::index_store(IndexStoreError::UnsafePath));
        }
        Ok(())
    }

    fn sync_directory(&self, directory: Option<&Path>) -> Result<()> {
        let Some(directory) = directory else {
            return Ok(());
        };
        File::open(directory)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| {
                Error::index_store(IndexStoreError::io("syncing typed index directory", error))
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexStaging {
    store: IndexStore,
    path: PathBuf,
}

impl IndexStaging {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn write_chunk(
        &self,
        locator: &IndexLocator,
        kind: IndexFileKind,
        chunk: &IndexChunk,
    ) -> Result<()> {
        locator.validate()?;
        self.store.verify_containment(&self.path)?;
        let path = self.path.join(locator.as_str());
        ChunkWriter::new(path, kind, self.store.limits).write(chunk)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexLocator {
    value: String,
}

impl IndexLocator {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }

    pub fn validate(&self) -> Result<()> {
        let path = Path::new(&self.value);
        if path.is_absolute()
            || path
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(Error::index_store(IndexStoreError::UnsafePath));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkWriter {
    path: PathBuf,
    kind: IndexFileKind,
    limits: IndexStoreLimits,
}

impl ChunkWriter {
    pub fn new(path: PathBuf, kind: IndexFileKind, limits: IndexStoreLimits) -> Self {
        Self { path, kind, limits }
    }

    pub fn write(&self, chunk: &IndexChunk) -> Result<()> {
        let record_count = chunk.records.len() as u64;
        let logical_bytes = ChunkLogicalBytes::new(chunk).count();
        if !self.limits.accepts_chunk(logical_bytes, record_count) {
            return Err(Error::index_store(IndexStoreError::OversizedEnvelope {
                kind: "chunk",
                bytes: logical_bytes,
                limit: self.limits.maximum_logical_chunk_bytes,
            }));
        }
        let archived = rkyv::to_bytes::<rkyv::rancor::Error>(chunk).map_err(|error| {
            Error::index_store(IndexStoreError::Serialization {
                detail: error.to_string(),
            })
        })?;
        if !self.limits.accepts_serialized_chunk(archived.len() as u64) {
            return Err(Error::index_store(IndexStoreError::OversizedEnvelope {
                kind: "serialized chunk",
                bytes: archived.len() as u64,
                limit: self.limits.maximum_serialized_chunk_bytes,
            }));
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.path)
            .map_err(|error| {
                Error::index_store(IndexStoreError::io("creating typed chunk", error))
            })?;
        IndexEnvelope::new(self.kind, archived.as_ref()).write_to(&mut file)?;
        file.sync_all()
            .map_err(|error| Error::index_store(IndexStoreError::io("syncing typed chunk", error)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkReader {
    path: PathBuf,
    expected_kind: IndexFileKind,
    limits: IndexStoreLimits,
}

impl ChunkReader {
    pub fn new(path: PathBuf, expected_kind: IndexFileKind, limits: IndexStoreLimits) -> Self {
        Self {
            path,
            expected_kind,
            limits,
        }
    }

    pub fn read(&self) -> Result<IndexChunk> {
        let metadata = fs::symlink_metadata(&self.path).map_err(|error| {
            Error::index_store(IndexStoreError::io("reading typed chunk metadata", error))
        })?;
        if metadata.file_type().is_symlink()
            || metadata.len() > self.limits.maximum_serialized_chunk_bytes + ENVELOPE_BYTES as u64
        {
            return Err(Error::index_store(IndexStoreError::UnsafePath));
        }
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        File::open(&self.path)
            .and_then(|mut file| file.read_to_end(&mut bytes))
            .map_err(|error| {
                Error::index_store(IndexStoreError::io("reading typed chunk", error))
            })?;
        let payload = IndexEnvelope::from_bytes(&bytes, self.expected_kind, self.limits)?;
        rkyv::from_bytes::<IndexChunk, rkyv::rancor::Error>(payload)
            .map_err(|_| Error::index_store(IndexStoreError::CorruptArchive))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEnvelope {
    kind: IndexFileKind,
    payload: Vec<u8>,
}

impl IndexEnvelope {
    pub fn new(kind: IndexFileKind, payload: &[u8]) -> Self {
        Self {
            kind,
            payload: payload.to_vec(),
        }
    }

    pub fn write_to(&self, output: &mut File) -> Result<()> {
        let payload_length = self.payload.len() as u64;
        output
            .write_all(&ENVELOPE_MAGIC)
            .and_then(|_| output.write_all(&[self.kind as u8]))
            .and_then(|_| output.write_all(&[IndexStoreFormatVersion::V3Chunked as u8]))
            .and_then(|_| output.write_all(&1_u32.to_le_bytes()))
            .and_then(|_| output.write_all(&payload_length.to_le_bytes()))
            .and_then(|_| output.write_all(blake3::hash(&self.payload).as_bytes()))
            .and_then(|_| output.write_all(&self.payload))
            .map_err(|error| {
                Error::index_store(IndexStoreError::io("writing typed chunk envelope", error))
            })
    }

    pub fn from_bytes(
        bytes: &[u8],
        expected_kind: IndexFileKind,
        limits: IndexStoreLimits,
    ) -> Result<&[u8]> {
        if bytes.len() < ENVELOPE_BYTES || bytes[..8] != ENVELOPE_MAGIC {
            return Err(Error::index_store(IndexStoreError::CorruptArchive));
        }
        let kind = IndexFileKind::from_u8(bytes[8]).ok_or_else(|| {
            Error::index_store(IndexStoreError::UnsupportedVersion {
                version: bytes[8] as u32,
            })
        })?;
        if kind != expected_kind {
            return Err(Error::index_store(IndexStoreError::CorruptArchive));
        }
        let version = IndexStoreFormatVersion::from_u8(bytes[9]).ok_or_else(|| {
            Error::index_store(IndexStoreError::UnsupportedVersion {
                version: bytes[9] as u32,
            })
        })?;
        if version != IndexStoreFormatVersion::V3Chunked
            || u32::from_le_bytes(bytes[10..14].try_into().expect("header length")) != 1
        {
            return Err(Error::index_store(IndexStoreError::UnsupportedVersion {
                version: bytes[9] as u32,
            }));
        }
        let length = u64::from_le_bytes(bytes[14..22].try_into().expect("header length"));
        if !limits.accepts_serialized_chunk(length)
            || length as usize + ENVELOPE_BYTES != bytes.len()
        {
            return Err(Error::index_store(IndexStoreError::OversizedEnvelope {
                kind: "chunk envelope",
                bytes: length,
                limit: limits.maximum_serialized_chunk_bytes,
            }));
        }
        let payload = &bytes[ENVELOPE_BYTES..];
        if blake3::hash(payload).as_bytes() != &bytes[22..54] {
            return Err(Error::index_store(IndexStoreError::InvalidChecksum));
        }
        Ok(payload)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ChunkLogicalBytes<'a> {
    chunk: &'a IndexChunk,
}

impl<'a> ChunkLogicalBytes<'a> {
    pub fn new(chunk: &'a IndexChunk) -> Self {
        Self { chunk }
    }

    pub fn count(&self) -> u64 {
        self.chunk
            .records
            .iter()
            .flat_map(|record| record.fields.iter())
            .map(|field| (field.name.len() + field.bytes.len()) as u64)
            .sum()
    }
}
