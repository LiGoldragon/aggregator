//! Crash-safe, bounded typed chunk persistence.

use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;

use crate::{Error, Result, error::IndexStoreError};

use super::{
    limits::IndexStoreLimits,
    migration_v2::{IndexFormat, IndexFormatProbe},
    schema::{CurrentPointer, IndexChunk, IndexFileKind, IndexStoreFormatVersion},
};

const ENVELOPE_MAGIC: [u8; 8] = *b"AGGIDX03";
const ENVELOPE_BYTES: usize = 54;
const POINTER_SCHEMA_VERSION: u32 = 1;
const POINTER_MAGIC: [u8; 8] = *b"AGGIDX03";
static GENERATION_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Owns the compatibility pointer and its adjacent immutable typed data root.
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

    pub fn limits(&self) -> IndexStoreLimits {
        self.limits
    }

    /// The immutable copy of the v2 JSON evidence retained solely for migration rollback.
    pub fn migration_backup_path(&self) -> PathBuf {
        self.data_root.join("migration").join("v2-backup.json")
    }

    /// Streams v2 evidence into its one immutable rollback copy before the v3 pointer replaces it.
    /// A prior backup is deliberately retained rather than turned into an unbounded archive.
    pub fn retain_v2_backup(&self, source: &Path) -> Result<PathBuf> {
        self.ensure_root()?;
        let source_metadata = fs::symlink_metadata(source).map_err(|error| {
            Error::index_store(IndexStoreError::io("inspecting v2 migration source", error))
        })?;
        if source_metadata.file_type().is_symlink() || !source_metadata.file_type().is_file() {
            return Err(Error::index_store(IndexStoreError::UnsafePath));
        }
        let backup = self.migration_backup_path();
        match fs::symlink_metadata(&backup) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
                    return Err(Error::index_store(IndexStoreError::UnsafePath));
                }
                return Ok(backup);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(Error::index_store(IndexStoreError::io(
                    "inspecting v2 migration backup",
                    error,
                )));
            }
        }
        let temporary =
            backup.with_extension(format!("{}.tmp", UniqueGeneration::new("backup").as_str()));
        let copy = File::open(source)
            .and_then(|mut input| {
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&temporary)
                    .and_then(|mut output| {
                        std::io::copy(&mut input, &mut output)?;
                        output.sync_all()
                    })
            })
            .map_err(|error| {
                Error::index_store(IndexStoreError::io("streaming v2 migration backup", error))
            });
        if let Err(error) = copy {
            let _ = fs::remove_file(&temporary);
            return Err(error);
        }
        fs::rename(&temporary, &backup).map_err(|error| {
            Error::index_store(IndexStoreError::io("committing v2 migration backup", error))
        })?;
        self.sync_directory(backup.parent())?;
        Ok(backup)
    }

    /// Creates a unique, unlinked generation. The base pointer hash is retained for CAS publication.
    pub fn create_staging(&self, nonce: &str) -> Result<IndexStaging> {
        let prefix = IndexLocator::new(nonce);
        prefix.validate_component()?;
        self.ensure_root()?;
        let base_pointer_identity = self.pointer_identity()?;
        let staging_root = self.data_root.join("staging");
        for _ in 0..16 {
            let generation = UniqueGeneration::new(prefix.as_str()).locator();
            let path = staging_root.join(generation.as_str());
            match fs::create_dir(&path) {
                Ok(()) => {
                    self.sync_directory(Some(&staging_root))?;
                    return Ok(IndexStaging {
                        store: self.clone(),
                        path,
                        generation,
                        base_pointer_identity,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(Error::index_store(IndexStoreError::io(
                        "creating index staging generation",
                        error,
                    )));
                }
            }
        }
        Err(Error::index_store(IndexStoreError::WriterConflict))
    }

    pub fn open_reader(&self, locator: &IndexLocator, kind: IndexFileKind) -> Result<ChunkReader> {
        let path = self.chunk_path(locator)?;
        self.verify_regular_file(&path)?;
        Ok(ChunkReader::new(path, kind, self.limits))
    }

    /// Classifies the current pointer from a bounded prefix before a migration attempts to read it.
    /// The JSON v2 file remains untouched until a complete v3 generation atomically replaces it.
    pub fn current_format(&self) -> Result<IndexFormat> {
        let Some(bytes) = self.read_pointer_bytes()? else {
            return Ok(IndexFormat::Missing);
        };
        Ok(IndexFormatProbe::new(&bytes[..bytes.len().min(4096)]).format())
    }

    /// Reads the typed compatibility pointer without decoding any referenced manifest.
    pub fn read_current_pointer(&self) -> Result<Option<CurrentPointer>> {
        let Some(bytes) = self.read_pointer_bytes()? else {
            return Ok(None);
        };
        if bytes.len() as u64 > self.limits.maximum_manifest_bytes {
            return Err(Error::index_store(IndexStoreError::OversizedEnvelope {
                kind: "current pointer",
                bytes: bytes.len() as u64,
                limit: self.limits.maximum_manifest_bytes,
            }));
        }
        let archived = bytes
            .strip_prefix(&POINTER_MAGIC)
            .ok_or_else(|| Error::index_store(IndexStoreError::CorruptArchive))?;
        let pointer = rkyv::from_bytes::<CurrentPointer, rkyv::rancor::Error>(archived)
            .map_err(|_| Error::index_store(IndexStoreError::CorruptArchive))?;
        if pointer.schema_version != POINTER_SCHEMA_VERSION
            || pointer.format_version != IndexStoreFormatVersion::V3Chunked as u32
        {
            return Err(Error::index_store(IndexStoreError::UnsupportedVersion {
                version: pointer.format_version,
            }));
        }
        IndexLocator::new(pointer.manifest_locator.clone()).validate()?;
        Ok(Some(pointer))
    }

    /// Opens the manifest selected by a validated current pointer. Runtime readers navigate only
    /// the manifest roots; generation directories are never a query surface.
    pub fn read_manifest_chunk(&self, pointer: &CurrentPointer) -> Result<IndexChunk> {
        let locator = IndexLocator::new(pointer.manifest_locator.clone());
        self.open_reader(&locator, IndexFileKind::Manifest)?.read()
    }

    /// Publishes a fully synced staging generation if its captured base pointer is still current.
    pub fn publish(
        &self,
        staging: &IndexStaging,
        manifest: &IndexLocator,
        snapshot_identity: [u8; 32],
    ) -> Result<CurrentPointer> {
        staging.belongs_to(self)?;
        manifest.validate_component()?;
        let manifest_path = staging.path.join(manifest.as_str());
        match fs::symlink_metadata(&manifest_path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::index_store(IndexStoreError::InterruptedPublication));
            }
            Err(error) => {
                return Err(Error::index_store(IndexStoreError::io(
                    "inspecting staged manifest",
                    error,
                )));
            }
            Ok(_) => staging.verify_regular_file(&manifest_path)?,
        }
        ChunkReader::new(manifest_path, IndexFileKind::Manifest, self.limits).read()?;

        let lock = self.lock()?;
        let publication = self.publish_locked(staging, manifest, snapshot_identity);
        let unlock = FileExt::unlock(&lock).map_err(|error| {
            Error::index_store(IndexStoreError::io("unlocking typed index", error))
        });
        match publication {
            Ok(pointer) => unlock.map(|()| pointer),
            Err(error) => {
                let _ = unlock;
                Err(error)
            }
        }
    }

    /// Backward-compatible publication entry point. The manifest checksum is the snapshot identity.
    pub fn publish_pointer(&self, staging: &IndexStaging, manifest: &IndexLocator) -> Result<()> {
        let manifest_path = staging.path.join(manifest.as_str());
        staging.verify_regular_file(&manifest_path)?;
        let identity = *blake3::hash(&fs::read(&manifest_path).map_err(|error| {
            Error::index_store(IndexStoreError::io(
                "reading staged manifest identity",
                error,
            ))
        })?)
        .as_bytes();
        self.publish(staging, manifest, identity).map(|_| ())
    }

    /// Removes only stale staging generations. It never follows a symlink or deletes a published generation.
    pub fn cleanup_orphans(&self, active_staging: &[IndexLocator]) -> Result<()> {
        self.ensure_root()?;
        let active = active_staging
            .iter()
            .map(|locator| {
                locator.validate_component()?;
                Ok(locator.as_str().to_owned())
            })
            .collect::<Result<BTreeSet<_>>>()?;
        let staging_root = self.data_root.join("staging");
        self.verify_directory(&staging_root)?;

        // Select the newest names with a fixed-size set instead of materializing every stale
        // generation. A crash loop must not turn reclamation itself into a corpus-sized allocation.
        let retained = self.limits.staging_generations_retained as usize;
        let mut retained_names = BTreeSet::new();
        for entry in fs::read_dir(&staging_root).map_err(|error| {
            Error::index_store(IndexStoreError::io("reading staging root", error))
        })? {
            let entry = entry.map_err(|error| {
                Error::index_store(IndexStoreError::io("reading staging entry", error))
            })?;
            let name = entry.file_name().to_string_lossy().into_owned();
            IndexLocator::new(name.clone()).validate_component()?;
            let kind = entry.file_type().map_err(|error| {
                Error::index_store(IndexStoreError::io("reading staging entry type", error))
            })?;
            if kind.is_symlink() || !kind.is_dir() {
                return Err(Error::index_store(IndexStoreError::UnsafePath));
            }
            self.verify_tree_without_symlink(&entry.path())?;
            retained_names.insert(name);
            if retained_names.len() > retained {
                retained_names.pop_first();
            }
        }

        for entry in fs::read_dir(&staging_root).map_err(|error| {
            Error::index_store(IndexStoreError::io("reading staging root", error))
        })? {
            let entry = entry.map_err(|error| {
                Error::index_store(IndexStoreError::io("reading staging entry", error))
            })?;
            let name = entry.file_name().to_string_lossy().into_owned();
            IndexLocator::new(name.clone()).validate_component()?;
            if active.contains(&name) || retained_names.contains(&name) {
                continue;
            }
            // Revalidate immediately before removal so a replacement cannot introduce a link
            // after the first safety pass.
            self.verify_tree_without_symlink(&entry.path())?;
            fs::remove_dir_all(entry.path()).map_err(|error| {
                Error::index_store(IndexStoreError::io(
                    "removing stale staging generation",
                    error,
                ))
            })?;
        }
        self.sync_directory(Some(&staging_root))
    }

    pub fn lock(&self) -> Result<File> {
        self.ensure_root()?;
        let path = self.data_root.join("lock");
        self.verify_regular_or_missing_file(&path)?;
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

    fn publish_locked(
        &self,
        staging: &IndexStaging,
        manifest: &IndexLocator,
        snapshot_identity: [u8; 32],
    ) -> Result<CurrentPointer> {
        if self.pointer_identity()? != staging.base_pointer_identity {
            return Err(Error::index_store(IndexStoreError::WriterConflict));
        }
        self.verify_directory(&staging.path)?;
        let generation_path = self
            .data_root
            .join("generations")
            .join(staging.generation.as_str());
        if generation_path.exists() {
            return Err(Error::index_store(IndexStoreError::WriterConflict));
        }
        self.sync_directory(Some(&staging.path))?;
        fs::rename(&staging.path, &generation_path).map_err(|error| {
            Error::index_store(IndexStoreError::io(
                "committing typed index generation",
                error,
            ))
        })?;
        self.sync_directory(Some(&self.data_root.join("generations")))?;
        self.sync_directory(Some(&self.data_root.join("staging")))?;

        let pointer = CurrentPointer {
            schema_version: POINTER_SCHEMA_VERSION,
            format_version: IndexStoreFormatVersion::V3Chunked as u32,
            manifest_locator: format!(
                "generations/{}/{}",
                staging.generation.as_str(),
                manifest.as_str()
            ),
            snapshot_identity,
        };
        self.write_pointer(&pointer)?;
        Ok(pointer)
    }

    fn write_pointer(&self, pointer: &CurrentPointer) -> Result<()> {
        let archived = rkyv::to_bytes::<rkyv::rancor::Error>(pointer).map_err(|error| {
            Error::index_store(IndexStoreError::Serialization {
                detail: error.to_string(),
            })
        })?;
        let mut bytes = POINTER_MAGIC.to_vec();
        bytes.extend_from_slice(&archived);
        if bytes.len() as u64 > self.limits.maximum_manifest_bytes {
            return Err(Error::index_store(IndexStoreError::OversizedEnvelope {
                kind: "current pointer",
                bytes: bytes.len() as u64,
                limit: self.limits.maximum_manifest_bytes,
            }));
        }
        let parent = self
            .pointer_path
            .parent()
            .ok_or_else(|| Error::index_store(IndexStoreError::UnsafePath))?;
        self.ensure_directory(parent)?;
        let temporary = self.unique_pointer_temporary(parent)?;
        let result = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .and_then(|mut file| file.write_all(&bytes).and_then(|_| file.sync_all()))
            .map_err(|error| {
                Error::index_store(IndexStoreError::io("syncing typed current pointer", error))
            });
        if let Err(error) = result {
            let _ = fs::remove_file(&temporary);
            return Err(error);
        }
        fs::rename(&temporary, &self.pointer_path).map_err(|error| {
            Error::index_store(IndexStoreError::io(
                "publishing typed current pointer",
                error,
            ))
        })?;
        self.sync_directory(Some(parent))
    }

    fn unique_pointer_temporary(&self, parent: &Path) -> Result<PathBuf> {
        let name = self
            .pointer_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| Error::index_store(IndexStoreError::UnsafePath))?;
        for _ in 0..16 {
            let candidate = parent.join(format!(
                ".{name}.{}.tmp",
                UniqueGeneration::new("pointer").as_str()
            ));
            if !candidate.exists() {
                return Ok(candidate);
            }
        }
        Err(Error::index_store(IndexStoreError::WriterConflict))
    }

    fn pointer_identity(&self) -> Result<Option<[u8; 32]>> {
        self.read_pointer_bytes()
            .map(|bytes| bytes.map(|bytes| *blake3::hash(&bytes).as_bytes()))
    }

    fn read_pointer_bytes(&self) -> Result<Option<Vec<u8>>> {
        match fs::symlink_metadata(&self.pointer_path) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
                    return Err(Error::index_store(IndexStoreError::UnsafePath));
                }
                if metadata.len() > self.limits.maximum_manifest_bytes {
                    return Err(Error::index_store(IndexStoreError::OversizedEnvelope {
                        kind: "current pointer",
                        bytes: metadata.len(),
                        limit: self.limits.maximum_manifest_bytes,
                    }));
                }
                let mut bytes = Vec::with_capacity(metadata.len() as usize);
                File::open(&self.pointer_path)
                    .and_then(|mut file| file.read_to_end(&mut bytes))
                    .map_err(|error| {
                        Error::index_store(IndexStoreError::io(
                            "reading typed current pointer",
                            error,
                        ))
                    })?;
                if bytes.len() as u64 > self.limits.maximum_manifest_bytes {
                    return Err(Error::index_store(IndexStoreError::OversizedEnvelope {
                        kind: "current pointer",
                        bytes: bytes.len() as u64,
                        limit: self.limits.maximum_manifest_bytes,
                    }));
                }
                Ok(Some(bytes))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(Error::index_store(IndexStoreError::io(
                "reading typed current pointer metadata",
                error,
            ))),
        }
    }

    fn ensure_root(&self) -> Result<()> {
        let parent = self
            .pointer_path
            .parent()
            .ok_or_else(|| Error::index_store(IndexStoreError::UnsafePath))?;
        self.ensure_directory(parent)?;
        self.ensure_directory(&self.data_root)?;
        for name in [
            "manifests",
            "generations",
            "snapshots",
            "staging",
            "migration",
        ] {
            self.ensure_directory(&self.data_root.join(name))?;
        }
        Ok(())
    }

    fn ensure_directory(&self, path: &Path) -> Result<()> {
        let absolute = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|error| {
                    Error::index_store(IndexStoreError::io(
                        "resolving typed index directory",
                        error,
                    ))
                })?
                .join(path)
        };
        let mut current = PathBuf::new();
        for component in absolute.components() {
            match component {
                Component::Prefix(prefix) => current.push(prefix.as_os_str()),
                Component::RootDir => current.push(component.as_os_str()),
                Component::CurDir => {}
                Component::ParentDir => {
                    return Err(Error::index_store(IndexStoreError::UnsafePath));
                }
                Component::Normal(component) => {
                    current.push(component);
                    match fs::symlink_metadata(&current) {
                        Ok(metadata)
                            if metadata.file_type().is_symlink()
                                || !metadata.file_type().is_dir() =>
                        {
                            return Err(Error::index_store(IndexStoreError::UnsafePath));
                        }
                        Ok(_) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            fs::create_dir(&current).map_err(|error| {
                                Error::index_store(IndexStoreError::io(
                                    "creating typed index directory",
                                    error,
                                ))
                            })?;
                        }
                        Err(error) => {
                            return Err(Error::index_store(IndexStoreError::io(
                                "inspecting typed index directory",
                                error,
                            )));
                        }
                    }
                }
            }
        }
        self.verify_directory(&absolute)
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
        let relative = candidate
            .strip_prefix(&self.data_root)
            .map_err(|_| Error::index_store(IndexStoreError::UnsafePath))?;
        let mut current = self.data_root.clone();
        for component in relative.components() {
            let Component::Normal(component) = component else {
                return Err(Error::index_store(IndexStoreError::UnsafePath));
            };
            current.push(component);
            if current.exists() {
                let metadata = fs::symlink_metadata(&current).map_err(|error| {
                    Error::index_store(IndexStoreError::io("inspecting typed index path", error))
                })?;
                if metadata.file_type().is_symlink() {
                    return Err(Error::index_store(IndexStoreError::UnsafePath));
                }
            }
        }
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

    fn verify_directory(&self, path: &Path) -> Result<()> {
        self.verify_no_symlink_ancestor(path)?;
        let metadata = fs::symlink_metadata(path).map_err(|error| {
            Error::index_store(IndexStoreError::io(
                "inspecting typed index directory",
                error,
            ))
        })?;
        if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
            return Err(Error::index_store(IndexStoreError::UnsafePath));
        }
        Ok(())
    }

    fn verify_no_symlink_ancestor(&self, path: &Path) -> Result<()> {
        for ancestor in path.ancestors() {
            match fs::symlink_metadata(ancestor) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(Error::index_store(IndexStoreError::UnsafePath));
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(Error::index_store(IndexStoreError::io(
                        "inspecting typed index path ancestor",
                        error,
                    )));
                }
            }
        }
        Ok(())
    }

    fn verify_tree_without_symlink(&self, root: &Path) -> Result<()> {
        let metadata = fs::symlink_metadata(root).map_err(|error| {
            Error::index_store(IndexStoreError::io(
                "inspecting stale staging generation",
                error,
            ))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(Error::index_store(IndexStoreError::UnsafePath));
        }
        if metadata.file_type().is_dir() {
            for entry in fs::read_dir(root).map_err(|error| {
                Error::index_store(IndexStoreError::io(
                    "reading stale staging generation",
                    error,
                ))
            })? {
                let entry = entry.map_err(|error| {
                    Error::index_store(IndexStoreError::io("reading stale staging entry", error))
                })?;
                self.verify_tree_without_symlink(&entry.path())?;
            }
        }
        Ok(())
    }

    fn verify_regular_file(&self, path: &Path) -> Result<()> {
        self.verify_containment(path)?;
        let metadata = fs::symlink_metadata(path).map_err(|error| {
            Error::index_store(IndexStoreError::io("inspecting typed index file", error))
        })?;
        if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
            return Err(Error::index_store(IndexStoreError::UnsafePath));
        }
        Ok(())
    }

    fn verify_regular_or_missing_file(&self, path: &Path) -> Result<()> {
        match fs::symlink_metadata(path) {
            Ok(metadata)
                if metadata.file_type().is_symlink() || !metadata.file_type().is_file() =>
            {
                Err(Error::index_store(IndexStoreError::UnsafePath))
            }
            Ok(_) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(Error::index_store(IndexStoreError::io(
                "inspecting typed index file",
                error,
            ))),
        }
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

/// A generation that is complete only after `IndexStore::publish` atomically replaces the pointer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexStaging {
    store: IndexStore,
    path: PathBuf,
    generation: IndexLocator,
    base_pointer_identity: Option<[u8; 32]>,
}

impl IndexStaging {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn generation(&self) -> &IndexLocator {
        &self.generation
    }

    /// Reopens one immutable staging chunk while an external sorter constructs its fixed-fanout
    /// publication tree. Callers retain only the decoded chunk they are presently merging.
    pub fn read_chunk(&self, locator: &IndexLocator, kind: IndexFileKind) -> Result<IndexChunk> {
        locator.validate_component()?;
        let path = self.path.join(locator.as_str());
        self.verify_regular_file(&path)?;
        ChunkReader::new(path, kind, self.store.limits).read()
    }

    pub fn write_chunk(
        &self,
        locator: &IndexLocator,
        kind: IndexFileKind,
        chunk: &IndexChunk,
    ) -> Result<()> {
        locator.validate_component()?;
        self.store.verify_directory(&self.path)?;
        let path = self.path.join(locator.as_str());
        ChunkWriter::new(path, kind, self.store.limits).write(chunk)?;
        self.store.sync_directory(Some(&self.path))
    }

    /// Replaces a staging checkpoint atomically; a partially written checkpoint is never visible.
    pub fn replace_checkpoint(&self, locator: &IndexLocator, chunk: &IndexChunk) -> Result<()> {
        locator.validate_component()?;
        let temporary = self.path.join(format!(
            ".{}.{}.tmp",
            locator.as_str(),
            UniqueGeneration::new("checkpoint").as_str()
        ));
        ChunkWriter::new(
            temporary.clone(),
            IndexFileKind::Checkpoint,
            self.store.limits,
        )
        .write(chunk)?;
        fs::rename(&temporary, self.path.join(locator.as_str())).map_err(|error| {
            Error::index_store(IndexStoreError::io("replacing typed checkpoint", error))
        })?;
        self.store.sync_directory(Some(&self.path))
    }

    fn belongs_to(&self, store: &IndexStore) -> Result<()> {
        if &self.store != store || !self.path.starts_with(store.data_root.join("staging")) {
            return Err(Error::index_store(IndexStoreError::UnsafePath));
        }
        store.verify_containment(&self.path)
    }

    fn verify_regular_file(&self, path: &Path) -> Result<()> {
        self.store.verify_regular_file(path)
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
        if self.value.is_empty()
            || path.is_absolute()
            || path
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(Error::index_store(IndexStoreError::UnsafePath));
        }
        Ok(())
    }

    fn validate_component(&self) -> Result<()> {
        self.validate()?;
        if Path::new(&self.value).components().count() != 1 {
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
        ChunkBounds::new(chunk, self.limits).validate()?;
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
        let write_result = IndexEnvelope::new(self.kind, archived.as_ref())
            .write_to(&mut file)
            .and_then(|_| {
                file.sync_all().map_err(|error| {
                    Error::index_store(IndexStoreError::io("syncing typed chunk", error))
                })
            });
        if let Err(error) = write_result {
            let _ = fs::remove_file(&self.path);
            return Err(error);
        }
        Ok(())
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
        if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
            return Err(Error::index_store(IndexStoreError::UnsafePath));
        }
        let maximum_file_bytes = self.limits.maximum_serialized_chunk_bytes + ENVELOPE_BYTES as u64;
        if metadata.len() > maximum_file_bytes {
            return Err(Error::index_store(IndexStoreError::OversizedEnvelope {
                kind: "typed chunk file",
                bytes: metadata.len(),
                limit: maximum_file_bytes,
            }));
        }
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        File::open(&self.path)
            .and_then(|mut file| {
                Read::by_ref(&mut file)
                    .take(maximum_file_bytes + 1)
                    .read_to_end(&mut bytes)
            })
            .map_err(|error| {
                Error::index_store(IndexStoreError::io("reading typed chunk", error))
            })?;
        if bytes.len() as u64 > maximum_file_bytes {
            return Err(Error::index_store(IndexStoreError::OversizedEnvelope {
                kind: "typed chunk file",
                bytes: bytes.len() as u64,
                limit: maximum_file_bytes,
            }));
        }
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
            .and_then(|_| output.write_all(&POINTER_SCHEMA_VERSION.to_le_bytes()))
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
            return Err(Error::index_store(IndexStoreError::UnexpectedFileKind {
                expected: expected_kind as u8,
                actual: kind as u8,
            }));
        }
        let format_version = IndexStoreFormatVersion::from_u8(bytes[9]).ok_or_else(|| {
            Error::index_store(IndexStoreError::UnsupportedVersion {
                version: bytes[9] as u32,
            })
        })?;
        let schema_version =
            u32::from_le_bytes(bytes[10..14].try_into().expect("fixed envelope header"));
        if format_version != IndexStoreFormatVersion::V3Chunked
            || schema_version != POINTER_SCHEMA_VERSION
        {
            return Err(Error::index_store(IndexStoreError::UnsupportedVersion {
                version: bytes[9] as u32,
            }));
        }
        let length = u64::from_le_bytes(bytes[14..22].try_into().expect("fixed envelope header"));
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
struct ChunkBounds<'a> {
    chunk: &'a IndexChunk,
    limits: IndexStoreLimits,
}

impl<'a> ChunkBounds<'a> {
    fn new(chunk: &'a IndexChunk, limits: IndexStoreLimits) -> Self {
        Self { chunk, limits }
    }

    fn validate(&self) -> Result<()> {
        let record_count = self.chunk.records.len() as u64;
        let mut logical_bytes = 0_u64;
        for record in &self.chunk.records {
            let mut record_bytes = 0_u64;
            for field in &record.fields {
                if field.name.len() as u64 > self.limits.maximum_string_bytes {
                    return Err(Error::index_store(IndexStoreError::OversizedString));
                }
                let field_bytes = (field.name.len() + field.bytes.len()) as u64;
                record_bytes = record_bytes.saturating_add(field_bytes);
                logical_bytes = logical_bytes.saturating_add(field_bytes);
            }
            if record_bytes > self.limits.maximum_record_bytes {
                return Err(Error::index_store(IndexStoreError::OversizedRecord));
            }
        }
        if !self.limits.accepts_chunk(logical_bytes, record_count) {
            return Err(Error::index_store(IndexStoreError::OversizedEnvelope {
                kind: "chunk",
                bytes: logical_bytes,
                limit: self.limits.maximum_logical_chunk_bytes,
            }));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UniqueGeneration {
    value: String,
}

impl UniqueGeneration {
    fn new(prefix: &str) -> Self {
        let sequence = GENERATION_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Self {
            value: format!("{prefix}-{}-{nanos}-{sequence}", std::process::id()),
        }
    }

    fn locator(&self) -> IndexLocator {
        IndexLocator::new(self.value.clone())
    }

    fn as_str(&self) -> &str {
        &self.value
    }
}
