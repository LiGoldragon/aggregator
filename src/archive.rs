use std::{
    collections::HashSet,
    fs::{self, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
};

use signal_aggregator::{
    ArchivePath, ArchiveRecordIdentifier, ArchiveSummaryText, ArchiveTextCompleteness, ByteCount,
    ByteLimit, OperationKind, OperationRejected, OperationRejectionReason, RequestIdentifier,
    SessionArchiveProvenanceProjection, SessionArchiveQueried, SessionArchiveQueryRequest,
    SessionArchiveRead, SessionArchiveReadRequest, SessionArchiveRecordCard,
    SessionArchiveRecordDraft, SessionArchiveRecordProjection, SessionArchiveTextProjection,
    SessionArchiveWriteRequest, SessionArchiveWritten,
};

use crate::output_index::{OperationRejectedFactory, OutputOperationResult};

pub const MAXIMUM_ARCHIVE_FILE_BYTES: u64 = 8 * 1024 * 1024;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SessionArchiveFile {
    version: u32,
    records: Vec<SessionArchiveStoredRecord>,
}

impl SessionArchiveFile {
    pub fn empty() -> Self {
        Self {
            version: 1,
            records: Vec::new(),
        }
    }

    pub fn records(&self) -> &[SessionArchiveStoredRecord] {
        &self.records
    }

    pub fn push(&mut self, record: SessionArchiveStoredRecord) {
        self.records.push(record);
    }

    pub fn next_record_identifier(&self) -> ArchiveRecordIdentifier {
        let mut sequence = self.records.len() as u64 + 1;
        loop {
            let candidate = ArchiveRecordIdentifier::new(format!("archive-record-{sequence:016}"));
            if !self.contains_record_identifier(&candidate) {
                return candidate;
            }
            sequence += 1;
        }
    }

    pub fn contains_record_identifier(&self, candidate: &ArchiveRecordIdentifier) -> bool {
        self.records
            .iter()
            .any(|record| record.matches_record_identifier(candidate))
    }

    pub fn has_unique_record_identifiers(&self) -> bool {
        let mut observed = HashSet::new();
        self.records
            .iter()
            .all(|record| observed.insert(record.record_identifier.as_str()))
    }
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SessionArchiveStoredRecord {
    record_identifier: ArchiveRecordIdentifier,
    draft: SessionArchiveRecordDraft,
}

impl SessionArchiveStoredRecord {
    pub fn new(
        record_identifier: ArchiveRecordIdentifier,
        draft: SessionArchiveRecordDraft,
    ) -> Self {
        Self {
            record_identifier,
            draft,
        }
    }

    pub fn card(&self) -> SessionArchiveRecordCard {
        SessionArchiveRecordCard {
            record_identifier: self.record_identifier.clone(),
            session_reference: self.draft.session.reference.clone(),
            source: self.draft.session.source,
            source_identifier: self.draft.session.source_identifier.clone(),
            producer_session_identifier: self.draft.session.producer_session_identifier.clone(),
            created_at: self.draft.created_at.clone(),
            summary_bytes: ByteCount::new(self.draft.summary.as_str().len() as u64),
            provenance_bytes: ByteCount::new(self.draft.provenance.as_str().len() as u64),
        }
    }

    pub fn matches_record_identifier(&self, record_identifier: &ArchiveRecordIdentifier) -> bool {
        &self.record_identifier == record_identifier
    }

    pub fn matches_session_reference(
        &self,
        reference: &signal_aggregator::FragileSessionReference,
    ) -> bool {
        &self.draft.session.reference == reference
    }

    pub fn projection(
        &self,
        summary_limit: ByteLimit,
        provenance_limit: ByteLimit,
    ) -> SessionArchiveRecordProjection {
        let summary = BoundedArchiveText::new(self.draft.summary.as_str(), summary_limit);
        let provenance = BoundedArchiveText::new(self.draft.provenance.as_str(), provenance_limit);
        SessionArchiveRecordProjection {
            card: self.card(),
            session: self.draft.session.clone(),
            summary: SessionArchiveTextProjection {
                text: ArchiveSummaryText::new(summary.text),
                byte_count: ByteCount::new(summary.byte_count),
                completeness: summary.completeness,
            },
            provenance: SessionArchiveProvenanceProjection {
                text: signal_aggregator::ArchiveProvenanceText::new(provenance.text),
                byte_count: ByteCount::new(provenance.byte_count),
                completeness: provenance.completeness,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedArchiveText {
    text: String,
    byte_count: u64,
    completeness: ArchiveTextCompleteness,
}

impl BoundedArchiveText {
    pub fn new(text: &str, limit: ByteLimit) -> Self {
        let limit = limit.into_u64() as usize;
        if text.len() <= limit {
            return Self {
                text: text.to_string(),
                byte_count: text.len() as u64,
                completeness: ArchiveTextCompleteness::Complete,
            };
        }
        let mut end = limit;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        Self {
            text: text[..end].to_string(),
            byte_count: end as u64,
            completeness: ArchiveTextCompleteness::Truncated,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionArchiveStore {
    archive_root: PathBuf,
    archive_path: ArchivePath,
}

impl SessionArchiveStore {
    pub fn new(archive_root: impl Into<PathBuf>, path: ArchivePath) -> Self {
        Self {
            archive_root: archive_root.into(),
            archive_path: path,
        }
    }

    pub fn write_record(
        &self,
        request: SessionArchiveWriteRequest,
    ) -> OutputOperationResult<SessionArchiveWritten> {
        let path = self.validated_path(
            &request.request_identifier,
            OperationKind::WriteSessionArchive,
            ArchivePathAccess::CreateRoot,
        )?;
        let mut file = path.read_or_empty(
            &request.request_identifier,
            OperationKind::WriteSessionArchive,
        )?;
        let record_identifier = file.next_record_identifier();
        let stored = SessionArchiveStoredRecord::new(record_identifier, request.record);
        let card = stored.card();
        file.push(stored);
        path.write_file(
            &file,
            &request.request_identifier,
            OperationKind::WriteSessionArchive,
        )?;
        Ok(SessionArchiveWritten {
            request_identifier: request.request_identifier,
            archive_path: request.archive_path,
            card,
        })
    }

    pub fn query(
        &self,
        request: SessionArchiveQueryRequest,
    ) -> OutputOperationResult<SessionArchiveQueried> {
        let path = self.validated_path(
            &request.request_identifier,
            OperationKind::QuerySessionArchive,
            ArchivePathAccess::ExistingRoot,
        )?;
        let file = path.read_existing(
            &request.request_identifier,
            OperationKind::QuerySessionArchive,
        )?;
        let records = file
            .records()
            .iter()
            .filter(|record| {
                request
                    .session_reference
                    .as_ref()
                    .is_none_or(|reference| record.matches_session_reference(reference))
            })
            .map(SessionArchiveStoredRecord::card)
            .collect();
        Ok(SessionArchiveQueried {
            request_identifier: request.request_identifier,
            archive_path: request.archive_path,
            records,
        })
    }

    pub fn read(
        &self,
        request: SessionArchiveReadRequest,
    ) -> OutputOperationResult<SessionArchiveRead> {
        let path = self.validated_path(
            &request.request_identifier,
            OperationKind::ReadSessionArchive,
            ArchivePathAccess::ExistingRoot,
        )?;
        let file = path.read_existing(
            &request.request_identifier,
            OperationKind::ReadSessionArchive,
        )?;
        let Some(record) = file
            .records()
            .iter()
            .find(|record| record.matches_record_identifier(&request.record_identifier))
        else {
            return Err(OperationRejectedFactory::new(
                request.request_identifier.clone(),
                OperationKind::ReadSessionArchive,
            )
            .rejected(OperationRejectionReason::Missing, None));
        };
        Ok(SessionArchiveRead {
            request_identifier: request.request_identifier,
            archive_path: request.archive_path,
            record: record.projection(
                request.maximum_summary_bytes,
                request.maximum_provenance_bytes,
            ),
        })
    }

    pub fn validated_path(
        &self,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
        access: ArchivePathAccess,
    ) -> OutputOperationResult<ArchiveFilePath> {
        ArchivePathBoundary::new(self.archive_root.clone(), self.archive_path.clone())
            .validated_file_path(request_identifier, operation, access)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchivePathAccess {
    ExistingRoot,
    CreateRoot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivePathBoundary {
    root: PathBuf,
    requested: ArchivePath,
}

impl ArchivePathBoundary {
    pub fn new(root: PathBuf, requested: ArchivePath) -> Self {
        Self { root, requested }
    }

    pub fn validated_file_path(
        &self,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
        access: ArchivePathAccess,
    ) -> OutputOperationResult<ArchiveFilePath> {
        let requested = PathBuf::from(self.requested.as_str());
        if requested.as_os_str().is_empty() {
            return Err(self.rejection(
                request_identifier,
                operation,
                OperationRejectionReason::InvalidRequest,
            ));
        }
        if requested.is_absolute() || self.has_forbidden_component(&requested) {
            return Err(self.rejection(
                request_identifier,
                operation,
                OperationRejectionReason::Unauthorized,
            ));
        }

        let root = self.canonical_root(request_identifier, operation, access)?;
        let candidate = root.join(requested);
        let Some(parent) = candidate.parent() else {
            return Err(self.rejection(
                request_identifier,
                operation,
                OperationRejectionReason::InvalidRequest,
            ));
        };
        if !parent.starts_with(&root) {
            return Err(self.rejection(
                request_identifier,
                operation,
                OperationRejectionReason::Unauthorized,
            ));
        }
        if self.has_symbolic_link(&candidate) {
            return Err(self.rejection(
                request_identifier,
                operation,
                OperationRejectionReason::Unauthorized,
            ));
        }
        if matches!(access, ArchivePathAccess::CreateRoot) {
            fs::create_dir_all(parent).map_err(|_| {
                self.rejection(
                    request_identifier,
                    operation,
                    OperationRejectionReason::Unsupported,
                )
            })?;
        }
        let parent = parent.canonicalize().map_err(|error| {
            self.filesystem_rejection(request_identifier, operation, error.kind())
        })?;
        if !parent.starts_with(&root) {
            return Err(self.rejection(
                request_identifier,
                operation,
                OperationRejectionReason::Unauthorized,
            ));
        }
        let Some(file_name) = candidate.file_name() else {
            return Err(self.rejection(
                request_identifier,
                operation,
                OperationRejectionReason::InvalidRequest,
            ));
        };
        let path = parent.join(file_name);
        if let Ok(metadata) = fs::symlink_metadata(&path) {
            if metadata.file_type().is_symlink() {
                return Err(self.rejection(
                    request_identifier,
                    operation,
                    OperationRejectionReason::Unauthorized,
                ));
            }
            if metadata.is_dir() {
                return Err(self.rejection(
                    request_identifier,
                    operation,
                    OperationRejectionReason::InvalidRequest,
                ));
            }
            let canonical = path.canonicalize().map_err(|error| {
                self.filesystem_rejection(request_identifier, operation, error.kind())
            })?;
            if !canonical.starts_with(&root) {
                return Err(self.rejection(
                    request_identifier,
                    operation,
                    OperationRejectionReason::Unauthorized,
                ));
            }
            return Ok(ArchiveFilePath::new(root, canonical));
        }
        Ok(ArchiveFilePath::new(root, path))
    }

    pub fn canonical_root(
        &self,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
        access: ArchivePathAccess,
    ) -> OutputOperationResult<PathBuf> {
        if matches!(access, ArchivePathAccess::CreateRoot) {
            fs::create_dir_all(&self.root).map_err(|_| {
                self.rejection(
                    request_identifier,
                    operation,
                    OperationRejectionReason::Unsupported,
                )
            })?;
        }
        self.root
            .canonicalize()
            .map_err(|error| self.filesystem_rejection(request_identifier, operation, error.kind()))
    }

    pub fn has_forbidden_component(&self, path: &Path) -> bool {
        path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::Prefix(_) | Component::RootDir
            )
        })
    }

    pub fn has_symbolic_link(&self, path: &Path) -> bool {
        path.ancestors().any(|ancestor| {
            fs::symlink_metadata(ancestor)
                .map(|metadata| metadata.file_type().is_symlink())
                .unwrap_or(false)
        })
    }

    pub fn filesystem_rejection(
        &self,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
        kind: std::io::ErrorKind,
    ) -> OperationRejected {
        let reason = match kind {
            std::io::ErrorKind::NotFound => OperationRejectionReason::Missing,
            std::io::ErrorKind::PermissionDenied => OperationRejectionReason::Unauthorized,
            _ => OperationRejectionReason::Unsupported,
        };
        self.rejection(request_identifier, operation, reason)
    }

    pub fn rejection(
        &self,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
        reason: OperationRejectionReason,
    ) -> OperationRejected {
        OperationRejectedFactory::new(request_identifier.clone(), operation).rejected(reason, None)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveFilePath {
    root: PathBuf,
    path: PathBuf,
}

impl ArchiveFilePath {
    pub fn new(root: PathBuf, path: PathBuf) -> Self {
        Self { root, path }
    }

    pub fn read_or_empty(
        &self,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<SessionArchiveFile> {
        if !self.path.exists() {
            return Ok(SessionArchiveFile::empty());
        }
        self.read_existing(request_identifier, operation)
    }

    pub fn read_existing(
        &self,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<SessionArchiveFile> {
        self.reject_oversized(request_identifier, operation)?;
        let bytes = fs::read(&self.path).map_err(|error| {
            self.filesystem_rejection(request_identifier, operation, error.kind())
        })?;
        let file =
            rkyv::from_bytes::<SessionArchiveFile, rkyv::rancor::Error>(&bytes).map_err(|_| {
                self.rejection(
                    request_identifier,
                    operation,
                    OperationRejectionReason::InvalidRequest,
                )
            })?;
        if !file.has_unique_record_identifiers() {
            return Err(self.rejection(
                request_identifier,
                operation,
                OperationRejectionReason::InvalidRequest,
            ));
        }
        Ok(file)
    }

    pub fn write_file(
        &self,
        file: &SessionArchiveFile,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<()> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(file).map_err(|_| {
            self.rejection(
                request_identifier,
                operation,
                OperationRejectionReason::Unsupported,
            )
        })?;
        if bytes.len() as u64 > MAXIMUM_ARCHIVE_FILE_BYTES {
            return Err(self.rejection(
                request_identifier,
                operation,
                OperationRejectionReason::Oversized,
            ));
        }
        ArchiveTemporaryPath::new(self.root.clone(), self.path.clone()).write_and_replace(
            bytes.as_ref(),
            request_identifier,
            operation,
        )
    }

    pub fn reject_oversized(
        &self,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<()> {
        let metadata = fs::metadata(&self.path).map_err(|error| {
            self.filesystem_rejection(request_identifier, operation, error.kind())
        })?;
        if metadata.len() > MAXIMUM_ARCHIVE_FILE_BYTES {
            return Err(self.rejection(
                request_identifier,
                operation,
                OperationRejectionReason::Oversized,
            ));
        }
        Ok(())
    }

    pub fn filesystem_rejection(
        &self,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
        kind: std::io::ErrorKind,
    ) -> OperationRejected {
        let reason = match kind {
            std::io::ErrorKind::NotFound => OperationRejectionReason::Missing,
            std::io::ErrorKind::PermissionDenied => OperationRejectionReason::Unauthorized,
            _ => OperationRejectionReason::Unsupported,
        };
        self.rejection(request_identifier, operation, reason)
    }

    pub fn rejection(
        &self,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
        reason: OperationRejectionReason,
    ) -> OperationRejected {
        OperationRejectedFactory::new(request_identifier.clone(), operation).rejected(reason, None)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveTemporaryPath {
    root: PathBuf,
    path: PathBuf,
}

impl ArchiveTemporaryPath {
    pub fn new(root: PathBuf, path: PathBuf) -> Self {
        Self { root, path }
    }

    pub fn path(&self) -> PathBuf {
        let file_name = self
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("archive.rkyv");
        self.path
            .with_file_name(format!(".{file_name}.{}.tmp", std::process::id()))
    }

    pub fn write_and_replace(
        &self,
        bytes: &[u8],
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<()> {
        let temporary_path = self.path();
        self.validate(request_identifier, operation, &temporary_path)?;
        let mut temporary_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary_path)
            .map_err(|error| {
                self.filesystem_rejection(request_identifier, operation, error.kind())
            })?;
        temporary_file.write_all(bytes).map_err(|error| {
            self.filesystem_rejection(request_identifier, operation, error.kind())
        })?;
        temporary_file.sync_all().map_err(|error| {
            self.filesystem_rejection(request_identifier, operation, error.kind())
        })?;
        fs::rename(&temporary_path, &self.path).map_err(|error| {
            self.filesystem_rejection(request_identifier, operation, error.kind())
        })?;
        Ok(())
    }

    pub fn validate(
        &self,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
        temporary_path: &Path,
    ) -> OutputOperationResult<()> {
        let Some(parent) = temporary_path.parent() else {
            return Err(self.rejection(
                request_identifier,
                operation,
                OperationRejectionReason::InvalidRequest,
            ));
        };
        let parent = parent.canonicalize().map_err(|error| {
            self.filesystem_rejection(request_identifier, operation, error.kind())
        })?;
        if !parent.starts_with(&self.root) || !temporary_path.starts_with(&parent) {
            return Err(self.rejection(
                request_identifier,
                operation,
                OperationRejectionReason::Unauthorized,
            ));
        }
        if let Ok(metadata) = fs::symlink_metadata(temporary_path) {
            if metadata.file_type().is_symlink() {
                return Err(self.rejection(
                    request_identifier,
                    operation,
                    OperationRejectionReason::Unauthorized,
                ));
            }
            return Err(self.rejection(
                request_identifier,
                operation,
                OperationRejectionReason::InvalidRequest,
            ));
        }
        Ok(())
    }

    pub fn filesystem_rejection(
        &self,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
        kind: std::io::ErrorKind,
    ) -> OperationRejected {
        let reason = match kind {
            std::io::ErrorKind::AlreadyExists => OperationRejectionReason::Unauthorized,
            std::io::ErrorKind::NotFound => OperationRejectionReason::Missing,
            std::io::ErrorKind::PermissionDenied => OperationRejectionReason::Unauthorized,
            _ => OperationRejectionReason::Unsupported,
        };
        self.rejection(request_identifier, operation, reason)
    }

    pub fn rejection(
        &self,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
        reason: OperationRejectionReason,
    ) -> OperationRejected {
        OperationRejectedFactory::new(request_identifier.clone(), operation).rejected(reason, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    use signal_aggregator::{
        ArchiveProvenanceText, FilesystemPath, FragileSessionReference, ItemCount,
        RootRelativePath, SessionArchiveStatus, SessionIdentifier, SessionInventoryCard,
        SessionLifecycleStatus, SessionRole, SourceHealthStatus, SourceIdentifier, SourceKind,
        SourceLocator,
    };
    use tempfile::TempDir;

    fn archive_draft(session_reference: &str) -> SessionArchiveRecordDraft {
        SessionArchiveRecordDraft {
            session: SessionInventoryCard {
                reference: FragileSessionReference::new(session_reference),
                role: SessionRole::MainSession,
                source: SourceKind::Claude,
                source_identifier: SourceIdentifier::new("claude-fixture"),
                producer_session_identifier: Some(SessionIdentifier::new("producer-session")),
                locator: SourceLocator {
                    root: FilesystemPath::new("/tmp/archive-test"),
                    relative_path: Some(RootRelativePath::new("session.jsonl")),
                },
                file_count: ItemCount::new(1),
                byte_count: ByteCount::new(12),
                earliest_modified_at: None,
                latest_modified_at: None,
                started_at: None,
                last_observed_at: None,
                subagent_count: None,
                output_count: None,
                lifecycle_status: SessionLifecycleStatus::Current,
                source_status: SourceHealthStatus::ReadableIndexed,
                archive_status: SessionArchiveStatus::ArchiveUnknown,
            },
            summary: ArchiveSummaryText::new("summary"),
            provenance: ArchiveProvenanceText::new("provenance"),
            created_at: signal_aggregator::Timestamp::new("2026-01-02T01:10:00Z"),
        }
    }

    fn write_archive_file(path: &Path, file: &SessionArchiveFile) {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(file).expect("serialize archive file");
        fs::write(path, bytes).expect("write archive file");
    }

    fn duplicate_archive_file(record_identifier: ArchiveRecordIdentifier) -> SessionArchiveFile {
        let mut file = SessionArchiveFile::empty();
        file.push(SessionArchiveStoredRecord::new(
            record_identifier.clone(),
            archive_draft("session-ref-1"),
        ));
        file.push(SessionArchiveStoredRecord::new(
            record_identifier,
            archive_draft("session-ref-2"),
        ));
        file
    }

    #[test]
    fn absolute_archive_write_rejects_before_creating_outside_parent() {
        let temporary_root = TempDir::new().expect("temporary root");
        let archive_root = temporary_root.path().join("session-archive");
        let outside_parent = temporary_root.path().join("outside-parent");
        let outside_path = outside_parent.join("archive.rkyv");
        let store = SessionArchiveStore::new(
            archive_root,
            ArchivePath::new(outside_path.display().to_string()),
        );

        let rejected = store
            .write_record(SessionArchiveWriteRequest {
                request_identifier: RequestIdentifier::new("write-outside-archive-root"),
                archive_path: ArchivePath::new(outside_path.display().to_string()),
                record: archive_draft("session-ref"),
            })
            .expect_err("absolute outside archive path must be rejected");

        assert_eq!(rejected.reason, OperationRejectionReason::Unauthorized);
        assert!(
            !outside_parent.exists(),
            "outside archive parent directory must not be created before rejection"
        );
    }

    #[test]
    fn archive_write_rejects_preexisting_temporary_symlink_without_touching_target() {
        let temporary_root = TempDir::new().expect("temporary root");
        let archive_root = temporary_root.path().join("session-archive");
        fs::create_dir_all(&archive_root).expect("archive root");
        let archive_path = archive_root.join("archive.rkyv");
        let temporary_path = ArchiveTemporaryPath::new(
            archive_root.canonicalize().expect("canonical root"),
            archive_path.clone(),
        )
        .path();
        let victim_path = temporary_root.path().join("victim.txt");
        fs::write(&victim_path, "victim").expect("victim content");
        symlink(&victim_path, &temporary_path).expect("temporary symlink");
        let store = SessionArchiveStore::new(archive_root, ArchivePath::new("archive.rkyv"));

        let rejected = store
            .write_record(SessionArchiveWriteRequest {
                request_identifier: RequestIdentifier::new("write-temp-symlink-archive"),
                archive_path: ArchivePath::new("archive.rkyv"),
                record: archive_draft("session-ref"),
            })
            .expect_err("temporary symlink must be rejected");

        assert_eq!(rejected.reason, OperationRejectionReason::Unauthorized);
        assert_eq!(
            fs::read_to_string(&victim_path).expect("victim unchanged"),
            "victim"
        );
        assert!(
            !archive_path.exists(),
            "archive file must not be installed from a symlinked temporary path"
        );
    }

    #[test]
    fn duplicate_archive_record_identifiers_reject_query_read_and_write() {
        let temporary_root = TempDir::new().expect("temporary root");
        let archive_root = temporary_root.path().join("session-archive");
        fs::create_dir_all(&archive_root).expect("archive root");
        let archive_path = archive_root.join("archive.rkyv");
        let duplicate_identifier = ArchiveRecordIdentifier::new("duplicate-record");
        write_archive_file(
            &archive_path,
            &duplicate_archive_file(duplicate_identifier.clone()),
        );
        let store = SessionArchiveStore::new(archive_root, ArchivePath::new("archive.rkyv"));

        let query_rejected = store
            .query(SessionArchiveQueryRequest {
                request_identifier: RequestIdentifier::new("query-duplicate-archive"),
                archive_path: ArchivePath::new("archive.rkyv"),
                session_reference: None,
            })
            .expect_err("duplicate identifiers must reject query");
        assert_eq!(
            query_rejected.reason,
            OperationRejectionReason::InvalidRequest
        );

        let read_rejected = store
            .read(SessionArchiveReadRequest {
                request_identifier: RequestIdentifier::new("read-duplicate-archive"),
                archive_path: ArchivePath::new("archive.rkyv"),
                record_identifier: duplicate_identifier,
                maximum_summary_bytes: ByteLimit::new(64),
                maximum_provenance_bytes: ByteLimit::new(64),
            })
            .expect_err("duplicate identifiers must reject read");
        assert_eq!(
            read_rejected.reason,
            OperationRejectionReason::InvalidRequest
        );

        let write_rejected = store
            .write_record(SessionArchiveWriteRequest {
                request_identifier: RequestIdentifier::new("write-duplicate-archive"),
                archive_path: ArchivePath::new("archive.rkyv"),
                record: archive_draft("session-ref-3"),
            })
            .expect_err("duplicate identifiers must reject write");
        assert_eq!(
            write_rejected.reason,
            OperationRejectionReason::InvalidRequest
        );
    }
}
