use std::{fs, path::PathBuf};

use signal_aggregator::{
    ArchivePath, ArchiveRecordIdentifier, ArchiveSummaryText, ArchiveTextCompleteness, ByteCount,
    ByteLimit, OperationKind, OperationRejected, OperationRejectionReason, RequestIdentifier,
    SessionArchiveProvenanceProjection, SessionArchiveQueried, SessionArchiveQueryRequest,
    SessionArchiveRead, SessionArchiveReadRequest, SessionArchiveRecordCard,
    SessionArchiveRecordDraft, SessionArchiveRecordProjection, SessionArchiveTextProjection,
    SessionArchiveWriteRequest, SessionArchiveWritten,
};

use crate::output_index::{OperationRejectedFactory, OutputOperationResult};

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
        SessionArchiveRecordProjection {
            card: self.card(),
            session: self.draft.session.clone(),
            summary: SessionArchiveTextProjection {
                text: ArchiveSummaryText::new(
                    BoundedArchiveText::new(self.draft.summary.as_str(), summary_limit).text,
                ),
                byte_count: ByteCount::new(
                    BoundedArchiveText::new(self.draft.summary.as_str(), summary_limit).byte_count,
                ),
                completeness: BoundedArchiveText::new(self.draft.summary.as_str(), summary_limit)
                    .completeness,
            },
            provenance: SessionArchiveProvenanceProjection {
                text: signal_aggregator::ArchiveProvenanceText::new(
                    BoundedArchiveText::new(self.draft.provenance.as_str(), provenance_limit).text,
                ),
                byte_count: ByteCount::new(
                    BoundedArchiveText::new(self.draft.provenance.as_str(), provenance_limit)
                        .byte_count,
                ),
                completeness: BoundedArchiveText::new(
                    self.draft.provenance.as_str(),
                    provenance_limit,
                )
                .completeness,
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
    path: PathBuf,
}

impl SessionArchiveStore {
    pub fn new(path: ArchivePath) -> Self {
        Self {
            path: PathBuf::from(path.as_str()),
        }
    }

    pub fn write_record(
        &self,
        request: SessionArchiveWriteRequest,
    ) -> OutputOperationResult<SessionArchiveWritten> {
        let mut file = self.read_or_empty(
            &request.request_identifier,
            OperationKind::WriteSessionArchive,
        )?;
        let record_identifier = ArchiveRecordIdentifier::new(format!(
            "archive-record-{}-{}",
            request.record.session.reference.as_str(),
            request.record.created_at.as_str()
        ));
        let stored = SessionArchiveStoredRecord::new(record_identifier, request.record);
        let card = stored.card();
        file.push(stored);
        self.write_file(
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
        let file = self.read_existing(
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
        let file = self.read_existing(
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

    fn read_or_empty(
        &self,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<SessionArchiveFile> {
        if !self.path.exists() {
            return Ok(SessionArchiveFile::empty());
        }
        self.read_existing(request_identifier, operation)
    }

    fn read_existing(
        &self,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<SessionArchiveFile> {
        let bytes = fs::read(&self.path)
            .map_err(|_| self.archive_rejection(request_identifier, operation))?;
        rkyv::from_bytes::<SessionArchiveFile, rkyv::rancor::Error>(&bytes)
            .map_err(|_| self.archive_rejection(request_identifier, operation))
    }

    fn write_file(
        &self,
        file: &SessionArchiveFile,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .map_err(|_| self.archive_rejection(request_identifier, operation))?;
        }
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(file)
            .map_err(|_| self.archive_rejection(request_identifier, operation))?;
        let temporary_path = self.path.with_extension("tmp");
        fs::write(&temporary_path, bytes)
            .map_err(|_| self.archive_rejection(request_identifier, operation))?;
        fs::rename(&temporary_path, &self.path)
            .map_err(|_| self.archive_rejection(request_identifier, operation))?;
        Ok(())
    }

    fn archive_rejection(
        &self,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OperationRejected {
        OperationRejectedFactory::new(request_identifier.clone(), operation)
            .rejected(OperationRejectionReason::Unsupported, None)
    }
}
