//! Bounded observation-run construction for v3 source generations.
//!
//! This module intentionally writes normalized observations before reducing them. A source run
//! never owns a session's children: downstream reducers join by `SourceKey` and scalar keys.

use signal_aggregator::{SourceIdentifier, SourceKind};

use crate::{
    Error, Result, RuntimeConfiguration, TranscriptAdapterConfiguration,
    adapter::{
        TranscriptRawReadOutcome, TranscriptRecord, TranscriptRecordSink, TranscriptScanRequest,
        claude::ClaudeJsonlRootReader, codex::CodexSessionRootReader, pi::PiRunHistoryRootReader,
    },
};

use super::{
    instrumentation::{IndexReservation, IndexResourceMeter, IndexWorkCategory},
    limits::IndexStoreLimits,
    schema::{CurrentPointer, IndexChunk, IndexFieldDto, IndexFileKind, IndexRecordDto},
    store::{IndexLocator, IndexStaging, IndexStore},
};

/// A configured source is distinct from another occurrence of the same root.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourceKey {
    source_kind: String,
    source_identifier: String,
    configured_occurrence: u64,
}

impl SourceKey {
    pub fn new(
        source_kind: SourceKind,
        source_identifier: SourceIdentifier,
        configured_occurrence: u64,
    ) -> Self {
        Self {
            source_kind: format!("{source_kind:?}"),
            source_identifier: source_identifier.as_str().to_owned(),
            configured_occurrence,
        }
    }

    pub fn configured_occurrence(&self) -> u64 {
        self.configured_occurrence
    }

    pub fn signature(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.source_kind.as_bytes());
        hasher.update(&[0]);
        hasher.update(self.source_identifier.as_bytes());
        hasher.update(&self.configured_occurrence.to_le_bytes());
        *hasher.finalize().as_bytes()
    }

    fn fields(&self) -> [IndexFieldDto; 3] {
        [
            IndexFieldDto {
                name: "source-kind".to_owned(),
                bytes: self.source_kind.as_bytes().to_vec(),
            },
            IndexFieldDto {
                name: "source-identifier".to_owned(),
                bytes: self.source_identifier.as_bytes().to_vec(),
            },
            IndexFieldDto {
                name: "configured-occurrence".to_owned(),
                bytes: self.configured_occurrence.to_le_bytes().to_vec(),
            },
        ]
    }
}

/// Immutable, synced observation chunks for exactly one configured source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceGenerationRun {
    pub source_key: SourceKey,
    /// Chunk names are deterministic from this source occurrence and ordinal; no child locator
    /// vector is retained in a source summary.
    pub chunk_count: u64,
    pub logical_bytes: u64,
    pub record_count: u64,
    pub content_identity: [u8; 32],
}

/// The immutable publication result contains only scalar source facts and opaque chunk locators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedGenerationPublication {
    pub pointer: CurrentPointer,
    pub source_runs: Vec<SourceGenerationRun>,
    pub scan_outcomes: Vec<TranscriptRawReadOutcome>,
    pub resource_high_water_bytes: u64,
}

/// Owns one refresh lifecycle. It scans every configured source once, writes synced runs and a
/// checkpoint after each source, then publishes a single typed pointer.
#[derive(Debug, Clone)]
pub struct BoundedIndexRefresher {
    configuration: RuntimeConfiguration,
    store: IndexStore,
    limits: IndexStoreLimits,
    meter: IndexResourceMeter,
}

impl BoundedIndexRefresher {
    pub fn new(
        configuration: RuntimeConfiguration,
        store: IndexStore,
        limits: IndexStoreLimits,
        meter: IndexResourceMeter,
    ) -> Self {
        Self {
            configuration,
            store,
            limits,
            meter,
        }
    }

    pub fn refresh(&self) -> Result<BoundedGenerationPublication> {
        let staging = self.store.create_staging("bounded-build")?;
        let mut source_runs = Vec::new();
        let mut scan_outcomes = Vec::new();
        for (occurrence, source) in self.configuration.transcript_sources().iter().enumerate() {
            let source_key = SourceKey::new(
                source.kind(),
                self.source_identifier(source),
                occurrence as u64,
            );
            let mut builder = BoundedGenerationBuilder::new(
                staging.clone(),
                source_key.clone(),
                self.limits,
                self.meter.clone(),
            );
            let request =
                TranscriptScanRequest::new(occurrence as u64, self.configuration_signature(source));
            let resumable = self.scan_source(source, &request, &mut builder);
            let run = builder.finish()?;
            self.write_checkpoint(&staging, occurrence as u64, &resumable.cursor, &run)?;
            scan_outcomes.push(resumable.outcome);
            source_runs.push(run);
        }
        let snapshot_identity = SnapshotIdentity::new(&source_runs).value();
        let manifest = IndexManifestRecord::new(&source_runs).chunk();
        let manifest_locator = IndexLocator::new("manifest");
        staging.write_chunk(&manifest_locator, IndexFileKind::Manifest, &manifest)?;
        let pointer = self
            .store
            .publish(&staging, &manifest_locator, snapshot_identity)?;
        Ok(BoundedGenerationPublication {
            pointer,
            source_runs,
            scan_outcomes,
            resource_high_water_bytes: self.meter.snapshot().high_water_bytes,
        })
    }

    fn scan_source(
        &self,
        source: &TranscriptAdapterConfiguration,
        request: &TranscriptScanRequest,
        sink: &mut BoundedGenerationBuilder,
    ) -> crate::adapter::TranscriptResumableScanOutcome {
        match source {
            TranscriptAdapterConfiguration::Claude(root) => ClaudeJsonlRootReader::with_limits(
                root.path().to_path_buf(),
                root.scan_limits().clone(),
            )
            .scan_records_resumable(request, sink),
            TranscriptAdapterConfiguration::ClaudeSubagentOutput(root) => {
                ClaudeJsonlRootReader::with_limits_and_source(
                    root.path().to_path_buf(),
                    root.scan_limits().clone(),
                    signal_aggregator::SourceKind::ClaudeSubagentOutput,
                )
                .scan_records_resumable(request, sink)
            }
            TranscriptAdapterConfiguration::PiSubagentOutput(root) => {
                ClaudeJsonlRootReader::with_limits_and_source(
                    root.path().to_path_buf(),
                    root.scan_limits().clone(),
                    signal_aggregator::SourceKind::PiSubagentOutput,
                )
                .scan_records_resumable(request, sink)
            }
            TranscriptAdapterConfiguration::Codex(root) => CodexSessionRootReader::with_limits(
                root.path().to_path_buf(),
                root.scan_limits().clone(),
            )
            .scan_records_resumable(request, sink),
            TranscriptAdapterConfiguration::Pi(root) => PiRunHistoryRootReader::with_limits(
                root.path().to_path_buf(),
                root.scan_limits().clone(),
            )
            .scan_records_resumable(request, sink),
        }
    }

    fn source_identifier(&self, source: &TranscriptAdapterConfiguration) -> SourceIdentifier {
        SourceIdentifier::new(format!(
            "{:?}:{}",
            source.kind(),
            source.root().path().display()
        ))
    }

    fn configuration_signature(&self, source: &TranscriptAdapterConfiguration) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(format!("{:?}", source.kind()).as_bytes());
        hasher.update(source.root().path().as_os_str().as_encoded_bytes());
        hasher.update(
            &source
                .root()
                .scan_limits()
                .maximum_file_bytes()
                .to_le_bytes(),
        );
        *hasher.finalize().as_bytes()
    }

    fn write_checkpoint(
        &self,
        staging: &IndexStaging,
        occurrence: u64,
        cursor: &crate::adapter::TranscriptScanCursor,
        run: &SourceGenerationRun,
    ) -> Result<()> {
        let chunk = IndexChunk {
            schema_version: 1,
            records: vec![IndexRecordDto {
                schema_version: 1,
                record_kind: 2,
                fields: vec![
                    IndexFieldDto {
                        name: "source-slot".to_owned(),
                        bytes: occurrence.to_le_bytes().to_vec(),
                    },
                    IndexFieldDto {
                        name: "cursor-next".to_owned(),
                        bytes: cursor.next_discovery_ordinal.to_le_bytes().to_vec(),
                    },
                    IndexFieldDto {
                        name: "cursor-prefix".to_owned(),
                        bytes: cursor.completed_prefix_digest.to_vec(),
                    },
                    IndexFieldDto {
                        name: "run-identity".to_owned(),
                        bytes: run.content_identity.to_vec(),
                    },
                ],
            }],
        };
        staging.replace_checkpoint(
            &IndexLocator::new(format!("checkpoint-{occurrence}")),
            &chunk,
        )
    }
}

/// Writes capped normalized observations. Text is reduced to its existing stable reference hash
/// while the adapter line is alive, so no transcript body survives in the builder.
#[derive(Debug)]
pub struct BoundedGenerationBuilder {
    staging: IndexStaging,
    source_key: SourceKey,
    limits: IndexStoreLimits,
    meter: IndexResourceMeter,
    records: Vec<IndexRecordDto>,
    logical_bytes: u64,
    logical_reservation: Option<IndexReservation>,
    chunk_count: u64,
    logical_bytes_written: u64,
    record_count: u64,
    next_chunk: u64,
    content_hasher: blake3::Hasher,
    failure: Option<Error>,
}

impl BoundedGenerationBuilder {
    pub fn new(
        staging: IndexStaging,
        source_key: SourceKey,
        limits: IndexStoreLimits,
        meter: IndexResourceMeter,
    ) -> Self {
        Self {
            staging,
            source_key,
            limits,
            meter,
            records: Vec::new(),
            logical_bytes: 0,
            logical_reservation: None,
            chunk_count: 0,
            logical_bytes_written: 0,
            record_count: 0,
            next_chunk: 0,
            content_hasher: blake3::Hasher::new(),
            failure: None,
        }
    }

    pub fn finish(mut self) -> Result<SourceGenerationRun> {
        self.flush()?;
        if let Some(error) = self.failure {
            return Err(error);
        }
        Ok(SourceGenerationRun {
            source_key: self.source_key,
            chunk_count: self.chunk_count,
            logical_bytes: self.logical_bytes_written,
            record_count: self.record_count,
            content_identity: *self.content_hasher.finalize().as_bytes(),
        })
    }

    fn observe_normalized(&mut self, record: TranscriptRecord) {
        if self.failure.is_some() {
            return;
        }
        let dto = NormalizedObservation::new(&self.source_key, &record).dto();
        let logical_bytes = NormalizedObservation::logical_bytes(&dto);
        self.content_hasher
            .update(&NormalizedObservation::identity(&dto));
        if logical_bytes > self.limits.maximum_record_bytes {
            self.failure = Some(Error::index_store(
                crate::error::IndexStoreError::OversizedRecord,
            ));
            return;
        }
        if !self.records.is_empty()
            && !self.limits.accepts_chunk(
                self.logical_bytes.saturating_add(logical_bytes),
                self.records.len() as u64 + 1,
            )
            && let Err(error) = self.flush()
        {
            self.failure = Some(error);
            return;
        }
        if !self.limits.accepts_chunk(logical_bytes, 1) {
            self.failure = Some(Error::index_store(
                crate::error::IndexStoreError::OversizedRecord,
            ));
            return;
        }
        self.logical_bytes = self.logical_bytes.saturating_add(logical_bytes);
        self.logical_reservation = None;
        self.logical_reservation = Some(
            self.meter
                .reserve(IndexWorkCategory::LogicalChunk, self.logical_bytes),
        );
        self.records.push(dto);
        self.record_count += 1;
    }

    fn flush(&mut self) -> Result<()> {
        if self.records.is_empty() {
            return Ok(());
        }
        let locator = IndexLocator::new(format!(
            "run-{}-{:016x}",
            self.source_key.configured_occurrence(),
            self.next_chunk
        ));
        let record_count = self.records.len() as u64;
        let chunk = IndexChunk {
            schema_version: 1,
            records: std::mem::take(&mut self.records),
        };
        self.staging
            .write_chunk(&locator, IndexFileKind::Chunk, &chunk)?;
        // The envelope is bounded by the writer; the descriptor's byte count is deliberately
        // logical so reducers do not need to reopen a chunk merely to schedule fixed fan-in work.
        self.meter.observe_chunk(record_count, self.logical_bytes);
        self.logical_bytes_written = self
            .logical_bytes_written
            .saturating_add(self.logical_bytes);
        self.chunk_count += 1;
        self.logical_bytes = 0;
        self.logical_reservation = None;
        self.next_chunk += 1;
        Ok(())
    }
}

impl TranscriptRecordSink for BoundedGenerationBuilder {
    fn observe_record(&mut self, record: TranscriptRecord) {
        self.observe_normalized(record);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedObservation {
    source_key: SourceKey,
    path: String,
    line_number: u64,
    session_identifier: Option<String>,
    task_identifier: Option<String>,
    subagent_name: Option<String>,
    text_hash: String,
    block_count: u64,
}

impl NormalizedObservation {
    fn new(source_key: &SourceKey, record: &TranscriptRecord) -> Self {
        Self {
            source_key: source_key.clone(),
            path: record.path.display().to_string(),
            line_number: record.line_number,
            session_identifier: record
                .session_identifier
                .as_ref()
                .map(|value| value.as_str().to_owned()),
            task_identifier: record
                .task_metadata
                .as_ref()
                .map(|value| value.task_identifier.as_str().to_owned()),
            subagent_name: record
                .subagent_name
                .as_ref()
                .map(|value| value.as_str().to_owned()),
            text_hash: super::StableHash::new(&record.text).hex(),
            block_count: record.blocks.len() as u64,
        }
    }

    fn dto(&self) -> IndexRecordDto {
        let mut fields = self.source_key.fields().to_vec();
        fields.extend([
            Self::field("path-display", self.path.as_bytes()),
            Self::field("line-number", &self.line_number.to_le_bytes()),
            Self::optional_field("session-identifier", self.session_identifier.as_deref()),
            Self::optional_field("task-identifier", self.task_identifier.as_deref()),
            Self::optional_field("subagent-name", self.subagent_name.as_deref()),
            Self::field("stable-text-hash", self.text_hash.as_bytes()),
            Self::field("block-count", &self.block_count.to_le_bytes()),
        ]);
        IndexRecordDto {
            schema_version: 1,
            record_kind: 1,
            fields,
        }
    }

    fn field(name: &str, bytes: &[u8]) -> IndexFieldDto {
        IndexFieldDto {
            name: name.to_owned(),
            bytes: bytes.to_vec(),
        }
    }

    fn optional_field(name: &str, value: Option<&str>) -> IndexFieldDto {
        Self::field(name, value.unwrap_or_default().as_bytes())
    }

    fn identity(dto: &IndexRecordDto) -> Vec<u8> {
        let mut identity = Vec::new();
        for field in &dto.fields {
            identity.extend_from_slice(field.name.as_bytes());
            identity.push(0);
            identity.extend_from_slice(&field.bytes);
            identity.push(0);
        }
        identity
    }

    fn logical_bytes(dto: &IndexRecordDto) -> u64 {
        dto.fields
            .iter()
            .map(|field| field.name.len() as u64 + field.bytes.len() as u64)
            .sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SnapshotIdentity {
    value: [u8; 32],
}

impl SnapshotIdentity {
    fn new(runs: &[SourceGenerationRun]) -> Self {
        let mut hasher = blake3::Hasher::new();
        for run in runs {
            hasher.update(&run.source_key.signature());
            hasher.update(&run.content_identity);
            hasher.update(&run.record_count.to_le_bytes());
        }
        Self {
            value: *hasher.finalize().as_bytes(),
        }
    }

    fn value(&self) -> [u8; 32] {
        self.value
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IndexManifestRecord {
    records: Vec<IndexRecordDto>,
}

impl IndexManifestRecord {
    fn new(runs: &[SourceGenerationRun]) -> Self {
        let records = runs
            .iter()
            .map(|run| IndexRecordDto {
                schema_version: 1,
                record_kind: 3,
                fields: vec![
                    IndexFieldDto {
                        name: "source-signature".to_owned(),
                        bytes: run.source_key.signature().to_vec(),
                    },
                    IndexFieldDto {
                        name: "generation-identity".to_owned(),
                        bytes: run.content_identity.to_vec(),
                    },
                    IndexFieldDto {
                        name: "record-count".to_owned(),
                        bytes: run.record_count.to_le_bytes().to_vec(),
                    },
                    IndexFieldDto {
                        name: "chunk-count".to_owned(),
                        bytes: run.chunk_count.to_le_bytes().to_vec(),
                    },
                ],
            })
            .collect();
        Self { records }
    }

    fn chunk(self) -> IndexChunk {
        IndexChunk {
            schema_version: 1,
            records: self.records,
        }
    }
}
