//! Bounded observation-run construction for v3 source generations.
//!
//! This module intentionally writes normalized observations before reducing them. A source run
//! never owns a session's children: downstream reducers join by `SourceKey` and scalar keys.

use signal_aggregator::{
    AuthoredStatus, ByteLimit, SourceIdentifier, SourceKind, TranscriptBlockKind,
    TranscriptBlockTextAvailability,
};

use crate::{
    Error, Result, RuntimeConfiguration, TranscriptAdapterConfiguration,
    adapter::{
        TranscriptRawReadOutcome, TranscriptRecord, TranscriptRecordSink, TranscriptScanRequest,
        claude::ClaudeJsonlRootReader, codex::CodexSessionRootReader, pi::PiRunHistoryRootReader,
    },
};

use super::{
    FragileSessionReference, FragileSubagentReference, IndexedOutput, IndexedOutputSegment,
    IndexedTranscriptBlock, SourceFingerprint, SourceKindName, StableReference,
    instrumentation::{IndexReservation, IndexResourceMeter, IndexWorkCategory},
    limits::IndexStoreLimits,
    schema::{
        CurrentPointer, DiskPath, IndexChunk, IndexFieldDto, IndexFileKind, IndexRecordDto,
        ProjectionOutputDto, ProjectionRecordDto, ProjectionSegmentDto, ProjectionSessionDto,
        ProjectionSizeDto, ProjectionSubagentDto, ProjectionTaskDto, ProjectionTranscriptBlockDto,
        TYPED_PROJECTION_DTO_VERSION,
    },
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

    pub fn source_kind(&self) -> &str {
        &self.source_kind
    }

    pub fn source_identifier(&self) -> &str {
        &self.source_identifier
    }

    pub fn signature(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.source_kind.as_bytes());
        hasher.update(&[0]);
        hasher.update(self.source_identifier.as_bytes());
        hasher.update(&self.configured_occurrence.to_le_bytes());
        *hasher.finalize().as_bytes()
    }

    /// Stable identity material for every reference rooted in this configured source.
    /// Producer identifiers are only unique within their configured source occurrence.
    pub fn scoped_reference_material(&self, kind: &str, producer_identifier: &str) -> String {
        format!(
            "{kind}|{}|{}|{}|{producer_identifier}",
            self.source_kind, self.source_identifier, self.configured_occurrence
        )
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
    /// The pointer is absent when an incomplete first scan has no last-complete v3 truth.
    pub pointer: Option<CurrentPointer>,
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
            let configuration_signature = self.configuration_signature(source);
            let request = TranscriptScanRequest::new(occurrence as u64, configuration_signature)
                .with_resume_cursor(self.resume_cursor(
                    source,
                    occurrence as u64,
                    configuration_signature,
                )?);
            let resumable = self.scan_source(source, &request, &mut builder);
            let run = builder.finish()?;
            let checkpoint = IndexLocator::new(format!("checkpoint-{occurrence}"));
            if resumable.outcome.is_complete() {
                self.store.remove_persisted_checkpoint(&checkpoint)?;
            } else {
                self.write_checkpoint(&staging, occurrence as u64, &resumable.cursor, &run)?;
            }
            scan_outcomes.push(resumable.outcome);
            source_runs.push(run);
        }
        let snapshot_identity = SnapshotIdentity::new(&source_runs).value();
        let complete = scan_outcomes
            .iter()
            .all(TranscriptRawReadOutcome::is_complete);
        if !complete {
            // A partially scanned first generation never becomes query truth. Existing complete
            // truth remains pointed to while the scan facts report the provisional coverage.
            return Ok(BoundedGenerationPublication {
                pointer: self.store.read_current_pointer()?,
                source_runs,
                scan_outcomes,
                resource_high_water_bytes: self.meter.snapshot().high_water_bytes,
            });
        }
        if let Some(current) = self.store.read_current_pointer()?
            && current.snapshot_identity == snapshot_identity
        {
            return Ok(BoundedGenerationPublication {
                pointer: Some(current),
                source_runs,
                scan_outcomes,
                resource_high_water_bytes: self.meter.snapshot().high_water_bytes,
            });
        }
        let manifest = IndexManifestRecord::new(&source_runs).chunk();
        let manifest_locator = IndexLocator::new("manifest");
        staging.write_chunk(&manifest_locator, IndexFileKind::Manifest, &manifest)?;
        let pointer = self
            .store
            .publish(&staging, &manifest_locator, snapshot_identity)?;
        Ok(BoundedGenerationPublication {
            pointer: Some(pointer),
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

    fn resume_cursor(
        &self,
        source: &TranscriptAdapterConfiguration,
        occurrence: u64,
        configuration_signature: [u8; 32],
    ) -> Result<Option<crate::adapter::TranscriptScanCursor>> {
        let locator = IndexLocator::new(format!("checkpoint-{occurrence}"));
        let Some(chunk) = self.store.read_persisted_checkpoint(&locator)? else {
            return Ok(None);
        };
        let Some(record) = chunk.records.first() else {
            return Ok(None);
        };
        let field = |name: &str| {
            record
                .fields
                .iter()
                .find(|field| field.name == name)
                .map(|field| field.bytes.as_slice())
        };
        let next = field("cursor-next")
            .and_then(|bytes| bytes.try_into().ok())
            .map(u64::from_le_bytes);
        let prefix = field("cursor-prefix").and_then(|bytes| bytes.try_into().ok());
        let checkpoint_signature: Option<[u8; 32]> =
            field("configuration-signature").and_then(|bytes| bytes.try_into().ok());
        match (next, prefix, checkpoint_signature) {
            (Some(next_discovery_ordinal), Some(completed_prefix_digest), Some(signature))
                if signature == configuration_signature =>
            {
                Ok(Some(crate::adapter::TranscriptScanCursor::new(
                    source.kind(),
                    self.source_identifier(source),
                    occurrence,
                    configuration_signature,
                    next_discovery_ordinal,
                    completed_prefix_digest,
                )))
            }
            _ => Ok(None),
        }
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
            projection: None,
            records: vec![IndexRecordDto {
                schema_version: 1,
                record_kind: 2,
                fields: vec![
                    IndexFieldDto {
                        name: "source-slot".to_owned(),
                        bytes: occurrence.to_le_bytes().to_vec(),
                    },
                    IndexFieldDto {
                        name: "configuration-signature".to_owned(),
                        bytes: cursor.configuration_signature.to_vec(),
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
        let locator = IndexLocator::new(format!("checkpoint-{occurrence}"));
        staging.replace_checkpoint(&locator, &chunk)?;
        self.store.replace_persisted_checkpoint(&locator, &chunk)
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
    next_projection_chunk: u64,
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
            next_projection_chunk: 0,
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
        if let Err(error) = self.emit_typed_projection(&record) {
            self.failure = Some(error);
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

    /// Emits each leaf as its own immutable typed chunk.  The only in-memory state is the
    /// current adapter record and its capped preview; parent cardinalities are deliberately not
    /// represented as child vectors.
    fn emit_typed_projection(&mut self, record: &TranscriptRecord) -> Result<()> {
        let fingerprint = SourceFingerprint::from_path(&record.path)
            .unwrap_or_else(|_| SourceFingerprint::missing());
        let session_material = record.session_identifier.as_ref().map_or_else(
            || {
                format!(
                    "{}|{}|{}|{}",
                    SourceKindName::new(record.source).as_str(),
                    record.source_identifier.as_str(),
                    record.path.display(),
                    fingerprint.material(),
                )
            },
            |identifier| {
                self.source_key
                    .scoped_reference_material("producer-session", identifier.as_str())
            },
        );
        let session_reference = FragileSessionReference::new(
            StableReference::new("session", session_material).as_string(),
        );
        let subagent_reference = record.subagent_name.as_ref().map(|name| {
            FragileSubagentReference::new(
                StableReference::new(
                    "subagent",
                    format!("{}|{}", session_reference.as_str(), name.as_str()),
                )
                .as_string(),
            )
        });
        let preview_limit = ByteLimit::new(self.limits.maximum_string_bytes);
        let output = IndexedOutput::from_record(
            record.clone(),
            session_reference.clone(),
            subagent_reference.clone(),
            fingerprint.clone(),
            preview_limit,
        );
        self.write_projection(ProjectionRecordDto::Output(self.output_dto(&output)))?;
        self.write_projection(ProjectionRecordDto::Segment(
            self.segment_dto(&IndexedOutputSegment::from_output(&output)),
        ))?;
        for block in record.transcript_blocks() {
            let block_subagent = block.subagent_name.as_ref().map(|name| {
                FragileSubagentReference::new(
                    StableReference::new(
                        "subagent",
                        format!("{}|{}", session_reference.as_str(), name.as_str()),
                    )
                    .as_string(),
                )
            });
            let indexed = IndexedTranscriptBlock::from_record(
                block,
                session_reference.clone(),
                block_subagent,
                fingerprint.clone(),
                preview_limit,
            );
            self.write_projection(ProjectionRecordDto::TranscriptBlock(
                self.block_dto(&indexed),
            ))?;
        }
        self.write_projection(ProjectionRecordDto::Session(ProjectionSessionDto {
            reference: session_reference.as_str().to_owned(),
            source: SourceKindCode::new(record.source).code(),
            source_identifier: record.source_identifier.as_str().to_owned(),
            path: DiskPath::new(
                record.path.as_os_str().as_encoded_bytes().to_vec(),
                record.path.display().to_string(),
            ),
            fingerprint_bytes: fingerprint.byte_count,
            fingerprint_seconds: fingerprint.modified_seconds,
            fingerprint_nanoseconds: fingerprint.modified_nanoseconds,
            started_at: record
                .timestamp
                .as_ref()
                .map(|timestamp| timestamp.as_str().to_owned()),
            last_observed_at: record
                .timestamp
                .as_ref()
                .map(|timestamp| timestamp.as_str().to_owned()),
            producer_session_identifier: record
                .session_identifier
                .as_ref()
                .map(|identifier| identifier.as_str().to_owned()),
            subagent_count: u64::from(record.subagent_name.is_some()),
            output_count: 1,
            size: self.size_dto(record.byte_count(), record.line_count(), 1, 1),
        }))?;
        if let Some(name) = &record.subagent_name {
            self.write_projection(ProjectionRecordDto::Subagent(ProjectionSubagentDto {
                reference: subagent_reference
                    .as_ref()
                    .expect("subagent reference follows subagent name")
                    .as_str()
                    .to_owned(),
                session_reference: session_reference.as_str().to_owned(),
                name: name.as_str().to_owned(),
                authored_status: AuthoredStatusCode::new(record.authored_status).code(),
                task: record.task_metadata.as_ref().map(|task| ProjectionTaskDto {
                    task_identifier: task.task_identifier.as_str().to_owned(),
                }),
                output_count: 1,
                size: self.size_dto(record.byte_count(), record.line_count(), 1, 1),
                first_observed_at: record
                    .timestamp
                    .as_ref()
                    .map(|timestamp| timestamp.as_str().to_owned()),
                last_observed_at: record
                    .timestamp
                    .as_ref()
                    .map(|timestamp| timestamp.as_str().to_owned()),
            }))?;
        }
        self.write_indexes(&session_reference, subagent_reference.as_ref(), &output)?;
        Ok(())
    }

    fn write_projection(&mut self, projection: ProjectionRecordDto) -> Result<()> {
        let locator = IndexLocator::new(format!(
            "run-{}-projection-{:016x}",
            self.source_key.configured_occurrence(),
            self.next_projection_chunk
        ));
        self.staging.write_chunk(
            &locator,
            IndexFileKind::Projection,
            &IndexChunk {
                schema_version: TYPED_PROJECTION_DTO_VERSION,
                records: Vec::new(),
                projection: Some(projection),
            },
        )?;
        self.next_projection_chunk += 1;
        self.chunk_count += 1;
        Ok(())
    }

    /// Index entries are scalar records: references and relationship edges never carry an
    /// unbounded child array.  Fixed fan-out readers group these immutable leaves by hash.
    fn write_indexes(
        &mut self,
        session: &FragileSessionReference,
        subagent: Option<&FragileSubagentReference>,
        output: &IndexedOutput,
    ) -> Result<()> {
        let session_entry = self.index_entry("reference", session.as_str(), "session");
        let output_entry = self.index_entry("reference", output.reference.as_str(), "output");
        let output_parent =
            self.index_entry("relationship", output.reference.as_str(), session.as_str());
        self.write_index_chunk(vec![session_entry])?;
        self.write_index_chunk(vec![output_entry])?;
        self.write_index_chunk(vec![output_parent])?;
        if let Some(subagent) = subagent {
            let subagent_entry = self.index_entry("reference", subagent.as_str(), "subagent");
            let subagent_parent =
                self.index_entry("relationship", subagent.as_str(), session.as_str());
            self.write_index_chunk(vec![subagent_entry])?;
            self.write_index_chunk(vec![subagent_parent])?;
        }
        Ok(())
    }

    fn write_index_chunk(&mut self, entries: Vec<IndexRecordDto>) -> Result<()> {
        let locator = IndexLocator::new(format!(
            "run-{}-index-{:016x}",
            self.source_key.configured_occurrence(),
            self.next_projection_chunk
        ));
        self.staging.write_chunk(
            &locator,
            IndexFileKind::ReferenceIndex,
            &IndexChunk {
                schema_version: TYPED_PROJECTION_DTO_VERSION,
                records: entries,
                projection: None,
            },
        )?;
        self.next_projection_chunk += 1;
        self.chunk_count += 1;
        Ok(())
    }

    fn index_entry(&self, kind: &str, key: &str, value: &str) -> IndexRecordDto {
        IndexRecordDto {
            schema_version: TYPED_PROJECTION_DTO_VERSION,
            record_kind: 40,
            fields: vec![
                IndexFieldDto {
                    name: "index-kind".to_owned(),
                    bytes: kind.as_bytes().to_vec(),
                },
                IndexFieldDto {
                    name: "key-hash".to_owned(),
                    bytes: blake3::hash(key.as_bytes()).as_bytes().to_vec(),
                },
                IndexFieldDto {
                    name: "exact-key".to_owned(),
                    bytes: key.as_bytes().to_vec(),
                },
                IndexFieldDto {
                    name: "exact-value".to_owned(),
                    bytes: value.as_bytes().to_vec(),
                },
            ],
        }
    }

    fn size_dto(
        &self,
        byte_count: u64,
        line_count: u64,
        segment_count: u64,
        certainty: u8,
    ) -> ProjectionSizeDto {
        ProjectionSizeDto {
            byte_count: Some(byte_count),
            line_count: Some(line_count),
            segment_count: Some(segment_count),
            certainty,
        }
    }

    fn disk_path(&self, path: &std::path::Path) -> DiskPath {
        DiskPath::new(
            path.as_os_str().as_encoded_bytes().to_vec(),
            path.display().to_string(),
        )
    }

    fn output_dto(&self, output: &IndexedOutput) -> ProjectionOutputDto {
        ProjectionOutputDto {
            reference: output.reference.as_str().to_owned(),
            session_reference: output.session_reference.as_str().to_owned(),
            subagent_reference: output
                .subagent_reference
                .as_ref()
                .map(|reference| reference.as_str().to_owned()),
            title: output.title.as_ref().map(|title| title.as_str().to_owned()),
            task: output.task.as_ref().map(|task| ProjectionTaskDto {
                task_identifier: task.task_identifier.as_str().to_owned(),
            }),
            source: SourceKindCode::new(output.provenance.source).code(),
            source_identifier: output.provenance.source_identifier.as_str().to_owned(),
            authored_status: AuthoredStatusCode::new(output.provenance.authored_status).code(),
            produced_at: output
                .provenance
                .produced_at
                .as_ref()
                .map(|timestamp| timestamp.as_str().to_owned()),
            path: self.disk_path(&output.path),
            fingerprint_bytes: output.fingerprint.byte_count,
            fingerprint_seconds: output.fingerprint.modified_seconds,
            fingerprint_nanoseconds: output.fingerprint.modified_nanoseconds,
            source_line_number: output.source_line_number,
            text_hash: output.text_hash.clone(),
            size: self.size_dto(
                output.size.byte_count.map_or(0, |count| count.into_u64()),
                output.size.line_count.map_or(0, |count| count.into_u64()),
                output
                    .size
                    .segment_count
                    .map_or(0, |count| count.into_u64()),
                1,
            ),
            preview_text: output.preview_text.clone(),
            preview_original_bytes: output.preview_original_bytes,
        }
    }

    fn segment_dto(&self, segment: &IndexedOutputSegment) -> ProjectionSegmentDto {
        ProjectionSegmentDto {
            reference: segment.reference.as_str().to_owned(),
            output_reference: segment.output_reference.as_str().to_owned(),
            segment_index: segment.segment_index.into_u64(),
            byte_range: segment
                .byte_range
                .as_ref()
                .map(|range| (range.start.into_u64(), range.end.into_u64())),
            line_range: segment
                .line_range
                .as_ref()
                .map(|range| (range.start.into_u64(), range.end.into_u64())),
            size: self.size_dto(
                segment.size.byte_count.map_or(0, |count| count.into_u64()),
                segment.size.line_count.map_or(0, |count| count.into_u64()),
                segment
                    .size
                    .segment_count
                    .map_or(0, |count| count.into_u64()),
                1,
            ),
            preview_text: segment.preview_text.clone(),
            preview_original_bytes: segment.preview_original_bytes,
            source: SourceKindCode::new(segment.source).code(),
            path: self.disk_path(&segment.path),
        }
    }

    fn block_dto(&self, block: &IndexedTranscriptBlock) -> ProjectionTranscriptBlockDto {
        ProjectionTranscriptBlockDto {
            reference: block.reference.as_str().to_owned(),
            session_reference: block.session_reference.as_str().to_owned(),
            subagent_reference: block
                .subagent_reference
                .as_ref()
                .map(|reference| reference.as_str().to_owned()),
            kind: TranscriptBlockKindCode::new(block.kind).code(),
            block_index: block.block_index.into_u64(),
            task: block.task.as_ref().map(|task| ProjectionTaskDto {
                task_identifier: task.task_identifier.as_str().to_owned(),
            }),
            source: SourceKindCode::new(block.provenance.source).code(),
            source_identifier: block.provenance.source_identifier.as_str().to_owned(),
            authored_status: AuthoredStatusCode::new(block.provenance.authored_status).code(),
            observed_at: block
                .provenance
                .observed_at
                .as_ref()
                .map(|timestamp| timestamp.as_str().to_owned()),
            path: self.disk_path(&block.path),
            fingerprint_bytes: block.fingerprint.byte_count,
            fingerprint_seconds: block.fingerprint.modified_seconds,
            fingerprint_nanoseconds: block.fingerprint.modified_nanoseconds,
            source_line_number: block.source_line_number,
            text_hash: block.text_hash.clone(),
            size: self.size_dto(
                block.size.byte_count.map_or(0, |count| count.into_u64()),
                block.size.line_count.map_or(0, |count| count.into_u64()),
                block.size.segment_count.map_or(0, |count| count.into_u64()),
                1,
            ),
            text_availability: TranscriptBlockTextAvailabilityCode::new(block.text_availability)
                .code(),
            preview_text: block.preview_text.clone(),
            preview_original_bytes: block.preview_original_bytes,
        }
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
            projection: None,
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

#[derive(Debug, Clone, Copy)]
struct SourceKindCode {
    source: SourceKind,
}
impl SourceKindCode {
    fn new(source: SourceKind) -> Self {
        Self { source }
    }
    fn code(self) -> u8 {
        match self.source {
            SourceKind::Claude => 1,
            SourceKind::ClaudeSubagentOutput => 2,
            SourceKind::Codex => 3,
            SourceKind::Pi => 4,
            SourceKind::PiSubagentOutput => 5,
            SourceKind::Repository => 6,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct AuthoredStatusCode {
    status: AuthoredStatus,
}
impl AuthoredStatusCode {
    fn new(status: AuthoredStatus) -> Self {
        Self { status }
    }
    fn code(self) -> u8 {
        match self.status {
            AuthoredStatus::AgentAuthored => 1,
            AuthoredStatus::HumanAuthored => 2,
            AuthoredStatus::MixedAuthorship => 3,
            AuthoredStatus::UnknownAuthorship => 4,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct TranscriptBlockKindCode {
    kind: TranscriptBlockKind,
}
impl TranscriptBlockKindCode {
    fn new(kind: TranscriptBlockKind) -> Self {
        Self { kind }
    }
    fn code(self) -> u8 {
        match self.kind {
            TranscriptBlockKind::UserPrompt => 1,
            TranscriptBlockKind::AgentResponse => 2,
            TranscriptBlockKind::ToolCall => 3,
            TranscriptBlockKind::ToolResult => 4,
            TranscriptBlockKind::Inference => 5,
            TranscriptBlockKind::SystemInstruction => 6,
            TranscriptBlockKind::Attachment => 7,
            TranscriptBlockKind::SessionEvent => 8,
            TranscriptBlockKind::Unclassified => 9,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct TranscriptBlockTextAvailabilityCode {
    availability: TranscriptBlockTextAvailability,
}
impl TranscriptBlockTextAvailabilityCode {
    fn new(availability: TranscriptBlockTextAvailability) -> Self {
        Self { availability }
    }
    fn code(self) -> u8 {
        match self.availability {
            TranscriptBlockTextAvailability::ReadableText => 1,
            TranscriptBlockTextAvailability::UnavailableText => 2,
            TranscriptBlockTextAvailability::EncryptedText => 3,
        }
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
            projection: None,
        }
    }
}
