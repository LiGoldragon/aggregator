//! Bounded observation-run construction for v3 source generations.
//!
//! This module intentionally writes normalized observations before reducing them. A source run
//! never owns a session's children: downstream reducers join by `SourceKey` and scalar keys.

use signal_aggregator::{
    AuthoredStatus, ByteLimit, ListingOrder, SourceIdentifier, SourceKind, TranscriptBlockKind,
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
    IndexedTranscriptBlock, ProjectionTreeOrdering, SourceFingerprint, SourceKindName,
    StableReference,
    instrumentation::{IndexReservation, IndexResourceMeter, IndexWorkCategory},
    limits::IndexStoreLimits,
    migration_v2::{IndexFormat, MigrationSource, V2Migration},
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
    /// Projection locators are a deterministic source-local sequence.  Publishers consume this
    /// scalar count instead of enumerating the staging directory.
    pub projection_count: u64,
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
        self.migrate_v2_before_v3_publication(&staging)?;
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
        let tree_roots = FixedFanoutTreePublisher::new(
            staging.clone(),
            self.limits,
            &source_runs,
            self.meter.clone(),
        )
        .publish()?;
        let snapshot_identity = SnapshotIdentity::new(&source_runs)
            .with_roots(&tree_roots)
            .value();
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
        let manifest = IndexManifestRecord::new(&source_runs, &tree_roots).chunk();
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

    /// A v2 pointer is immutable rollback evidence until `IndexStore::publish` has committed a
    /// complete v3 manifest.  Importing into the same unlinked staging generation ensures a
    /// crash can leave either the original v2 pointer or a fully verified v3 pointer, never a
    /// partially replaced compatibility file.
    fn migrate_v2_before_v3_publication(&self, staging: &IndexStaging) -> Result<()> {
        if self.store.current_format()? != IndexFormat::MigratableV2 {
            return Ok(());
        }
        let legacy_pointer = self.store.pointer_path().to_path_buf();
        self.store.retain_v2_backup(&legacy_pointer)?;
        let sources = self
            .configuration
            .transcript_sources()
            .iter()
            .enumerate()
            .map(|(occurrence, source)| {
                MigrationSource::from_source_key(&SourceKey::new(
                    source.kind(),
                    self.source_identifier(source),
                    occurrence as u64,
                ))
            })
            .collect();
        V2Migration::new(self.limits, sources).import_into_staging(&legacy_pointer, staging)?;
        Ok(())
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
    next_projection_chunk: u64,
    next_index_chunk: u64,
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
            next_index_chunk: 0,
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
            projection_count: self.next_projection_chunk,
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
            self.next_index_chunk
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
        self.next_index_chunk += 1;
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

    fn with_roots(mut self, roots: &[FixedTreeRoot]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.value);
        for root in roots {
            hasher.update(root.specification.name().as_bytes());
            hasher.update(&[0]);
            // Locators are generation-local capabilities.  The snapshot binds the deterministic
            // root identity and cardinality, not the random staging-generation spelling.
            hasher.update(&root.item_count.to_le_bytes());
        }
        self.value = *hasher.finalize().as_bytes();
        self
    }

    fn value(&self) -> [u8; 32] {
        self.value
    }
}

/// An immutable tree root.  Every root is a scalar manifest fact; the tree body is a
/// fixed-fanout external merge of scalar entries.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FixedTreeRoot {
    specification: TreeSpecification,
    locator: Option<String>,
    item_count: u64,
}

impl FixedTreeRoot {
    fn manifest_record(&self) -> IndexRecordDto {
        IndexRecordDto {
            schema_version: 1,
            record_kind: 61,
            fields: vec![
                IndexFieldDto {
                    name: "tree-collection".to_owned(),
                    bytes: self.specification.name().into_bytes(),
                },
                IndexFieldDto {
                    name: "tree-root".to_owned(),
                    bytes: self.locator.clone().unwrap_or_default().into_bytes(),
                },
                IndexFieldDto {
                    name: "tree-count".to_owned(),
                    bytes: self.item_count.to_le_bytes().to_vec(),
                },
            ],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FixedTreeCollection {
    Sessions,
    Subagents,
    Outputs,
    Segments,
    TranscriptBlocks,
}

impl FixedTreeCollection {
    fn all() -> [Self; 5] {
        [
            Self::Sessions,
            Self::Subagents,
            Self::Outputs,
            Self::Segments,
            Self::TranscriptBlocks,
        ]
    }

    fn name(self) -> &'static str {
        match self {
            Self::Sessions => "sessions",
            Self::Subagents => "subagents",
            Self::Outputs => "outputs",
            Self::Segments => "segments",
            Self::TranscriptBlocks => "transcript-blocks",
        }
    }

    fn reference(self, projection: &ProjectionRecordDto) -> Option<&str> {
        match (self, projection) {
            (Self::Sessions, ProjectionRecordDto::Session(value)) => Some(&value.reference),
            (Self::Subagents, ProjectionRecordDto::Subagent(value)) => Some(&value.reference),
            (Self::Outputs, ProjectionRecordDto::Output(value)) => Some(&value.reference),
            (Self::Segments, ProjectionRecordDto::Segment(value)) => Some(&value.reference),
            (Self::TranscriptBlocks, ProjectionRecordDto::TranscriptBlock(value)) => {
                Some(&value.reference)
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TreeOrder {
    Reference,
    Oldest,
    Newest,
    OldestModified,
    NewestModified,
}
impl TreeOrder {
    fn all() -> [Self; 5] {
        [
            Self::Reference,
            Self::Oldest,
            Self::Newest,
            Self::OldestModified,
            Self::NewestModified,
        ]
    }
    fn listing_order(self) -> ListingOrder {
        match self {
            Self::Reference => ListingOrder::ReferenceAscending,
            Self::Oldest => ListingOrder::OldestFirst,
            Self::Newest => ListingOrder::NewestFirst,
            Self::OldestModified => ListingOrder::OldestModifiedFirst,
            Self::NewestModified => ListingOrder::NewestModifiedFirst,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Reference => "reference",
            Self::Oldest => "oldest",
            Self::Newest => "newest",
            Self::OldestModified => "modified-oldest",
            Self::NewestModified => "modified-newest",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TreeRelationship {
    SessionSubagent,
    SessionOutput,
    SubagentOutput,
    OutputSegment,
    SessionTranscriptBlock,
    SubagentTranscriptBlock,
}
impl TreeRelationship {
    fn all() -> [Self; 6] {
        [
            Self::SessionSubagent,
            Self::SessionOutput,
            Self::SubagentOutput,
            Self::OutputSegment,
            Self::SessionTranscriptBlock,
            Self::SubagentTranscriptBlock,
        ]
    }
    fn name(self) -> &'static str {
        match self {
            Self::SessionSubagent => "session-subagent",
            Self::SessionOutput => "session-output",
            Self::SubagentOutput => "subagent-output",
            Self::OutputSegment => "output-segment",
            Self::SessionTranscriptBlock => "session-transcript-block",
            Self::SubagentTranscriptBlock => "subagent-transcript-block",
        }
    }

    fn child_collection(self) -> FixedTreeCollection {
        match self {
            Self::SessionSubagent => FixedTreeCollection::Subagents,
            Self::SessionOutput | Self::SubagentOutput => FixedTreeCollection::Outputs,
            Self::OutputSegment => FixedTreeCollection::Segments,
            Self::SessionTranscriptBlock | Self::SubagentTranscriptBlock => {
                FixedTreeCollection::TranscriptBlocks
            }
        }
    }

    fn parent_reference(self, projection: &ProjectionRecordDto) -> Option<&str> {
        match (self, projection) {
            (Self::SessionSubagent, ProjectionRecordDto::Subagent(value)) => {
                Some(&value.session_reference)
            }
            (Self::SessionOutput, ProjectionRecordDto::Output(value)) => {
                Some(&value.session_reference)
            }
            (Self::SubagentOutput, ProjectionRecordDto::Output(value)) => {
                value.subagent_reference.as_deref()
            }
            (Self::OutputSegment, ProjectionRecordDto::Segment(value)) => {
                Some(&value.output_reference)
            }
            (Self::SessionTranscriptBlock, ProjectionRecordDto::TranscriptBlock(value)) => {
                Some(&value.session_reference)
            }
            (Self::SubagentTranscriptBlock, ProjectionRecordDto::TranscriptBlock(value)) => {
                value.subagent_reference.as_deref()
            }
            _ => None,
        }
    }
}

/// The manifest name is the query-independent selection key for a tree.  Filter predicates are
/// evaluated from the leaf projection later; relationship roots make the cardinality boundary
/// explicit before those predicates run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TreeSpecification {
    collection: FixedTreeCollection,
    relationship: Option<TreeRelationship>,
    order: TreeOrder,
}
impl TreeSpecification {
    pub(crate) fn all() -> Vec<Self> {
        let collections = FixedTreeCollection::all()
            .into_iter()
            .flat_map(|collection| {
                TreeOrder::all().into_iter().map(move |order| Self {
                    collection,
                    relationship: None,
                    order,
                })
            });
        let relationships = TreeRelationship::all()
            .into_iter()
            .flat_map(|relationship| {
                TreeOrder::all().into_iter().map(move |order| Self {
                    collection: relationship.child_collection(),
                    relationship: Some(relationship),
                    order,
                })
            });
        collections.chain(relationships).collect()
    }

    pub(crate) fn name(self) -> String {
        match self.relationship {
            Some(relationship) => {
                format!("relationship:{}:{}", relationship.name(), self.order.name())
            }
            // Reference roots retain their established v3 names for point lookup.
            None if self.order == TreeOrder::Reference => self.collection.name().to_owned(),
            None => format!("order:{}:{}", self.collection.name(), self.order.name()),
        }
    }

    fn entry(self, projection: &ProjectionRecordDto, locator: String) -> Option<TreeLeafEntry> {
        let reference = self.collection.reference(projection)?.to_owned();
        let ordering = if self.order == TreeOrder::Reference {
            // Point-lookup roots retain the exact fragile reference as their B-tree key.
            reference.clone()
        } else {
            ProjectionTreeOrdering::new(self.order.listing_order()).key(projection)?
        };
        let key = match self.relationship {
            Some(relationship) => {
                let parent = relationship.parent_reference(projection)?;
                format!("{parent}\u{1e}{ordering}")
            }
            None => ordering,
        };
        Some(TreeLeafEntry::new(key, reference, locator))
    }
}

/// External-sort publication is directory-independent.  Each pass opens at most the configured
/// fan-in of bounded chunks and writes a new deterministic run, so a corpus larger than fanout
/// cannot force a corpus-sized merge vector.
#[derive(Debug, Clone)]
struct FixedFanoutTreePublisher<'a> {
    staging: IndexStaging,
    limits: IndexStoreLimits,
    source_runs: &'a [SourceGenerationRun],
    meter: IndexResourceMeter,
}

impl<'a> FixedFanoutTreePublisher<'a> {
    const FANOUT: usize = 16;

    fn new(
        staging: IndexStaging,
        limits: IndexStoreLimits,
        source_runs: &'a [SourceGenerationRun],
        meter: IndexResourceMeter,
    ) -> Self {
        Self {
            staging,
            limits,
            source_runs,
            meter,
        }
    }

    fn publish(&self) -> Result<Vec<FixedTreeRoot>> {
        TreeSpecification::all()
            .into_iter()
            .map(|specification| self.publish_specification(specification))
            .collect()
    }

    fn publish_specification(&self, specification: TreeSpecification) -> Result<FixedTreeRoot> {
        let raw_runs = self.write_sorted_runs(specification)?;
        if raw_runs == 0 {
            return Ok(FixedTreeRoot {
                specification,
                locator: None,
                item_count: 0,
            });
        }
        let final_run = self.merge_to_one_run(specification, raw_runs)?;
        let (leaf_count, item_count) = self.write_leaves(specification, final_run)?;
        let locator = self.write_branches(specification, leaf_count)?;
        Ok(FixedTreeRoot {
            specification,
            locator: Some(locator),
            item_count,
        })
    }

    fn fanout(&self) -> u64 {
        self.limits
            .maximum_records_per_chunk
            .max(2)
            .min(Self::FANOUT as u64)
    }

    fn maximum_fan_in(&self) -> u64 {
        self.limits.maximum_merge_fan_in.max(2).min(self.fanout())
    }

    fn write_sorted_runs(&self, specification: TreeSpecification) -> Result<u64> {
        let mut entries = Vec::new();
        let mut logical_bytes = 0_u64;
        let mut run = 0_u64;
        for source_run in self.source_runs {
            for ordinal in 0..source_run.projection_count {
                let locator = format!(
                    "run-{}-projection-{ordinal:016x}",
                    source_run.source_key.configured_occurrence()
                );
                let chunk = self.staging.read_chunk(
                    &IndexLocator::new(locator.clone()),
                    IndexFileKind::Projection,
                )?;
                if let Some(projection) = chunk.projection
                    && let Some(entry) = specification.entry(&projection, locator)
                {
                    if !entries.is_empty()
                        && !self.accepts_entries(logical_bytes, entries.len() as u64, &entry)
                    {
                        self.write_initial_run(specification, run, &mut entries)?;
                        run += 1;
                        logical_bytes = 0;
                    }
                    logical_bytes = logical_bytes.saturating_add(entry.logical_bytes());
                    entries.push(entry);
                }
            }
        }
        if !entries.is_empty() {
            self.write_initial_run(specification, run, &mut entries)?;
            run += 1;
        }
        Ok(run)
    }

    fn write_initial_run(
        &self,
        specification: TreeSpecification,
        ordinal: u64,
        entries: &mut Vec<TreeLeafEntry>,
    ) -> Result<()> {
        let _reservation = self.meter.reserve(
            IndexWorkCategory::DecodedChunk,
            self.limits.maximum_logical_chunk_bytes,
        );
        entries.sort_by(TreeLeafEntry::compare);
        self.write_run_chunk(specification, 0, ordinal, 0, std::mem::take(entries))?;
        self.write_run_metadata(specification, 0, ordinal, 1)
    }

    fn merge_to_one_run(
        &self,
        specification: TreeSpecification,
        mut run_count: u64,
    ) -> Result<TreeRun> {
        let mut stage = 0_u64;
        while run_count > 1 {
            let output_count = run_count.div_ceil(self.maximum_fan_in());
            for output in 0..output_count {
                let first = output * self.maximum_fan_in();
                let last = (first + self.maximum_fan_in()).min(run_count);
                self.merge_run_group(specification, stage, output, first, last)?;
            }
            run_count = output_count;
            stage += 1;
        }
        Ok(TreeRun::new(stage, 0))
    }

    fn merge_run_group(
        &self,
        specification: TreeSpecification,
        output_stage: u64,
        output_run: u64,
        first_input: u64,
        last_input: u64,
    ) -> Result<()> {
        let mut cursors = (first_input..last_input)
            .map(|input| {
                TreeRunCursor::open(self, specification, TreeRun::new(output_stage, input))
            })
            .collect::<Result<Vec<_>>>()?;
        let _reservation = self.meter.reserve(
            IndexWorkCategory::MergeHead,
            self.maximum_fan_in() * self.limits.maximum_logical_chunk_bytes,
        );
        for cursor in &mut cursors {
            cursor.current()?;
        }
        let mut output_chunk = 0_u64;
        let mut entries = Vec::new();
        let mut logical_bytes = 0_u64;
        let mut previous: Option<TreeLeafEntry> = None;
        while let Some(index) = TreeRunHead::new(&cursors).smallest() {
            let entry = cursors[index]
                .take_next()?
                .expect("selected cursor has entry");
            cursors[index].current()?;
            if previous.as_ref().is_some_and(|previous| {
                previous.key == entry.key && previous.reference == entry.reference
            }) {
                continue;
            }
            previous = Some(entry.clone());
            if !entries.is_empty()
                && !self.accepts_entries(logical_bytes, entries.len() as u64, &entry)
            {
                self.write_run_chunk(
                    specification,
                    output_stage + 1,
                    output_run,
                    output_chunk,
                    std::mem::take(&mut entries),
                )?;
                output_chunk += 1;
                logical_bytes = 0;
            }
            logical_bytes = logical_bytes.saturating_add(entry.logical_bytes());
            entries.push(entry);
            if entries.len() as u64 == self.limits.maximum_records_per_chunk {
                self.write_run_chunk(
                    specification,
                    output_stage + 1,
                    output_run,
                    output_chunk,
                    std::mem::take(&mut entries),
                )?;
                output_chunk += 1;
                logical_bytes = 0;
            }
        }
        if !entries.is_empty() {
            self.write_run_chunk(
                specification,
                output_stage + 1,
                output_run,
                output_chunk,
                entries,
            )?;
            output_chunk += 1;
        }
        self.write_run_metadata(specification, output_stage + 1, output_run, output_chunk)
    }

    fn accepts_entries(&self, logical_bytes: u64, count: u64, entry: &TreeLeafEntry) -> bool {
        self.limits.accepts_chunk(
            logical_bytes.saturating_add(entry.logical_bytes()),
            count.saturating_add(1),
        )
    }

    fn write_run_chunk(
        &self,
        specification: TreeSpecification,
        stage: u64,
        run: u64,
        chunk: u64,
        entries: Vec<TreeLeafEntry>,
    ) -> Result<()> {
        self.staging.write_chunk(
            &IndexLocator::new(TreeRun::new(stage, run).chunk_name(&specification, chunk)),
            IndexFileKind::OrderIndex,
            &TreeChunk::entries(entries).chunk(),
        )
    }

    fn write_run_metadata(
        &self,
        specification: TreeSpecification,
        stage: u64,
        run: u64,
        chunks: u64,
    ) -> Result<()> {
        self.staging.write_chunk(
            &IndexLocator::new(TreeRun::new(stage, run).metadata_name(&specification)),
            IndexFileKind::OrderIndex,
            &IndexChunk {
                schema_version: 1,
                projection: None,
                records: vec![IndexRecordDto {
                    schema_version: 1,
                    record_kind: 64,
                    fields: vec![IndexFieldDto {
                        name: "tree-run-chunks".to_owned(),
                        bytes: chunks.to_le_bytes().to_vec(),
                    }],
                }],
            },
        )
    }

    fn write_leaves(&self, specification: TreeSpecification, run: TreeRun) -> Result<(u64, u64)> {
        let mut cursor = TreeRunCursor::open(self, specification, run)?;
        let mut leaves = Vec::new();
        let mut leaf_logical_bytes = 0_u64;
        let mut leaf_count = 0_u64;
        let mut item_count = 0_u64;
        let mut previous: Option<TreeLeafEntry> = None;
        while let Some(entry) = cursor.take_next()? {
            if previous.as_ref().is_some_and(|previous| {
                previous.key == entry.key && previous.reference == entry.reference
            }) {
                continue;
            }
            previous = Some(entry.clone());
            item_count += 1;
            if !leaves.is_empty()
                && !self.accepts_entries(leaf_logical_bytes, leaves.len() as u64, &entry)
            {
                self.write_leaf(specification, leaf_count, &mut leaves)?;
                leaf_count += 1;
                leaf_logical_bytes = 0;
            }
            leaf_logical_bytes = leaf_logical_bytes.saturating_add(entry.logical_bytes());
            leaves.push(entry);
            if leaves.len() as u64 == self.fanout() {
                self.write_leaf(specification, leaf_count, &mut leaves)?;
                leaf_count += 1;
                leaf_logical_bytes = 0;
            }
        }
        if !leaves.is_empty() {
            self.write_leaf(specification, leaf_count, &mut leaves)?;
            leaf_count += 1;
        }
        Ok((leaf_count, item_count))
    }

    fn write_leaf(
        &self,
        specification: TreeSpecification,
        ordinal: u64,
        entries: &mut Vec<TreeLeafEntry>,
    ) -> Result<()> {
        self.staging.write_chunk(
            &IndexLocator::new(format!("tree-leaf-{}-{ordinal:016x}", specification.name())),
            IndexFileKind::IndexNode,
            &TreeChunk::entries(std::mem::take(entries)).chunk(),
        )
    }

    fn write_branches(
        &self,
        specification: TreeSpecification,
        mut children: u64,
    ) -> Result<String> {
        let mut level = 0_u64;
        let mut prefix = "tree-leaf".to_owned();
        while children > 1 {
            let parent_count = children.div_ceil(self.fanout());
            for parent in 0..parent_count {
                let start = parent * self.fanout();
                let end = (start + self.fanout()).min(children);
                let mut entries = Vec::new();
                let mut logical_bytes = 0_u64;
                for child in start..end {
                    let child_name = format!("{prefix}-{}-{child:016x}", specification.name());
                    let child_chunk = self.staging.read_chunk(
                        &IndexLocator::new(child_name.clone()),
                        IndexFileKind::IndexNode,
                    )?;
                    let child_tree = TreeChunk::from_chunk(child_chunk)?;
                    let entry = TreeLeafEntry::branch(
                        child_tree.first_key()?,
                        child_tree.last_key()?,
                        child_name,
                    );
                    if !entries.is_empty()
                        && !self.accepts_entries(logical_bytes, entries.len() as u64, &entry)
                    {
                        return Err(Error::protocol(
                            "typed tree",
                            "branch fanout exceeds chunk limit",
                        ));
                    }
                    logical_bytes = logical_bytes.saturating_add(entry.logical_bytes());
                    entries.push(entry);
                }
                self.staging.write_chunk(
                    &IndexLocator::new(format!(
                        "tree-branch-{level}-{}-{parent:016x}",
                        specification.name()
                    )),
                    IndexFileKind::IndexNode,
                    &TreeChunk::children(entries).chunk(),
                )?;
            }
            children = parent_count;
            prefix = format!("tree-branch-{level}");
            level += 1;
        }
        Ok(format!("{prefix}-{}-{:016x}", specification.name(), 0))
    }
}

#[derive(Debug, Clone, Copy)]
struct TreeRun {
    stage: u64,
    ordinal: u64,
}
impl TreeRun {
    fn new(stage: u64, ordinal: u64) -> Self {
        Self { stage, ordinal }
    }
    fn chunk_name(self, specification: &TreeSpecification, chunk: u64) -> String {
        format!(
            "tree-run-{}-{:04x}-{:016x}-{chunk:016x}",
            specification.name(),
            self.stage,
            self.ordinal
        )
    }
    fn metadata_name(self, specification: &TreeSpecification) -> String {
        format!(
            "tree-run-meta-{}-{:04x}-{:016x}",
            specification.name(),
            self.stage,
            self.ordinal
        )
    }
}

#[derive(Debug)]
struct TreeRunCursor<'a> {
    publisher: &'a FixedFanoutTreePublisher<'a>,
    specification: TreeSpecification,
    run: TreeRun,
    chunks: u64,
    chunk_ordinal: u64,
    entries: Vec<TreeLeafEntry>,
    entry_reservation: Option<IndexReservation>,
    entry_ordinal: usize,
}
impl<'a> TreeRunCursor<'a> {
    fn open(
        publisher: &'a FixedFanoutTreePublisher<'a>,
        specification: TreeSpecification,
        run: TreeRun,
    ) -> Result<Self> {
        let metadata = publisher.staging.read_chunk(
            &IndexLocator::new(run.metadata_name(&specification)),
            IndexFileKind::OrderIndex,
        )?;
        let chunks = metadata
            .records
            .first()
            .and_then(|record| {
                record
                    .fields
                    .iter()
                    .find(|field| field.name == "tree-run-chunks")
            })
            .and_then(|field| <[u8; 8]>::try_from(field.bytes.as_slice()).ok())
            .map(u64::from_le_bytes)
            .ok_or_else(|| Error::protocol("typed tree", "invalid run metadata"))?;
        Ok(Self {
            publisher,
            specification,
            run,
            chunks,
            chunk_ordinal: 0,
            entries: Vec::new(),
            entry_reservation: None,
            entry_ordinal: 0,
        })
    }
    fn current(&mut self) -> Result<Option<&TreeLeafEntry>> {
        while self.entry_ordinal >= self.entries.len() && self.chunk_ordinal < self.chunks {
            let entries = TreeChunk::from_chunk(self.publisher.staging.read_chunk(
                &IndexLocator::new(self.run.chunk_name(&self.specification, self.chunk_ordinal)),
                IndexFileKind::OrderIndex,
            )?)?
            .entries;
            let bytes = entries
                .iter()
                .map(TreeLeafEntry::logical_bytes)
                .sum::<u64>();
            self.entry_reservation = Some(
                self.publisher
                    .meter
                    .reserve(IndexWorkCategory::DecodedChunk, bytes),
            );
            self.entries = entries;
            self.entry_ordinal = 0;
            self.chunk_ordinal += 1;
        }
        Ok(self.entries.get(self.entry_ordinal))
    }
    fn take_next(&mut self) -> Result<Option<TreeLeafEntry>> {
        let next = self.current()?.cloned();
        if next.is_some() {
            self.entry_ordinal += 1;
        }
        Ok(next)
    }
}

#[derive(Debug, Clone, Copy)]
struct TreeRunHead<'a> {
    cursors: &'a [TreeRunCursor<'a>],
}
impl<'a> TreeRunHead<'a> {
    fn new(cursors: &'a [TreeRunCursor<'a>]) -> Self {
        Self { cursors }
    }
    fn smallest(self) -> Option<usize> {
        self.cursors
            .iter()
            .enumerate()
            .filter_map(|(index, cursor)| {
                cursor
                    .entries
                    .get(cursor.entry_ordinal)
                    .map(|entry| (index, entry))
            })
            .min_by(|(_, left), (_, right)| TreeLeafEntry::compare(left, right))
            .map(|(index, _)| index)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TreeLeafEntry {
    key: String,
    reference: String,
    locator: String,
    range_max: Option<String>,
}
impl TreeLeafEntry {
    fn new(key: String, reference: String, locator: String) -> Self {
        Self {
            key,
            reference,
            locator,
            range_max: None,
        }
    }
    fn branch(key: String, range_max: String, locator: String) -> Self {
        Self {
            key,
            reference: String::new(),
            locator,
            range_max: Some(range_max),
        }
    }
    fn logical_bytes(&self) -> u64 {
        ("tree-key".len()
            + self.key.len()
            + "tree-key-hash".len()
            + 32
            + "tree-reference".len()
            + self.reference.len()
            + "tree-projection".len()
            + self.locator.len()
            + self
                .range_max
                .as_ref()
                .map_or(0, |value| "tree-range-max".len() + value.len())) as u64
    }
    fn compare(left: &Self, right: &Self) -> std::cmp::Ordering {
        left.key
            .cmp(&right.key)
            .then(left.reference.cmp(&right.reference))
            .then(left.locator.cmp(&right.locator))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TreeChunk {
    entries: Vec<TreeLeafEntry>,
    children: bool,
}
impl TreeChunk {
    fn entries(entries: Vec<TreeLeafEntry>) -> Self {
        Self {
            entries,
            children: false,
        }
    }
    fn children(entries: Vec<TreeLeafEntry>) -> Self {
        Self {
            entries,
            children: true,
        }
    }
    fn chunk(self) -> IndexChunk {
        IndexChunk {
            schema_version: 1,
            projection: None,
            records: self
                .entries
                .into_iter()
                .map(|entry| {
                    let key_hash = blake3::hash(entry.key.as_bytes());
                    IndexRecordDto {
                        schema_version: 1,
                        record_kind: if self.children { 63 } else { 62 },
                        fields: vec![
                            IndexFieldDto {
                                name: "tree-key".to_owned(),
                                bytes: entry.key.into_bytes(),
                            },
                            IndexFieldDto {
                                name: "tree-key-hash".to_owned(),
                                bytes: key_hash.as_bytes().to_vec(),
                            },
                            IndexFieldDto {
                                name: "tree-reference".to_owned(),
                                bytes: entry.reference.into_bytes(),
                            },
                            IndexFieldDto {
                                name: "tree-range-max".to_owned(),
                                bytes: entry.range_max.unwrap_or_default().into_bytes(),
                            },
                            IndexFieldDto {
                                name: if self.children {
                                    "tree-child".to_owned()
                                } else {
                                    "tree-projection".to_owned()
                                },
                                bytes: entry.locator.into_bytes(),
                            },
                        ],
                    }
                })
                .collect(),
        }
    }
    fn from_chunk(chunk: IndexChunk) -> Result<Self> {
        let children = chunk
            .records
            .first()
            .is_some_and(|record| record.record_kind == 63);
        let mut entries = Vec::with_capacity(chunk.records.len());
        for record in chunk.records {
            let value = |name: &str| {
                record
                    .fields
                    .iter()
                    .find(|field| field.name == name)
                    .map(|field| field.bytes.as_slice())
                    .ok_or_else(|| Error::protocol("typed tree", "missing node field"))
            };
            let key = String::from_utf8(value("tree-key")?.to_vec())
                .map_err(|_| Error::protocol("typed tree", "invalid key"))?;
            let hash = <[u8; 32]>::try_from(value("tree-key-hash")?)
                .map_err(|_| Error::protocol("typed tree", "invalid key hash"))?;
            if blake3::hash(key.as_bytes()).as_bytes() != &hash {
                return Err(Error::protocol(
                    "typed tree",
                    "key hash collision or corruption",
                ));
            }
            let reference = String::from_utf8(value("tree-reference")?.to_vec())
                .map_err(|_| Error::protocol("typed tree", "invalid reference"))?;
            let locator = String::from_utf8(
                value(if children {
                    "tree-child"
                } else {
                    "tree-projection"
                })?
                .to_vec(),
            )
            .map_err(|_| Error::protocol("typed tree", "invalid locator"))?;
            let range_max = String::from_utf8(value("tree-range-max")?.to_vec())
                .map_err(|_| Error::protocol("typed tree", "invalid range max"))?;
            let mut entry = TreeLeafEntry::new(key, reference, locator);
            entry.range_max = (!range_max.is_empty()).then_some(range_max);
            entries.push(entry);
        }
        if entries
            .windows(2)
            .any(|pair| TreeLeafEntry::compare(&pair[0], &pair[1]).is_gt())
        {
            return Err(Error::protocol(
                "typed tree",
                "non-deterministic node ordering",
            ));
        }
        Ok(Self { entries, children })
    }
    fn first_key(&self) -> Result<String> {
        self.entries
            .first()
            .map(|entry| entry.key.clone())
            .ok_or_else(|| Error::protocol("typed tree", "empty node"))
    }
    fn last_key(&self) -> Result<String> {
        self.entries
            .last()
            .map(|entry| entry.range_max.clone().unwrap_or_else(|| entry.key.clone()))
            .ok_or_else(|| Error::protocol("typed tree", "empty node"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IndexManifestRecord {
    records: Vec<IndexRecordDto>,
}

impl IndexManifestRecord {
    fn new(runs: &[SourceGenerationRun], roots: &[FixedTreeRoot]) -> Self {
        let mut records = runs
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
            .collect::<Vec<_>>();
        records.extend(roots.iter().map(FixedTreeRoot::manifest_record));
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

#[cfg(test)]
mod persistent_tree_tests {
    use std::{fs, path::PathBuf};

    use signal_aggregator::{SourceIdentifier, SourceKind, SubagentName, TranscriptBlockKind};
    use tempfile::TempDir;

    use super::*;
    use crate::output_index::PersistentIndex;

    fn limits() -> IndexStoreLimits {
        IndexStoreLimits {
            maximum_logical_chunk_bytes: 32 * 1024,
            maximum_serialized_chunk_bytes: 64 * 1024,
            maximum_records_per_chunk: 64,
            maximum_manifest_bytes: 64 * 1024,
            maximum_checkpoint_bytes: 4096,
            maximum_cursor_bytes: 4096,
            maximum_record_bytes: 4096,
            maximum_string_bytes: 1024,
            maximum_merge_fan_in: 2,
            maximum_query_candidates: 16,
            staging_generations_retained: 2,
        }
    }

    fn transcript(line_number: u64) -> TranscriptRecord {
        let path = PathBuf::from("/synthetic/persistent-tree.jsonl");
        TranscriptRecord::new(
            SourceKind::Claude,
            SourceIdentifier::new("claude:persistent-tree"),
            path.clone(),
            line_number,
            Some(signal_aggregator::Timestamp::new(format!(
                "2026-01-01T00:00:{line_number:02}Z"
            ))),
            format!("synthetic bounded record {line_number}"),
        )
        .with_subagent_name(Some(SubagentName::new("worker")))
        .with_blocks(vec![
            crate::adapter::TranscriptBlockSourceContext::new(
                SourceKind::Claude,
                SourceIdentifier::new("claude:persistent-tree"),
                path,
                line_number,
                Some(signal_aggregator::Timestamp::new(format!(
                    "2026-01-01T00:00:{line_number:02}Z"
                ))),
            )
            .readable_block(
                0,
                TranscriptBlockKind::AgentResponse,
                format!("synthetic block {line_number}"),
            ),
        ])
    }

    fn published(
        records: u64,
    ) -> (
        IndexStore,
        Vec<FixedTreeRoot>,
        PersistentIndex,
        IndexResourceMeter,
    ) {
        let root = TempDir::new().expect("temporary persistent tree root");
        let root_path = root.keep();
        let store = IndexStore::new(root_path.join("output-index"), limits());
        let staging = store.create_staging("tree-test").expect("staging");
        let meter = IndexResourceMeter::default();
        let mut builder = BoundedGenerationBuilder::new(
            staging.clone(),
            SourceKey::new(
                SourceKind::Claude,
                SourceIdentifier::new("claude:persistent-tree"),
                0,
            ),
            limits(),
            meter.clone(),
        );
        for line_number in 1..=records {
            builder.observe_record(transcript(line_number));
        }
        let run = builder.finish().expect("source run");
        let runs = vec![run];
        let roots = FixedFanoutTreePublisher::new(staging.clone(), limits(), &runs, meter.clone())
            .publish()
            .expect("tree publication");
        let manifest = IndexManifestRecord::new(&runs, &roots).chunk();
        let manifest_locator = IndexLocator::new("manifest");
        staging
            .write_chunk(&manifest_locator, IndexFileKind::Manifest, &manifest)
            .expect("manifest");
        let identity = SnapshotIdentity::new(&runs).with_roots(&roots).value();
        store
            .publish(&staging, &manifest_locator, identity)
            .expect("atomic publication");
        let index = PersistentIndex::from_typed_store_with_meter(&store, meter.clone())
            .expect("reopen persistent trees");
        (store, roots, index, meter)
    }

    #[test]
    fn publication_emits_every_order_and_relationship_root_and_reopens_them() {
        let (_store, roots, index, _meter) = published(5);
        let expected = TreeSpecification::all()
            .into_iter()
            .map(TreeSpecification::name)
            .collect::<std::collections::BTreeSet<_>>();
        let actual = index
            .persistent_tree_names()
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            actual, expected,
            "manifest preserves every deterministic root"
        );
        for root in &roots {
            let mut seen = Vec::new();
            let count = index
                .visit_persistent_tree(&root.specification.name(), 2, |key, reference| {
                    seen.push((key.to_owned(), reference.to_owned()));
                    true
                })
                .expect("bounded root traversal");
            assert!(count <= 2, "fixed visitor budget applies to every root");
            assert!(seen.windows(2).all(|pair| pair[0] <= pair[1]));
        }
        let mut relationship_references = Vec::new();
        let count = index
            .visit_persistent_tree("relationship:session-output:oldest", 16, |_, reference| {
                relationship_references.push(reference.to_owned());
                true
            })
            .expect("session output relationship traversal");
        assert_eq!(count, 5);
        assert_eq!(relationship_references.len(), 5);
    }

    #[test]
    fn external_merge_spans_more_than_fanout_without_directory_enumeration() {
        let (_store, roots, index, meter) = published(129);
        let output_root = roots
            .iter()
            .find(|root| root.specification.name() == "order:outputs:oldest")
            .expect("output chronology root");
        assert!(output_root.locator.is_some());
        let mut references = Vec::new();
        let count = index
            .visit_persistent_tree("order:outputs:oldest", 32, |_, reference| {
                references.push(reference.to_owned());
                true
            })
            .expect("multi-level traversal");
        assert_eq!(
            count,
            limits().maximum_query_candidates,
            "the visitor caps requested candidates at the persistent query limit"
        );
        assert_eq!(references.len(), limits().maximum_query_candidates as usize);
        let mut production_records = index.output_records();
        assert!(
            meter.snapshot().live_bytes > 0,
            "production list keeps projection reservations through card conversion"
        );
        let output = production_records
            .next()
            .expect("live collection projection");
        drop(production_records);
        assert_eq!(
            meter.snapshot().live_bytes,
            0,
            "production list drop releases bytes"
        );
        let production_lookup = index
            .output(&output.reference)
            .expect("guarded point lookup");
        assert!(
            meter.snapshot().live_bytes > 0,
            "production lookup keeps its projection reservation"
        );
        drop(production_lookup);
        assert_eq!(
            meter.snapshot().live_bytes,
            0,
            "production lookup drop releases bytes"
        );
        let page = index
            .persistent_tree_projection_page("outputs", 2)
            .expect("metered page owner");
        assert_eq!(page.projections().len(), 2);
        assert_eq!(page.reservation_count(), 2);
        assert!(
            meter.snapshot().live_bytes > 0,
            "page owns live reservations"
        );
        drop(page);
        assert_eq!(meter.snapshot().live_bytes, 0, "page drop releases bytes");
        let lookup = index
            .persistent_tree_lookup("outputs", output.reference.as_str())
            .expect("metered lookup")
            .expect("lookup projection");
        assert!(
            meter.snapshot().live_bytes > 0,
            "lookup owns live reservation"
        );
        drop(lookup);
        assert_eq!(meter.snapshot().live_bytes, 0, "lookup drop releases bytes");
        let (_larger_store, _larger_roots, larger_index, larger_meter) = published(257);
        larger_index
            .visit_persistent_tree("order:outputs:oldest", 32, |_, _| true)
            .expect("larger bounded traversal");
        assert_eq!(
            larger_index.output_records().count(),
            limits().maximum_query_candidates as usize,
            "live collection traversal is candidate-capped"
        );
        let fixed_high_water = 6 * limits().maximum_logical_chunk_bytes;
        assert!(
            meter.snapshot().high_water_bytes <= fixed_high_water
                && larger_meter.snapshot().high_water_bytes <= fixed_high_water,
            "reader and merge high water are bounded by the fixed fan-in/chunk formula"
        );
        assert_eq!(meter.snapshot().live_bytes, 0);
        assert_eq!(larger_meter.snapshot().live_bytes, 0);
    }

    fn relationship_output(session_reference: String, reference: &str) -> ProjectionRecordDto {
        ProjectionRecordDto::Output(ProjectionOutputDto {
            reference: reference.to_owned(),
            session_reference,
            subagent_reference: Some("subagent".to_owned()),
            title: None,
            task: None,
            source: 1,
            source_identifier: "source".to_owned(),
            authored_status: 1,
            produced_at: Some("2026-01-01T00:00:00Z".to_owned()),
            path: DiskPath::new(Vec::new(), "/synthetic/output".to_owned()),
            fingerprint_bytes: 1,
            fingerprint_seconds: 0,
            fingerprint_nanoseconds: 0,
            source_line_number: 1,
            text_hash: "hash".to_owned(),
            size: ProjectionSizeDto {
                byte_count: Some(1),
                line_count: Some(1),
                segment_count: Some(1),
                certainty: 1,
            },
            preview_text: String::new(),
            preview_original_bytes: 0,
        })
    }

    #[test]
    fn relationship_keys_group_parents_and_preserve_configured_source_isolation() {
        let first_source = SourceKey::new(
            SourceKind::Claude,
            SourceIdentifier::new("claude:shared"),
            0,
        );
        let second_source = SourceKey::new(
            SourceKind::Claude,
            SourceIdentifier::new("claude:shared"),
            1,
        );
        let first_session = StableReference::new(
            "session",
            first_source.scoped_reference_material("producer-session", "same"),
        )
        .as_string();
        let second_session = StableReference::new(
            "session",
            second_source.scoped_reference_material("producer-session", "same"),
        )
        .as_string();
        assert_ne!(first_session, second_session);
        let specification = TreeSpecification::all()
            .into_iter()
            .find(|specification| specification.name() == "relationship:session-output:oldest")
            .expect("session output relationship specification");
        let first = specification.entry(
            &relationship_output(first_session.clone(), "output-a"),
            "first".to_owned(),
        );
        let second = specification.entry(
            &relationship_output(second_session.clone(), "output-b"),
            "second".to_owned(),
        );
        let (Some(first), Some(second)) = (first, second) else {
            panic!("relationship output entries");
        };
        assert!(first.key.starts_with(&first_session));
        assert!(second.key.starts_with(&second_session));
        assert_ne!(first.key, second.key);
    }

    #[test]
    fn tree_key_corruption_rejects_reopened_traversal() {
        let (store, roots, index, _meter) = published(3);
        let root = roots
            .iter()
            .find(|root| root.specification.name() == "relationship:subagent-output:reference")
            .expect("relationship root");
        let pointer = store
            .read_current_pointer()
            .expect("pointer")
            .expect("published pointer");
        let generation = pointer
            .manifest_locator
            .rsplit_once('/')
            .expect("manifest parent")
            .0;
        let path = store
            .data_root()
            .join(generation)
            .join(root.locator.as_ref().expect("root locator"));
        let mut bytes = fs::read(&path).expect("node bytes");
        let last = bytes.last_mut().expect("non-empty node");
        *last ^= 0x01;
        fs::write(path, bytes).expect("corrupt node");
        assert!(
            index
                .visit_persistent_tree("relationship:subagent-output:reference", 1, |_, _| true)
                .is_err()
        );
    }
}
