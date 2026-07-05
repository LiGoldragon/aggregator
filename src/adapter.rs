pub mod claude;
pub mod codex;
pub mod pi;
pub mod repository;

use std::path::{Path, PathBuf};

use signal_aggregator::{
    ByteCount, ByteLimit, ByteRange, FilesystemPath, ItemCount, LimitPolicy, LineNumber, LineRange,
    Projection, ReadFailure, ReadFailureReason, SegmentProjection, SourceIdentifier, SourceKind,
    SourceVolume, TimeWindow, Timestamp, TranscriptSegment, TranscriptSegmentIdentifier,
    TranscriptText, TranscriptTextExcerpt, Truncation, TruncationReason,
};

use crate::time_model::CanonicalTimestamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AdapterKind {
    ClaudeTranscript,
    CodexTranscript,
    PiTranscript,
    Repository,
}

impl AdapterKind {
    pub fn source_name(self) -> &'static str {
        match self {
            Self::ClaudeTranscript => "claude-transcript",
            Self::CodexTranscript => "codex-transcript",
            Self::PiTranscript => "pi-transcript",
            Self::Repository => "repository",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptReadRequest {
    pub time_window: TimeWindow,
    pub projection: Projection,
    pub limit_policy: LimitPolicy,
}

impl TranscriptReadRequest {
    pub fn new(time_window: TimeWindow, projection: Projection, limit_policy: LimitPolicy) -> Self {
        Self {
            time_window,
            projection,
            limit_policy,
        }
    }

    pub fn accepts_timestamp(&self, timestamp: Option<&Timestamp>) -> TimeWindowAcceptance {
        TimeWindowMatcher::new(self.time_window.clone()).accepts(timestamp)
    }

    pub fn unsupported_relative_window_outcome(
        &self,
        source: SourceKind,
        source_identifier: SourceIdentifier,
    ) -> Option<TranscriptReadOutcome> {
        if !matches!(self.time_window, TimeWindow::Recent(_)) {
            return None;
        }
        Some(TranscriptReadOutcome::from_records(
            source,
            source_identifier.clone(),
            Vec::new(),
            vec![ReadFailure {
                source,
                path: None,
                source_identifier: Some(source_identifier),
                reason: ReadFailureReason::UnsupportedFormat,
            }],
            self,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeWindowAcceptance {
    Accepted,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeWindowMatcher {
    time_window: TimeWindow,
}

impl TimeWindowMatcher {
    pub fn new(time_window: TimeWindow) -> Self {
        Self { time_window }
    }

    pub fn accepts(&self, timestamp: Option<&Timestamp>) -> TimeWindowAcceptance {
        let Some(timestamp) = timestamp else {
            return TimeWindowAcceptance::Rejected;
        };
        let Ok(candidate) = CanonicalTimestamp::parse(timestamp) else {
            return TimeWindowAcceptance::Rejected;
        };
        match &self.time_window {
            TimeWindow::Recent(_) => TimeWindowAcceptance::Rejected,
            TimeWindow::Since(start) => {
                let Ok(start) = CanonicalTimestamp::parse(start) else {
                    return TimeWindowAcceptance::Rejected;
                };
                if candidate.is_at_or_after(&start) {
                    TimeWindowAcceptance::Accepted
                } else {
                    TimeWindowAcceptance::Rejected
                }
            }
            TimeWindow::Range(range) => {
                let Ok(start) = CanonicalTimestamp::parse(&range.start) else {
                    return TimeWindowAcceptance::Rejected;
                };
                let Ok(end) = CanonicalTimestamp::parse(&range.end) else {
                    return TimeWindowAcceptance::Rejected;
                };
                if candidate.is_at_or_after(&start) && candidate.is_at_or_before(&end) {
                    TimeWindowAcceptance::Accepted
                } else {
                    TimeWindowAcceptance::Rejected
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptReadOutcome {
    pub source_volumes: Vec<SourceVolume>,
    pub transcript_segments: Vec<TranscriptSegment>,
    pub truncations: Vec<Truncation>,
    pub read_failures: Vec<signal_aggregator::ReadFailure>,
}

impl TranscriptReadOutcome {
    pub fn empty() -> Self {
        Self {
            source_volumes: Vec::new(),
            transcript_segments: Vec::new(),
            truncations: Vec::new(),
            read_failures: Vec::new(),
        }
    }

    pub fn from_records(
        source: SourceKind,
        source_identifier: SourceIdentifier,
        records: Vec<TranscriptRecord>,
        read_failures: Vec<signal_aggregator::ReadFailure>,
        request: &TranscriptReadRequest,
    ) -> Self {
        Self::from_records_and_truncations(
            source,
            source_identifier,
            records,
            read_failures,
            Vec::new(),
            request,
        )
    }

    pub fn from_records_and_truncations(
        source: SourceKind,
        source_identifier: SourceIdentifier,
        records: Vec<TranscriptRecord>,
        read_failures: Vec<signal_aggregator::ReadFailure>,
        read_truncations: Vec<Truncation>,
        request: &TranscriptReadRequest,
    ) -> Self {
        TranscriptProjectionBuilder::new(
            source,
            source_identifier,
            records,
            read_failures,
            read_truncations,
        )
        .project(request)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptRecord {
    pub source: SourceKind,
    pub source_identifier: SourceIdentifier,
    pub path: PathBuf,
    pub line_number: u64,
    pub timestamp: Option<Timestamp>,
    pub text: String,
}

impl TranscriptRecord {
    pub fn new(
        source: SourceKind,
        source_identifier: SourceIdentifier,
        path: PathBuf,
        line_number: u64,
        timestamp: Option<Timestamp>,
        text: String,
    ) -> Self {
        Self {
            source,
            source_identifier,
            path,
            line_number,
            timestamp,
            text,
        }
    }

    pub fn byte_count(&self) -> u64 {
        self.text.len() as u64
    }

    pub fn segment_identifier(&self) -> TranscriptSegmentIdentifier {
        TranscriptSegmentIdentifier::new(format!("{}:{}", self.path.display(), self.line_number))
    }

    pub fn filesystem_path(&self) -> FilesystemPath {
        FilesystemPath::new(self.path.display().to_string())
    }

    pub fn line_range(&self) -> LineRange {
        LineRange {
            start: LineNumber::new(self.line_number),
            end: LineNumber::new(self.line_number),
        }
    }

    pub fn byte_range(&self) -> ByteRange {
        ByteRange {
            start: ByteCount::new(0),
            end: ByteCount::new(self.byte_count()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptProjectionBuilder {
    source: SourceKind,
    source_identifier: SourceIdentifier,
    records: Vec<TranscriptRecord>,
    read_failures: Vec<signal_aggregator::ReadFailure>,
    read_truncations: Vec<Truncation>,
}

impl TranscriptProjectionBuilder {
    pub fn new(
        source: SourceKind,
        source_identifier: SourceIdentifier,
        records: Vec<TranscriptRecord>,
        read_failures: Vec<signal_aggregator::ReadFailure>,
        read_truncations: Vec<Truncation>,
    ) -> Self {
        Self {
            source,
            source_identifier,
            records,
            read_failures,
            read_truncations,
        }
    }

    pub fn project(self, request: &TranscriptReadRequest) -> TranscriptReadOutcome {
        let mut projection_state = TranscriptProjectionState::new(
            self.source,
            self.source_identifier.clone(),
            request.projection.clone(),
            request.limit_policy.clone(),
        );
        for record in self.records {
            if matches!(
                request.accepts_timestamp(record.timestamp.as_ref()),
                TimeWindowAcceptance::Accepted
            ) {
                projection_state.observe(record);
            }
        }
        projection_state.finish(self.read_failures, self.read_truncations)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptProjectionState {
    source: SourceKind,
    source_identifier: SourceIdentifier,
    projection: Projection,
    limit_policy: LimitPolicy,
    source_volume: SourceVolumeAccumulator,
    segments: Vec<TranscriptSegment>,
    truncations: Vec<Truncation>,
    projected_bytes: u64,
    segment_limit_truncated: SegmentLimitTruncation,
}

impl TranscriptProjectionState {
    pub fn new(
        source: SourceKind,
        source_identifier: SourceIdentifier,
        projection: Projection,
        limit_policy: LimitPolicy,
    ) -> Self {
        Self {
            source,
            source_identifier: source_identifier.clone(),
            projection,
            limit_policy,
            source_volume: SourceVolumeAccumulator::new(source, source_identifier),
            segments: Vec::new(),
            truncations: Vec::new(),
            projected_bytes: 0,
            segment_limit_truncated: SegmentLimitTruncation::NotTruncated,
        }
    }

    pub fn observe(&mut self, record: TranscriptRecord) {
        self.source_volume.observe(&record);
        if self.segments.len() as u64 >= self.limit_policy.maximum_segments.into_u64() {
            self.segment_limit_truncated = SegmentLimitTruncation::Truncated;
            return;
        }
        let projection = self.segment_projection(&record);
        self.segments.push(TranscriptSegment {
            source: record.source,
            source_identifier: record.source_identifier.clone(),
            segment_identifier: record.segment_identifier(),
            path: record.filesystem_path(),
            timestamp: record.timestamp.clone(),
            line_range: Some(record.line_range()),
            byte_range: Some(record.byte_range()),
            projection,
        });
    }

    pub fn finish(
        mut self,
        read_failures: Vec<signal_aggregator::ReadFailure>,
        read_truncations: Vec<Truncation>,
    ) -> TranscriptReadOutcome {
        if matches!(
            self.segment_limit_truncated,
            SegmentLimitTruncation::Truncated
        ) {
            self.truncations.push(Truncation {
                source: self.source,
                path: None,
                original_bytes: None,
                projected_bytes: ByteCount::new(self.projected_bytes),
                reason: TruncationReason::RequestLimit,
            });
        }
        self.truncations.extend(read_truncations);
        let source_volumes = self.source_volume.finish();
        TranscriptReadOutcome {
            source_volumes,
            transcript_segments: self.segments,
            truncations: self.truncations,
            read_failures,
        }
    }

    pub fn segment_projection(&mut self, record: &TranscriptRecord) -> SegmentProjection {
        match &self.projection {
            Projection::MetadataOnly => SegmentProjection::MetadataOnly,
            Projection::IdentifiersOnly => SegmentProjection::IdentifiersOnly,
            Projection::BoundedText(bound) => {
                let remaining_request_bytes = self
                    .limit_policy
                    .maximum_bytes
                    .into_u64()
                    .saturating_sub(self.projected_bytes);
                let truncation_reason = if remaining_request_bytes < bound.maximum_bytes.into_u64()
                {
                    TruncationReason::RequestLimit
                } else {
                    TruncationReason::ProjectionLimit
                };
                let text_limit = TextProjectionLimit::new(
                    ByteLimit::new(bound.maximum_bytes.into_u64().min(remaining_request_bytes)),
                    truncation_reason,
                );
                let excerpt = text_limit.project(record);
                self.projected_bytes += excerpt.byte_count.into_u64();
                if excerpt.truncation.is_some() {
                    self.truncations.push(Truncation {
                        source: record.source,
                        path: Some(record.filesystem_path()),
                        original_bytes: Some(ByteCount::new(record.byte_count())),
                        projected_bytes: excerpt.byte_count,
                        reason: truncation_reason,
                    });
                }
                SegmentProjection::Text(excerpt)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SegmentLimitTruncation {
    NotTruncated,
    Truncated,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextProjectionLimit {
    maximum_bytes: ByteLimit,
    truncation_reason: TruncationReason,
}

impl TextProjectionLimit {
    pub fn new(maximum_bytes: ByteLimit, truncation_reason: TruncationReason) -> Self {
        Self {
            maximum_bytes,
            truncation_reason,
        }
    }

    pub fn project(&self, record: &TranscriptRecord) -> TranscriptTextExcerpt {
        let selected_text = TruncatedText::new(&record.text, self.maximum_bytes).into_string();
        let projected_bytes = selected_text.len() as u64;
        let truncation = if projected_bytes < record.byte_count() {
            Some(Truncation {
                source: record.source,
                path: Some(record.filesystem_path()),
                original_bytes: Some(ByteCount::new(record.byte_count())),
                projected_bytes: ByteCount::new(projected_bytes),
                reason: self.truncation_reason,
            })
        } else {
            None
        };
        TranscriptTextExcerpt {
            text: TranscriptText::new(selected_text),
            byte_count: ByteCount::new(projected_bytes),
            truncation,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TruncatedText<'a> {
    text: &'a str,
    maximum_bytes: ByteLimit,
}

impl<'a> TruncatedText<'a> {
    pub fn new(text: &'a str, maximum_bytes: ByteLimit) -> Self {
        Self {
            text,
            maximum_bytes,
        }
    }

    pub fn into_string(self) -> String {
        let maximum_bytes = self.maximum_bytes.into_u64() as usize;
        if self.text.len() <= maximum_bytes {
            return self.text.to_string();
        }
        let mut boundary = maximum_bytes;
        while boundary > 0 && !self.text.is_char_boundary(boundary) {
            boundary -= 1;
        }
        self.text[..boundary].to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceVolumeAccumulator {
    source: SourceKind,
    source_identifier: SourceIdentifier,
    item_count: u64,
    byte_count: u64,
    earliest_timestamp: Option<Timestamp>,
    latest_timestamp: Option<Timestamp>,
}

impl SourceVolumeAccumulator {
    pub fn new(source: SourceKind, source_identifier: SourceIdentifier) -> Self {
        Self {
            source,
            source_identifier,
            item_count: 0,
            byte_count: 0,
            earliest_timestamp: None,
            latest_timestamp: None,
        }
    }

    pub fn observe(&mut self, record: &TranscriptRecord) {
        self.item_count += 1;
        self.byte_count += record.byte_count();
        if let Some(timestamp) = &record.timestamp {
            let Ok(candidate) = CanonicalTimestamp::parse(timestamp) else {
                return;
            };
            if self
                .earliest_timestamp
                .as_ref()
                .and_then(|value| CanonicalTimestamp::parse(value).ok())
                .is_none_or(|value| candidate.is_before(&value))
            {
                self.earliest_timestamp = Some(timestamp.clone());
            }
            if self
                .latest_timestamp
                .as_ref()
                .and_then(|value| CanonicalTimestamp::parse(value).ok())
                .is_none_or(|value| candidate.is_after(&value))
            {
                self.latest_timestamp = Some(timestamp.clone());
            }
        }
    }

    pub fn finish(self) -> Vec<SourceVolume> {
        if self.item_count == 0 {
            Vec::new()
        } else {
            vec![SourceVolume {
                source: self.source,
                source_identifier: self.source_identifier,
                item_count: ItemCount::new(self.item_count),
                byte_count: ByteCount::new(self.byte_count),
                earliest_timestamp: self.earliest_timestamp,
                latest_timestamp: self.latest_timestamp,
            }]
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaximumScanEntries(u64);

impl MaximumScanEntries {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn into_u64(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaximumDiscoveredFiles(u64);

impl MaximumDiscoveredFiles {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn into_u64(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaximumFileBytes(u64);

impl MaximumFileBytes {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn into_u64(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaximumLineBytes(u64);

impl MaximumLineBytes {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn into_u64(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaximumReadFailures(u64);

impl MaximumReadFailures {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn into_u64(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TranscriptScanLimitConfiguration {
    maximum_scan_entries: MaximumScanEntries,
    maximum_discovered_files: MaximumDiscoveredFiles,
    maximum_file_bytes: MaximumFileBytes,
    maximum_line_bytes: MaximumLineBytes,
    maximum_read_failures: MaximumReadFailures,
}

impl TranscriptScanLimitConfiguration {
    pub fn new(
        maximum_scan_entries: MaximumScanEntries,
        maximum_discovered_files: MaximumDiscoveredFiles,
        maximum_file_bytes: MaximumFileBytes,
        maximum_line_bytes: MaximumLineBytes,
        maximum_read_failures: MaximumReadFailures,
    ) -> Self {
        Self {
            maximum_scan_entries,
            maximum_discovered_files,
            maximum_file_bytes,
            maximum_line_bytes,
            maximum_read_failures,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptScanLimits {
    maximum_scan_entries: MaximumScanEntries,
    maximum_discovered_files: MaximumDiscoveredFiles,
    maximum_file_bytes: MaximumFileBytes,
    maximum_line_bytes: MaximumLineBytes,
    maximum_read_failures: MaximumReadFailures,
}

impl TranscriptScanLimits {
    pub fn default_runtime() -> Self {
        Self::new(TranscriptScanLimitConfiguration::new(
            MaximumScanEntries::new(4096),
            MaximumDiscoveredFiles::new(1024),
            MaximumFileBytes::new(8 * 1024 * 1024),
            MaximumLineBytes::new(256 * 1024),
            MaximumReadFailures::new(128),
        ))
    }

    pub fn new(configuration: TranscriptScanLimitConfiguration) -> Self {
        Self {
            maximum_scan_entries: configuration.maximum_scan_entries,
            maximum_discovered_files: configuration.maximum_discovered_files,
            maximum_file_bytes: configuration.maximum_file_bytes,
            maximum_line_bytes: configuration.maximum_line_bytes,
            maximum_read_failures: configuration.maximum_read_failures,
        }
    }

    pub fn maximum_scan_entries(&self) -> u64 {
        self.maximum_scan_entries.into_u64()
    }

    pub fn maximum_discovered_files(&self) -> u64 {
        self.maximum_discovered_files.into_u64()
    }

    pub fn maximum_file_bytes(&self) -> u64 {
        self.maximum_file_bytes.into_u64()
    }

    pub fn maximum_line_bytes(&self) -> u64 {
        self.maximum_line_bytes.into_u64()
    }

    pub fn maximum_failures(&self) -> u64 {
        self.maximum_read_failures.into_u64()
    }
}

impl Default for TranscriptScanLimits {
    fn default() -> Self {
        Self::default_runtime()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptLimitTruncation {
    pub path: Option<PathBuf>,
    pub original_bytes: Option<u64>,
    pub projected_bytes: u64,
}

impl TranscriptLimitTruncation {
    pub fn new(path: Option<PathBuf>, original_bytes: Option<u64>, projected_bytes: u64) -> Self {
        Self {
            path,
            original_bytes,
            projected_bytes,
        }
    }

    pub fn into_truncation(self, source: SourceKind) -> Truncation {
        Truncation {
            source,
            path: self
                .path
                .map(|value| FilesystemPath::new(value.display().to_string())),
            original_bytes: self.original_bytes.map(ByteCount::new),
            projected_bytes: ByteCount::new(self.projected_bytes),
            reason: TruncationReason::RequestLimit,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptDiscoveryOutcome {
    pub files: Vec<PathBuf>,
    pub truncations: Vec<TranscriptLimitTruncation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptFileDiscovery {
    root: PathBuf,
    limits: TranscriptScanLimits,
}

impl TranscriptFileDiscovery {
    pub fn new(root: PathBuf) -> Self {
        Self::with_limits(root, TranscriptScanLimits::default_runtime())
    }

    pub fn with_limits(root: PathBuf, limits: TranscriptScanLimits) -> Self {
        Self { root, limits }
    }

    pub fn jsonl_files(&self) -> std::io::Result<Vec<PathBuf>> {
        Ok(self.discover_jsonl_files()?.files)
    }

    pub fn discover_jsonl_files(&self) -> std::io::Result<TranscriptDiscoveryOutcome> {
        let mut state = TranscriptDiscoveryState::new(self.limits.clone());
        self.collect_jsonl_files(&self.root, &mut state)?;
        state.finish()
    }

    pub fn collect_jsonl_files(
        &self,
        directory: &Path,
        state: &mut TranscriptDiscoveryState,
    ) -> std::io::Result<()> {
        if state.is_complete() {
            return Ok(());
        }
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(directory)? {
            if !state.observe_scan_entry(directory.to_path_buf()) {
                break;
            }
            entries.push(entry?);
            if state.is_complete() {
                break;
            }
        }
        entries.sort_by_key(|entry| entry.path());
        for entry in entries {
            if state.is_complete() {
                break;
            }
            let path = entry.path();
            if path.is_dir() {
                self.collect_jsonl_files(&path, state)?;
            } else if path
                .extension()
                .is_some_and(|extension| extension == "jsonl")
            {
                state.observe_jsonl_file(path);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptDiscoveryState {
    limits: TranscriptScanLimits,
    scanned_entries: u64,
    files: Vec<PathBuf>,
    truncations: Vec<TranscriptLimitTruncation>,
    scan_limit_reported: bool,
    file_limit_reported: bool,
}

impl TranscriptDiscoveryState {
    pub fn new(limits: TranscriptScanLimits) -> Self {
        Self {
            limits,
            scanned_entries: 0,
            files: Vec::new(),
            truncations: Vec::new(),
            scan_limit_reported: false,
            file_limit_reported: false,
        }
    }

    pub fn observe_scan_entry(&mut self, directory: PathBuf) -> bool {
        if self.scanned_entries >= self.limits.maximum_scan_entries() {
            if !self.scan_limit_reported {
                self.truncations
                    .push(TranscriptLimitTruncation::new(Some(directory), None, 0));
                self.scan_limit_reported = true;
            }
            return false;
        }
        self.scanned_entries += 1;
        true
    }

    pub fn observe_jsonl_file(&mut self, path: PathBuf) {
        if self.files.len() as u64 >= self.limits.maximum_discovered_files() {
            if !self.file_limit_reported {
                self.truncations
                    .push(TranscriptLimitTruncation::new(Some(path), None, 0));
                self.file_limit_reported = true;
            }
            return;
        }
        self.files.push(path);
    }

    pub fn is_complete(&self) -> bool {
        self.scan_limit_reported || self.file_limit_reported
    }

    pub fn finish(mut self) -> std::io::Result<TranscriptDiscoveryOutcome> {
        self.files.sort();
        Ok(TranscriptDiscoveryOutcome {
            files: self.files,
            truncations: self.truncations,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptBoundedFile {
    path: PathBuf,
    limits: TranscriptScanLimits,
}

impl TranscriptBoundedFile {
    pub fn new(path: PathBuf, limits: TranscriptScanLimits) -> Self {
        Self { path, limits }
    }

    pub fn read_to_string(&self) -> std::io::Result<TranscriptBoundedFileRead> {
        let metadata = std::fs::metadata(&self.path)?;
        let byte_count = metadata.len();
        if byte_count > self.limits.maximum_file_bytes() {
            return Ok(TranscriptBoundedFileRead::Truncated(
                TranscriptLimitTruncation::new(
                    Some(self.path.clone()),
                    Some(byte_count),
                    self.limits.maximum_file_bytes(),
                ),
            ));
        }
        Ok(TranscriptBoundedFileRead::Text(std::fs::read_to_string(
            &self.path,
        )?))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TranscriptBoundedFileRead {
    Text(String),
    Truncated(TranscriptLimitTruncation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptLineText<'a> {
    path: &'a Path,
    line_number: u64,
    text: &'a str,
    limits: TranscriptScanLimits,
}

impl<'a> TranscriptLineText<'a> {
    pub fn new(
        path: &'a Path,
        line_number: u64,
        text: &'a str,
        limits: TranscriptScanLimits,
    ) -> Self {
        Self {
            path,
            line_number,
            text,
            limits,
        }
    }

    pub fn bounded_text(&self) -> TranscriptLineTextOutcome<'a> {
        if self.text.len() as u64 > self.limits.maximum_line_bytes() {
            TranscriptLineTextOutcome::Truncated(TranscriptLimitTruncation::new(
                Some(TranscriptLineLocator::new(self.path.to_path_buf(), self.line_number).path()),
                Some(self.text.len() as u64),
                self.limits.maximum_line_bytes(),
            ))
        } else {
            TranscriptLineTextOutcome::Text(self.text)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TranscriptLineTextOutcome<'a> {
    Text(&'a str),
    Truncated(TranscriptLimitTruncation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptFailureAccumulator {
    failures: Vec<ReadFailure>,
    limit: u64,
    source: SourceKind,
    limit_path: Option<PathBuf>,
    truncated: bool,
}

impl TranscriptFailureAccumulator {
    pub fn new(source: SourceKind, limit_path: Option<PathBuf>, limit: u64) -> Self {
        Self {
            failures: Vec::new(),
            limit,
            source,
            limit_path,
            truncated: false,
        }
    }

    pub fn push(&mut self, failure: ReadFailure) {
        if self.failures.len() as u64 >= self.limit {
            self.truncated = true;
            return;
        }
        self.failures.push(failure);
    }

    pub fn finish(self) -> TranscriptFailureOutcome {
        let truncations = if self.truncated {
            vec![
                TranscriptLimitTruncation::new(self.limit_path, None, 0)
                    .into_truncation(self.source),
            ]
        } else {
            Vec::new()
        };
        TranscriptFailureOutcome {
            failures: self.failures,
            truncations,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptFailureOutcome {
    pub failures: Vec<ReadFailure>,
    pub truncations: Vec<Truncation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptLineLocator {
    path: PathBuf,
    line_number: u64,
}

impl TranscriptLineLocator {
    pub fn new(path: PathBuf, line_number: u64) -> Self {
        Self { path, line_number }
    }

    pub fn path(&self) -> PathBuf {
        PathBuf::from(format!("{}:{}", self.path.display(), self.line_number))
    }
}
