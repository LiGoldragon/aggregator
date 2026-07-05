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
        match &self.time_window {
            TimeWindow::Recent(_) => TimeWindowAcceptance::Rejected,
            TimeWindow::Since(start) => timestamp.map_or(TimeWindowAcceptance::Rejected, |value| {
                if value.as_str() >= start.as_str() {
                    TimeWindowAcceptance::Accepted
                } else {
                    TimeWindowAcceptance::Rejected
                }
            }),
            TimeWindow::Range(range) => timestamp.map_or(TimeWindowAcceptance::Rejected, |value| {
                if value.as_str() >= range.start.as_str() && value.as_str() <= range.end.as_str() {
                    TimeWindowAcceptance::Accepted
                } else {
                    TimeWindowAcceptance::Rejected
                }
            }),
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
        TranscriptProjectionBuilder::new(source, source_identifier, records, read_failures)
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
}

impl TranscriptProjectionBuilder {
    pub fn new(
        source: SourceKind,
        source_identifier: SourceIdentifier,
        records: Vec<TranscriptRecord>,
        read_failures: Vec<signal_aggregator::ReadFailure>,
    ) -> Self {
        Self {
            source,
            source_identifier,
            records,
            read_failures,
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
        projection_state.finish(self.read_failures)
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
            if self
                .earliest_timestamp
                .as_ref()
                .is_none_or(|value| timestamp.as_str() < value.as_str())
            {
                self.earliest_timestamp = Some(timestamp.clone());
            }
            if self
                .latest_timestamp
                .as_ref()
                .is_none_or(|value| timestamp.as_str() > value.as_str())
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptFileDiscovery {
    root: PathBuf,
}

impl TranscriptFileDiscovery {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn jsonl_files(&self) -> std::io::Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        self.collect_jsonl_files(&self.root, &mut files)?;
        files.sort();
        Ok(files)
    }

    pub fn collect_jsonl_files(
        &self,
        directory: &Path,
        files: &mut Vec<PathBuf>,
    ) -> std::io::Result<()> {
        let mut entries = std::fs::read_dir(directory)?.collect::<std::io::Result<Vec<_>>>()?;
        entries.sort_by_key(|entry| entry.path());
        for entry in entries {
            let path = entry.path();
            if path.is_dir() {
                self.collect_jsonl_files(&path, files)?;
            } else if path
                .extension()
                .is_some_and(|extension| extension == "jsonl")
            {
                files.push(path);
            }
        }
        Ok(())
    }
}
