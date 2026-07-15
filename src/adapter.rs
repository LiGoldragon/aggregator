pub mod claude;
pub mod codex;
pub mod pi;
pub mod repository;

use std::{
    collections::BTreeSet,
    ffi::OsStr,
    io::{BufReader, Read},
    path::{Path, PathBuf},
};

use serde_json::Value;
use signal_aggregator::{
    AuthoredStatus, ByteCount, ByteLimit, ByteRange, FilesystemPath, ItemCount, LimitPolicy,
    LineNumber, LineRange, OutputTitle, Projection, ReadFailure, ReadFailureReason, ScanLimitKind,
    ScanLimitReport, SegmentProjection, SessionIdentifier, SourceHealthStatus, SourceIdentifier,
    SourceKind, SourceLocator, SourceVolume, SubagentName, SubagentTaskMetadata, TaskIdentifier,
    TaskResult, TaskTitle, TimeWindow, Timestamp, ToolUseIdentifier, TranscriptBlockKind,
    TranscriptBlockTextAvailability, TranscriptSegment, TranscriptSegmentIdentifier,
    TranscriptText, TranscriptTextExcerpt, Truncation, TruncationReason, UsageSummary,
};

use crate::time_model::CanonicalTimestamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AdapterKind {
    ClaudeTranscript,
    CodexTranscript,
    PiTranscript,
    ClaudeSubagentOutput,
    PiSubagentOutput,
    Repository,
}

impl AdapterKind {
    pub fn source_name(self) -> &'static str {
        match self {
            Self::ClaudeTranscript => "claude-transcript",
            Self::CodexTranscript => "codex-transcript",
            Self::PiTranscript => "pi-transcript",
            Self::ClaudeSubagentOutput => "claude-subagent-output",
            Self::PiSubagentOutput => "pi-subagent-output",
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

/// Receives one parsed transcript record while its source line is still bounded.
pub trait TranscriptRecordSink {
    fn observe_record(&mut self, record: TranscriptRecord);
}

impl TranscriptRecordSink for Vec<TranscriptRecord> {
    fn observe_record(&mut self, record: TranscriptRecord) {
        self.push(record);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptRawReadOutcome {
    pub source: SourceKind,
    pub source_identifier: SourceIdentifier,
    pub records: Vec<TranscriptRecord>,
    /// Counts parsed records even when a streaming caller elects not to retain them.
    pub record_count: u64,
    pub truncations: Vec<Truncation>,
    pub read_failures: Vec<signal_aggregator::ReadFailure>,
    pub discovered_files: u64,
    pub scan_limits: Vec<ScanLimitReport>,
}

impl TranscriptRawReadOutcome {
    pub fn new(
        source: SourceKind,
        source_identifier: SourceIdentifier,
        records: Vec<TranscriptRecord>,
        truncations: Vec<Truncation>,
        read_failures: Vec<signal_aggregator::ReadFailure>,
    ) -> Self {
        let discovered_files =
            TranscriptDiscoveredFiles::new(&records, &truncations, &read_failures).count();
        Self::with_discovered_file_count(
            source,
            source_identifier,
            records,
            truncations,
            read_failures,
            discovered_files,
        )
    }

    pub fn with_discovered_file_count(
        source: SourceKind,
        source_identifier: SourceIdentifier,
        records: Vec<TranscriptRecord>,
        truncations: Vec<Truncation>,
        read_failures: Vec<signal_aggregator::ReadFailure>,
        discovered_files: u64,
    ) -> Self {
        let record_count = records.len() as u64;
        Self {
            source,
            source_identifier,
            records,
            record_count,
            truncations,
            read_failures,
            discovered_files,
            scan_limits: Vec::new(),
        }
    }

    pub fn with_record_count(mut self, record_count: u64) -> Self {
        self.record_count = record_count;
        self
    }

    pub fn with_scan_limits(mut self, scan_limits: Vec<ScanLimitReport>) -> Self {
        self.scan_limits = scan_limits;
        self
    }

    pub fn is_complete(&self) -> bool {
        self.truncations.is_empty() && self.read_failures.is_empty() && self.scan_limits.is_empty()
    }

    pub fn empty(source: SourceKind, source_identifier: SourceIdentifier) -> Self {
        Self::new(
            source,
            source_identifier,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    }

    pub fn project(self, request: &TranscriptReadRequest) -> TranscriptReadOutcome {
        TranscriptReadOutcome::from_records_and_truncations(
            self.source,
            self.source_identifier,
            self.records,
            self.read_failures,
            self.truncations,
            request,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptDiscoveredFiles<'a> {
    records: &'a [TranscriptRecord],
    truncations: &'a [Truncation],
    read_failures: &'a [signal_aggregator::ReadFailure],
}

impl<'a> TranscriptDiscoveredFiles<'a> {
    pub fn new(
        records: &'a [TranscriptRecord],
        truncations: &'a [Truncation],
        read_failures: &'a [signal_aggregator::ReadFailure],
    ) -> Self {
        Self {
            records,
            truncations,
            read_failures,
        }
    }

    pub fn count(&self) -> u64 {
        let mut paths = BTreeSet::new();
        for record in self.records {
            paths.insert(record.path.display().to_string());
        }
        for truncation in self.truncations {
            if let Some(path) = &truncation.path {
                paths.insert(path.as_str().to_string());
            }
        }
        for failure in self.read_failures {
            if let Some(path) = &failure.path {
                paths.insert(path.as_str().to_string());
            }
        }
        paths.len() as u64
    }
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
    pub title: Option<OutputTitle>,
    pub subagent_name: Option<SubagentName>,
    pub authored_status: AuthoredStatus,
    pub session_identifier: Option<SessionIdentifier>,
    pub task_metadata: Option<SubagentTaskMetadata>,
    pub blocks: Vec<TranscriptBlockRecord>,
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
            title: None,
            subagent_name: None,
            authored_status: AuthoredStatus::AgentAuthored,
            session_identifier: None,
            task_metadata: None,
            blocks: Vec::new(),
        }
    }

    pub fn with_title(mut self, title: Option<OutputTitle>) -> Self {
        self.title = title;
        self
    }

    pub fn with_subagent_name(mut self, subagent_name: Option<SubagentName>) -> Self {
        self.subagent_name = subagent_name;
        self
    }

    pub fn with_authored_status(mut self, authored_status: AuthoredStatus) -> Self {
        self.authored_status = authored_status;
        self
    }

    pub fn with_task_metadata(mut self, task_metadata: Option<SubagentTaskMetadata>) -> Self {
        self.task_metadata = task_metadata;
        self
    }

    pub fn with_session_identifier(
        mut self,
        session_identifier: Option<SessionIdentifier>,
    ) -> Self {
        self.session_identifier = session_identifier;
        self
    }

    pub fn with_blocks(mut self, blocks: Vec<TranscriptBlockRecord>) -> Self {
        self.blocks = blocks;
        self
    }

    pub fn transcript_blocks(&self) -> Vec<TranscriptBlockRecord> {
        if !self.blocks.is_empty() {
            return self.blocks.clone();
        }
        vec![
            TranscriptBlockSourceContext::new(
                self.source,
                self.source_identifier.clone(),
                self.path.clone(),
                self.line_number,
                self.timestamp.clone(),
            )
            .readable_block(0, TranscriptBlockKind::AgentResponse, self.text.clone())
            .with_title(self.title.clone())
            .with_subagent_name(self.subagent_name.clone())
            .with_task_metadata(self.task_metadata.clone())
            .with_authored_status(self.authored_status),
        ]
    }

    pub fn byte_count(&self) -> u64 {
        self.text.len() as u64
    }

    pub fn line_count(&self) -> u64 {
        OutputLineCounter::new(&self.text).count()
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
            end: LineNumber::new(self.line_number + 1),
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
pub struct TranscriptBlockSourceContext {
    source: SourceKind,
    source_identifier: SourceIdentifier,
    path: PathBuf,
    line_number: u64,
    timestamp: Option<Timestamp>,
}

impl TranscriptBlockSourceContext {
    pub fn new(
        source: SourceKind,
        source_identifier: SourceIdentifier,
        path: PathBuf,
        line_number: u64,
        timestamp: Option<Timestamp>,
    ) -> Self {
        Self {
            source,
            source_identifier,
            path,
            line_number,
            timestamp,
        }
    }

    pub fn readable_block(
        &self,
        block_index: u64,
        kind: TranscriptBlockKind,
        text: String,
    ) -> TranscriptBlockRecord {
        TranscriptBlockRecord {
            source: self.source,
            source_identifier: self.source_identifier.clone(),
            path: self.path.clone(),
            line_number: self.line_number,
            block_index,
            kind,
            text_availability: TranscriptBlockTextAvailability::ReadableText,
            text: Some(text),
            timestamp: self.timestamp.clone(),
            title: None,
            subagent_name: None,
            authored_status: AuthoredStatus::UnknownAuthorship,
            task_metadata: None,
        }
    }

    pub fn unavailable_block(
        &self,
        block_index: u64,
        kind: TranscriptBlockKind,
    ) -> TranscriptBlockRecord {
        TranscriptBlockRecord {
            source: self.source,
            source_identifier: self.source_identifier.clone(),
            path: self.path.clone(),
            line_number: self.line_number,
            block_index,
            kind,
            text_availability: TranscriptBlockTextAvailability::UnavailableText,
            text: None,
            timestamp: self.timestamp.clone(),
            title: None,
            subagent_name: None,
            authored_status: AuthoredStatus::UnknownAuthorship,
            task_metadata: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptBlockRecord {
    pub source: SourceKind,
    pub source_identifier: SourceIdentifier,
    pub path: PathBuf,
    pub line_number: u64,
    pub block_index: u64,
    pub kind: TranscriptBlockKind,
    pub text_availability: TranscriptBlockTextAvailability,
    pub text: Option<String>,
    pub timestamp: Option<Timestamp>,
    pub title: Option<OutputTitle>,
    pub subagent_name: Option<SubagentName>,
    pub authored_status: AuthoredStatus,
    pub task_metadata: Option<SubagentTaskMetadata>,
}

impl TranscriptBlockRecord {
    pub fn with_title(mut self, title: Option<OutputTitle>) -> Self {
        self.title = title;
        self
    }

    pub fn with_subagent_name(mut self, subagent_name: Option<SubagentName>) -> Self {
        self.subagent_name = subagent_name;
        self
    }

    pub fn with_authored_status(mut self, authored_status: AuthoredStatus) -> Self {
        self.authored_status = authored_status;
        self
    }

    pub fn with_task_metadata(mut self, task_metadata: Option<SubagentTaskMetadata>) -> Self {
        self.task_metadata = task_metadata;
        self
    }

    pub fn with_metadata(mut self, metadata: &TranscriptJsonMetadata<'_>) -> Self {
        self.title = metadata.title();
        self.subagent_name = metadata.subagent_name();
        self.authored_status = metadata.authored_status();
        self.task_metadata = metadata.task_metadata();
        self
    }

    pub fn readable_text(&self) -> Option<&str> {
        if self.text_availability == TranscriptBlockTextAvailability::ReadableText {
            self.text.as_deref()
        } else {
            None
        }
    }

    pub fn byte_count(&self) -> Option<u64> {
        self.readable_text().map(|text| text.len() as u64)
    }

    pub fn line_count(&self) -> Option<u64> {
        self.readable_text()
            .map(|text| OutputLineCounter::new(text).count())
    }

    pub fn filesystem_path(&self) -> FilesystemPath {
        FilesystemPath::new(self.path.display().to_string())
    }

    pub fn line_range(&self) -> LineRange {
        LineRange {
            start: LineNumber::new(self.line_number),
            end: LineNumber::new(self.line_number + 1),
        }
    }
}

#[derive(Debug)]
pub struct TranscriptBlockCollector<'collector, 'json> {
    context: &'collector TranscriptBlockSourceContext,
    metadata: TranscriptJsonMetadata<'json>,
    blocks: &'collector mut Vec<TranscriptBlockRecord>,
}

impl<'collector, 'json> TranscriptBlockCollector<'collector, 'json> {
    pub fn new(
        context: &'collector TranscriptBlockSourceContext,
        metadata: TranscriptJsonMetadata<'json>,
        blocks: &'collector mut Vec<TranscriptBlockRecord>,
    ) -> Self {
        Self {
            context,
            metadata,
            blocks,
        }
    }

    pub fn push_readable(&mut self, kind: TranscriptBlockKind, text: impl Into<String>) {
        let text = text.into();
        if text.trim().is_empty() {
            return;
        }
        let block_index = self.blocks.len() as u64;
        self.blocks.push(
            self.context
                .readable_block(block_index, kind, text)
                .with_metadata(&self.metadata),
        );
    }

    pub fn push_unavailable(&mut self, kind: TranscriptBlockKind) {
        let block_index = self.blocks.len() as u64;
        self.blocks.push(
            self.context
                .unavailable_block(block_index, kind)
                .with_metadata(&self.metadata),
        );
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TranscriptBlockTextJoiner<'a> {
    blocks: &'a [TranscriptBlockRecord],
}

impl<'a> TranscriptBlockTextJoiner<'a> {
    pub fn new(blocks: &'a [TranscriptBlockRecord]) -> Self {
        Self { blocks }
    }

    pub fn text(&self) -> Option<String> {
        let readable = self
            .blocks
            .iter()
            .filter_map(|block| block.readable_text())
            .collect::<Vec<_>>();
        if readable.is_empty() {
            None
        } else {
            Some(readable.join("\n"))
        }
    }

    pub fn record_text(&self) -> Option<String> {
        self.text().or_else(|| {
            if self.blocks.is_empty() {
                None
            } else {
                Some(String::new())
            }
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputLineCounter<'a> {
    text: &'a str,
}

impl<'a> OutputLineCounter<'a> {
    pub fn new(text: &'a str) -> Self {
        Self { text }
    }

    pub fn count(&self) -> u64 {
        if self.text.is_empty() {
            0
        } else {
            self.text.lines().count() as u64
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TranscriptJsonMetadata<'a> {
    value: &'a Value,
}

impl<'a> TranscriptJsonMetadata<'a> {
    pub fn new(value: &'a Value) -> Self {
        Self { value }
    }

    pub fn title(&self) -> Option<OutputTitle> {
        self.string_field(&["title", "summary", "name"])
            .map(OutputTitle::new)
    }

    pub fn subagent_name(&self) -> Option<SubagentName> {
        self.string_field(&["subagent", "subagent_name", "agent_name"])
            .or_else(|| self.task_input_string_field(&["subagent_type", "agent_type"]))
            .map(SubagentName::new)
    }

    pub fn task_metadata(&self) -> Option<SubagentTaskMetadata> {
        let task_identifier = self
            .string_field(&["task_identifier", "task_id", "id"])
            .or_else(|| self.tool_use_identifier_value())?;
        Some(SubagentTaskMetadata {
            task_identifier: TaskIdentifier::new(task_identifier),
            title: self
                .string_field(&["task_title", "title", "description"])
                .or_else(|| self.task_input_string_field(&["description", "title"]))
                .map(TaskTitle::new),
            tool_use_identifier: self.tool_use_identifier_value().map(ToolUseIdentifier::new),
            output_locator: self.output_locator(),
            source_status: SourceHealthStatus::ReadableIndexed,
            result: self
                .string_field(&["result", "status"])
                .map(TaskResult::new),
            usage: self.usage_summary(),
            duration: None,
        })
    }

    pub fn output_locator(&self) -> Option<SourceLocator> {
        self.string_field(&["output_path", "output_file", "file_path"])
            .or_else(|| self.task_input_string_field(&["output_path", "output_file", "file_path"]))
            .map(|path| SourceLocator {
                root: FilesystemPath::new(path),
                relative_path: None,
            })
    }

    pub fn usage_summary(&self) -> Option<UsageSummary> {
        self.string_field(&["usage", "usage_summary"])
            .map(UsageSummary::new)
            .or_else(|| {
                self.value
                    .get("usage")
                    .filter(|value| value.is_object())
                    .map(|value| UsageSummary::new(value.to_string()))
            })
    }

    pub fn tool_use_identifier_value(&self) -> Option<&'a str> {
        self.string_field(&["tool_use_id", "tool_use_identifier"])
            .or_else(|| {
                self.value
                    .get("content")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .find(|item| {
                        item.get("type")
                            .and_then(Value::as_str)
                            .is_some_and(|value| value == "tool_use")
                            && item
                                .get("name")
                                .and_then(Value::as_str)
                                .is_some_and(|value| value == "Task")
                    })
                    .and_then(|item| item.get("id").and_then(Value::as_str))
            })
    }

    pub fn task_input_string_field(&self, names: &[&str]) -> Option<&'a str> {
        self.value
            .get("content")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|item| item.get("name").and_then(Value::as_str) == Some("Task"))
            .filter_map(|item| item.get("input"))
            .find_map(|input| {
                names
                    .iter()
                    .find_map(|name| input.get(*name).and_then(Value::as_str))
            })
            .filter(|value| !value.trim().is_empty())
    }

    pub fn authored_status(&self) -> AuthoredStatus {
        self.string_field(&["authored_status", "authorship"])
            .and_then(|value| AuthoredStatusName::new(value).authored_status())
            .or_else(|| {
                self.string_field(&["role", "type", "author"])
                    .and_then(|value| AuthoredStatusName::new(value).authored_status())
            })
            .unwrap_or(AuthoredStatus::AgentAuthored)
    }

    pub fn string_field(&self, names: &[&str]) -> Option<&'a str> {
        names
            .iter()
            .find_map(|name| self.value.get(*name).and_then(Value::as_str))
            .filter(|value| !value.trim().is_empty())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AuthoredStatusName<'a> {
    value: &'a str,
}

impl<'a> AuthoredStatusName<'a> {
    pub fn new(value: &'a str) -> Self {
        Self { value }
    }

    pub fn authored_status(&self) -> Option<AuthoredStatus> {
        match self.value.to_ascii_lowercase().as_str() {
            "agent" | "agent-authored" | "agentauthored" | "assistant" | "tool" | "subagent" => {
                Some(AuthoredStatus::AgentAuthored)
            }
            "human" | "human-authored" | "humanauthored" | "user" => {
                Some(AuthoredStatus::HumanAuthored)
            }
            "mixed" | "mixed-authorship" | "mixedauthorship" => {
                Some(AuthoredStatus::MixedAuthorship)
            }
            "unknown" | "unknown-authorship" | "unknownauthorship" => {
                Some(AuthoredStatus::UnknownAuthorship)
            }
            _ => None,
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
            MaximumScanEntries::new(131_072),
            MaximumDiscoveredFiles::new(32_768),
            MaximumFileBytes::new(8 * 1024 * 1024),
            MaximumLineBytes::new(256 * 1024),
            MaximumReadFailures::new(1024),
        ))
    }

    pub fn from_output_interface_limits(
        limits: &meta_signal_aggregator::OutputInterfaceLimitPolicy,
    ) -> Self {
        Self::new(TranscriptScanLimitConfiguration::new(
            MaximumScanEntries::new(limits.maximum_transcript_scan_entries.into_u64()),
            MaximumDiscoveredFiles::new(limits.maximum_transcript_discovered_files.into_u64()),
            MaximumFileBytes::new(limits.maximum_transcript_file_bytes.into_u64()),
            MaximumLineBytes::new(limits.maximum_transcript_line_bytes.into_u64()),
            MaximumReadFailures::new(limits.maximum_transcript_read_failures.into_u64()),
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
    pub kind: ScanLimitKind,
    pub limit: u64,
}

impl TranscriptLimitTruncation {
    pub fn new(path: Option<PathBuf>, original_bytes: Option<u64>, projected_bytes: u64) -> Self {
        Self::with_kind(
            path,
            original_bytes,
            projected_bytes,
            ScanLimitKind::FileBytes,
            projected_bytes,
        )
    }

    pub fn with_kind(
        path: Option<PathBuf>,
        original_bytes: Option<u64>,
        projected_bytes: u64,
        kind: ScanLimitKind,
        limit: u64,
    ) -> Self {
        Self {
            path,
            original_bytes,
            projected_bytes,
            kind,
            limit,
        }
    }

    pub fn scan_limit_report(&self) -> ScanLimitReport {
        ScanLimitReport {
            kind: self.kind.clone(),
            limit: ItemCount::new(self.limit),
            path: self
                .path
                .as_ref()
                .map(|path| FilesystemPath::new(path.display().to_string())),
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
    pub failures: Vec<TranscriptDiscoveryFailure>,
    pub scan_limits: Vec<ScanLimitReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptDiscoveryFailure {
    pub path: PathBuf,
    pub reason: ReadFailureReason,
}

impl TranscriptDiscoveryFailure {
    pub fn new(path: PathBuf, reason: ReadFailureReason) -> Self {
        Self { path, reason }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TranscriptFileShape {
    Jsonl,
    ClaudeSubagentOutput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TranscriptSymbolicLinkPolicy {
    ConfinedToRoot,
    FollowOutputFileLinks,
}

impl TranscriptFileShape {
    pub fn accepts_extension(self, extension: &OsStr) -> bool {
        match self {
            Self::Jsonl => extension == "jsonl",
            Self::ClaudeSubagentOutput => extension == "output",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptFileDiscovery {
    root: PathBuf,
    limits: TranscriptScanLimits,
    file_shape: TranscriptFileShape,
    symbolic_link_policy: TranscriptSymbolicLinkPolicy,
}

impl TranscriptFileDiscovery {
    pub fn new(root: PathBuf) -> Self {
        Self::with_limits(root, TranscriptScanLimits::default_runtime())
    }

    pub fn with_limits(root: PathBuf, limits: TranscriptScanLimits) -> Self {
        Self::with_limits_and_file_shape(root, limits, TranscriptFileShape::Jsonl)
    }

    pub fn with_limits_and_file_shape(
        root: PathBuf,
        limits: TranscriptScanLimits,
        file_shape: TranscriptFileShape,
    ) -> Self {
        Self {
            root,
            limits,
            file_shape,
            symbolic_link_policy: TranscriptSymbolicLinkPolicy::ConfinedToRoot,
        }
    }

    pub fn with_symbolic_link_policy(mut self, policy: TranscriptSymbolicLinkPolicy) -> Self {
        self.symbolic_link_policy = policy;
        self
    }

    pub fn jsonl_files(&self) -> std::io::Result<Vec<PathBuf>> {
        Ok(self.discover_jsonl_files()?.files)
    }

    pub fn discover_jsonl_files(&self) -> std::io::Result<TranscriptDiscoveryOutcome> {
        let discovery = Self::with_limits_and_file_shape(
            self.root.clone(),
            self.limits.clone(),
            TranscriptFileShape::Jsonl,
        )
        .with_symbolic_link_policy(self.symbolic_link_policy);
        discovery.discover_files()
    }

    pub fn discover_files(&self) -> std::io::Result<TranscriptDiscoveryOutcome> {
        let canonical_root = self.root.canonicalize()?;
        let mut state = TranscriptDiscoveryState::new(self.limits.clone());
        self.collect_files(&canonical_root, &canonical_root, &mut state)?;
        state.finish()
    }

    pub fn collect_jsonl_files(
        &self,
        directory: &Path,
        state: &mut TranscriptDiscoveryState,
    ) -> std::io::Result<()> {
        let canonical_root = self.root.canonicalize()?;
        let canonical_directory = directory.canonicalize()?;
        if !canonical_directory.starts_with(&canonical_root) {
            state.observe_discovery_failure(TranscriptDiscoveryFailure::new(
                directory.to_path_buf(),
                ReadFailureReason::PermissionDenied,
            ));
            return Ok(());
        }
        self.collect_files(&canonical_root, &canonical_directory, state)
    }

    pub fn collect_files(
        &self,
        canonical_root: &Path,
        directory: &Path,
        state: &mut TranscriptDiscoveryState,
    ) -> std::io::Result<()> {
        if state.is_complete() || !state.observe_directory(directory.to_path_buf()) {
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
            let file_type = entry.file_type()?;
            let canonical_path = match path.canonicalize() {
                Ok(path) => path,
                Err(error) => {
                    state.observe_discovery_failure(TranscriptDiscoveryFailure::new(
                        path,
                        match error.kind() {
                            std::io::ErrorKind::NotFound => ReadFailureReason::Missing,
                            std::io::ErrorKind::PermissionDenied => {
                                ReadFailureReason::PermissionDenied
                            }
                            _ => ReadFailureReason::IoFailure,
                        },
                    ));
                    continue;
                }
            };
            if file_type.is_dir() || file_type.is_symlink() && canonical_path.is_dir() {
                if canonical_path.starts_with(canonical_root) {
                    self.collect_files(canonical_root, &canonical_path, state)?;
                } else {
                    state.observe_discovery_failure(TranscriptDiscoveryFailure::new(
                        path,
                        ReadFailureReason::PermissionDenied,
                    ));
                }
                continue;
            }
            if file_type.is_symlink() && self.accepts_output_file_symlink(&path) {
                state.observe_transcript_file(path);
                continue;
            }
            if !canonical_path.starts_with(canonical_root) {
                state.observe_discovery_failure(TranscriptDiscoveryFailure::new(
                    path,
                    ReadFailureReason::PermissionDenied,
                ));
                continue;
            }
            if canonical_path
                .extension()
                .is_some_and(|extension| self.file_shape.accepts_extension(extension))
            {
                state.observe_transcript_file(canonical_path);
            }
        }
        Ok(())
    }

    pub fn accepts_output_file_symlink(&self, path: &Path) -> bool {
        self.symbolic_link_policy == TranscriptSymbolicLinkPolicy::FollowOutputFileLinks
            && self.file_shape == TranscriptFileShape::ClaudeSubagentOutput
            && path
                .extension()
                .is_some_and(|extension| self.file_shape.accepts_extension(extension))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptDiscoveryState {
    limits: TranscriptScanLimits,
    scanned_entries: u64,
    files: Vec<PathBuf>,
    truncations: Vec<TranscriptLimitTruncation>,
    failures: Vec<TranscriptDiscoveryFailure>,
    visited_directories: BTreeSet<PathBuf>,
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
            failures: Vec::new(),
            visited_directories: BTreeSet::new(),
            scan_limit_reported: false,
            file_limit_reported: false,
        }
    }

    pub fn observe_directory(&mut self, directory: PathBuf) -> bool {
        self.visited_directories.insert(directory)
    }

    pub fn observe_scan_entry(&mut self, directory: PathBuf) -> bool {
        if self.scanned_entries >= self.limits.maximum_scan_entries() {
            if !self.scan_limit_reported {
                self.truncations.push(TranscriptLimitTruncation::with_kind(
                    Some(directory),
                    None,
                    0,
                    ScanLimitKind::ScanEntries,
                    self.limits.maximum_scan_entries(),
                ));
                self.scan_limit_reported = true;
            }
            return false;
        }
        self.scanned_entries += 1;
        true
    }

    pub fn observe_transcript_file(&mut self, path: PathBuf) {
        if self.files.len() as u64 >= self.limits.maximum_discovered_files() {
            if !self.file_limit_reported {
                self.truncations.push(TranscriptLimitTruncation::with_kind(
                    Some(path),
                    None,
                    0,
                    ScanLimitKind::DiscoveredFiles,
                    self.limits.maximum_discovered_files(),
                ));
                self.file_limit_reported = true;
            }
            return;
        }
        self.files.push(path);
    }

    pub fn observe_discovery_failure(&mut self, failure: TranscriptDiscoveryFailure) {
        self.failures.push(failure);
    }

    pub fn is_complete(&self) -> bool {
        self.scan_limit_reported || self.file_limit_reported
    }

    pub fn finish(mut self) -> std::io::Result<TranscriptDiscoveryOutcome> {
        self.files.sort();
        let scan_limits = self
            .truncations
            .iter()
            .map(|truncation| ScanLimitReport {
                kind: truncation.kind.clone(),
                limit: ItemCount::new(truncation.limit),
                path: truncation
                    .path
                    .as_ref()
                    .map(|path| FilesystemPath::new(path.display().to_string())),
            })
            .collect();
        Ok(TranscriptDiscoveryOutcome {
            files: self.files,
            truncations: self.truncations,
            failures: self.failures,
            scan_limits,
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

    /// Delivers each line while retaining only a capped line and a fixed read buffer.
    ///
    /// A line beyond the configured cap is drained before the next line is delivered. This
    /// deliberately avoids `BufRead::read_line`, whose allocation follows hostile input.
    pub fn read_lines<F>(&self, receive: &mut F) -> std::io::Result<TranscriptBoundedFileRead>
    where
        F: FnMut(TranscriptBoundedLine),
    {
        let before = std::fs::metadata(&self.path)?;
        let byte_count = before.len();
        if byte_count > self.limits.maximum_file_bytes() {
            return Ok(TranscriptBoundedFileRead::Truncated(
                TranscriptLimitTruncation::new(
                    Some(self.path.clone()),
                    Some(byte_count),
                    self.limits.maximum_file_bytes(),
                ),
            ));
        }

        let maximum_line_bytes = self.limits.maximum_line_bytes() as usize;
        let mut reader = BufReader::with_capacity(8 * 1024, std::fs::File::open(&self.path)?);
        let mut input = [0_u8; 8 * 1024];
        let mut line = Vec::with_capacity(maximum_line_bytes.min(8 * 1024));
        let mut line_number = 1_u64;
        let mut line_bytes = 0_u64;
        let mut line_is_oversized = false;
        let mut saw_bytes = false;

        loop {
            let count = reader.read(&mut input)?;
            if count == 0 {
                break;
            }
            for byte in &input[..count] {
                saw_bytes = true;
                if *byte == b'\n' {
                    Self::deliver_line(
                        &self.path,
                        &self.limits,
                        line_number,
                        line_bytes,
                        line_is_oversized,
                        &mut line,
                        receive,
                    )?;
                    line_number += 1;
                    line_bytes = 0;
                    line_is_oversized = false;
                    continue;
                }
                line_bytes += 1;
                if line_bytes > self.limits.maximum_line_bytes() {
                    line_is_oversized = true;
                    continue;
                }
                line.push(*byte);
            }
        }
        if saw_bytes && (line_bytes > 0 || line_is_oversized) {
            Self::deliver_line(
                &self.path,
                &self.limits,
                line_number,
                line_bytes,
                line_is_oversized,
                &mut line,
                receive,
            )?;
        }

        let after = std::fs::metadata(&self.path)?;
        if before.len() != after.len() || before.modified().ok() != after.modified().ok() {
            return Err(std::io::Error::other(
                "transcript changed during bounded scan",
            ));
        }
        Ok(TranscriptBoundedFileRead::Complete)
    }

    fn deliver_line<F>(
        path: &Path,
        limits: &TranscriptScanLimits,
        line_number: u64,
        line_bytes: u64,
        line_is_oversized: bool,
        line: &mut Vec<u8>,
        receive: &mut F,
    ) -> std::io::Result<()>
    where
        F: FnMut(TranscriptBoundedLine),
    {
        if line_is_oversized {
            receive(TranscriptBoundedLine::Truncated(
                TranscriptLimitTruncation::with_kind(
                    Some(TranscriptLineLocator::new(path.to_path_buf(), line_number).path()),
                    Some(line_bytes),
                    limits.maximum_line_bytes(),
                    ScanLimitKind::LineBytes,
                    limits.maximum_line_bytes(),
                ),
            ));
        } else {
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            let text = std::str::from_utf8(line)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            receive(TranscriptBoundedLine::Text {
                line_number,
                text: text.to_owned(),
            });
        }
        line.clear();
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TranscriptBoundedFileRead {
    Complete,
    Truncated(TranscriptLimitTruncation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TranscriptBoundedLine {
    Text { line_number: u64, text: String },
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
            TranscriptLineTextOutcome::Truncated(TranscriptLimitTruncation::with_kind(
                Some(TranscriptLineLocator::new(self.path.to_path_buf(), self.line_number).path()),
                Some(self.text.len() as u64),
                self.limits.maximum_line_bytes(),
                ScanLimitKind::LineBytes,
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
        let limit = if self.truncated {
            Some(TranscriptLimitTruncation::with_kind(
                self.limit_path,
                None,
                0,
                ScanLimitKind::ReadFailures,
                self.limit,
            ))
        } else {
            None
        };
        let truncations = limit
            .iter()
            .cloned()
            .map(|truncation| truncation.into_truncation(self.source))
            .collect();
        let scan_limits = limit
            .iter()
            .map(TranscriptLimitTruncation::scan_limit_report)
            .collect();
        TranscriptFailureOutcome {
            failures: self.failures,
            truncations,
            scan_limits,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptFailureOutcome {
    pub failures: Vec<ReadFailure>,
    pub truncations: Vec<Truncation>,
    pub scan_limits: Vec<ScanLimitReport>,
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
