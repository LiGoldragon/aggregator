use std::path::{Path, PathBuf};

use serde_json::Value;
use signal_aggregator::{
    FilesystemPath, ReadFailure, ReadFailureReason, SessionIdentifier, SourceHealthStatus,
    SourceIdentifier, SourceKind, SourceLocator, SubagentTaskMetadata, TaskIdentifier, Timestamp,
    TranscriptBlockKind,
};

use crate::{
    AdapterKind,
    adapter::{
        TranscriptBlockCollector, TranscriptBlockSourceContext, TranscriptBlockTextJoiner,
        TranscriptBoundedFile, TranscriptBoundedFileRead, TranscriptBoundedLine,
        TranscriptFailureAccumulator, TranscriptFileAction, TranscriptFileCoverage,
        TranscriptFileDescriptor, TranscriptFileDiscovery, TranscriptFileShape, TranscriptFileSync,
        TranscriptJsonMetadata, TranscriptLineLocator, TranscriptRawReadOutcome,
        TranscriptReadOutcome, TranscriptReadRequest, TranscriptRecord, TranscriptRecordSink,
        TranscriptResumableScanOutcome, TranscriptScanCursor, TranscriptScanLimits,
        TranscriptScanRequest, TranscriptSymbolicLinkPolicy,
    },
    configuration::TranscriptRootConfiguration,
    time_model::CanonicalTimestamp,
};

#[derive(Debug)]
pub struct CountingTranscriptRecordSink<'a, S> {
    sink: &'a mut S,
    count: &'a mut u64,
}

impl<'a, S> CountingTranscriptRecordSink<'a, S> {
    pub fn new(sink: &'a mut S, count: &'a mut u64) -> Self {
        Self { sink, count }
    }
}

impl<S: TranscriptRecordSink> TranscriptRecordSink for CountingTranscriptRecordSink<'_, S> {
    fn observe_record(&mut self, record: TranscriptRecord) {
        *self.count += 1;
        self.sink.observe_record(record);
    }

    fn begin_source(&mut self, source: SourceKind, configured_occurrence: u64) {
        self.sink.begin_source(source, configured_occurrence);
    }

    fn begin_file(&mut self, descriptor: &TranscriptFileDescriptor) -> TranscriptFileAction {
        self.sink.begin_file(descriptor)
    }

    fn complete_file(&mut self, coverage: &TranscriptFileCoverage) -> TranscriptFileSync {
        self.sink.complete_file(coverage)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeTranscriptAdapter {
    root: TranscriptRootConfiguration,
}

impl ClaudeTranscriptAdapter {
    pub fn new(root: TranscriptRootConfiguration) -> Self {
        Self { root }
    }

    pub fn kind(&self) -> AdapterKind {
        AdapterKind::ClaudeTranscript
    }

    pub fn collect(&self, request: &TranscriptReadRequest) -> TranscriptReadOutcome {
        let reader = ClaudeJsonlRootReader::with_limits(
            self.root.path().to_path_buf(),
            self.root.scan_limits().clone(),
        );
        reader.collect(request)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaudeTranscriptFileShape {
    SessionJsonl,
    SubagentOutput,
}

impl ClaudeTranscriptFileShape {
    pub fn new(source: SourceKind) -> Self {
        match source {
            SourceKind::ClaudeSubagentOutput | SourceKind::PiSubagentOutput => Self::SubagentOutput,
            _ => Self::SessionJsonl,
        }
    }

    pub fn discovery_file_shape(self) -> TranscriptFileShape {
        match self {
            Self::SessionJsonl => TranscriptFileShape::Jsonl,
            Self::SubagentOutput => TranscriptFileShape::ClaudeSubagentOutput,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeJsonlRootReader {
    root: PathBuf,
    limits: TranscriptScanLimits,
    source: SourceKind,
    file_shape: ClaudeTranscriptFileShape,
}

impl ClaudeJsonlRootReader {
    pub fn new(root: PathBuf) -> Self {
        Self::with_limits_and_source(
            root,
            TranscriptScanLimits::default_runtime(),
            SourceKind::Claude,
        )
    }

    pub fn with_limits(root: PathBuf, limits: TranscriptScanLimits) -> Self {
        Self::with_limits_and_source(root, limits, SourceKind::Claude)
    }

    pub fn subagent_output(root: PathBuf) -> Self {
        Self::with_limits_and_source(
            root,
            TranscriptScanLimits::default_runtime(),
            SourceKind::ClaudeSubagentOutput,
        )
    }

    pub fn pi_subagent_output(root: PathBuf) -> Self {
        Self::with_limits_and_source(
            root,
            TranscriptScanLimits::default_runtime(),
            SourceKind::PiSubagentOutput,
        )
    }

    pub fn with_limits_and_source(
        root: PathBuf,
        limits: TranscriptScanLimits,
        source: SourceKind,
    ) -> Self {
        Self {
            root,
            limits,
            source,
            file_shape: ClaudeTranscriptFileShape::new(source),
        }
    }

    pub fn collect(&self, request: &TranscriptReadRequest) -> TranscriptReadOutcome {
        let source_identifier = self.source_identifier();
        if let Some(outcome) =
            request.unsupported_relative_window_outcome(self.source, source_identifier.clone())
        {
            return outcome;
        }
        self.read_records().project(request)
    }

    pub fn read_records(&self) -> TranscriptRawReadOutcome {
        let mut records = Vec::new();
        let mut outcome = self.scan_records(&mut records);
        outcome.records = records;
        outcome
    }

    /// Streams records to the caller; parsed JSON and record text are dropped after each callback.
    /// Scans in discovery order and advances the returned cursor only after the sink has made
    /// the corresponding file output durable. A valid cursor reuses only an unchanged prefix.
    pub fn scan_records_resumable<S: TranscriptRecordSink>(
        &self,
        request: &TranscriptScanRequest,
        sink: &mut S,
    ) -> TranscriptResumableScanOutcome {
        let source_identifier = self.source_identifier();
        sink.begin_source(self.source, request.configured_occurrence);
        let discovery = TranscriptFileDiscovery::with_limits_and_file_shape(
            self.root.clone(),
            self.limits.clone(),
            self.file_shape.discovery_file_shape(),
        )
        .with_symbolic_link_policy(self.symbolic_link_policy())
        .discover_files();
        let Ok(discovery) = discovery else {
            let outcome = self.scan_records(sink);
            return TranscriptResumableScanOutcome {
                cursor: TranscriptScanCursor::new(
                    self.source,
                    source_identifier,
                    request.configured_occurrence,
                    request.configuration_signature,
                    0,
                    [0; 32],
                ),
                completed_files: 0,
                resumed: false,
                outcome,
            };
        };
        let descriptors = discovery
            .files
            .iter()
            .enumerate()
            .map(|(ordinal, path)| TranscriptFileDescriptor::from_path(path, ordinal as u64))
            .collect::<std::io::Result<Vec<_>>>();
        let Ok(descriptors) = descriptors else {
            let outcome = self.scan_records(sink);
            return TranscriptResumableScanOutcome {
                cursor: TranscriptScanCursor::new(
                    self.source,
                    source_identifier,
                    request.configured_occurrence,
                    request.configuration_signature,
                    0,
                    [0; 32],
                ),
                completed_files: 0,
                resumed: false,
                outcome,
            };
        };
        let resume_files = request
            .resume_cursor
            .as_ref()
            .map_or(0, |cursor| cursor.next_discovery_ordinal);
        let resumed = request.accepts(self.source, &source_identifier)
            && resume_files <= descriptors.len() as u64
            && request.resume_cursor.as_ref().is_some_and(|cursor| {
                TranscriptFileDescriptor::prefix_digest(&descriptors[..resume_files as usize])
                    == cursor.completed_prefix_digest
            });
        let mut record_count = 0_u64;
        let mut counted = CountingTranscriptRecordSink::new(sink, &mut record_count);
        let mut failures = TranscriptFailureAccumulator::new(
            self.source,
            Some(self.root.clone()),
            self.limits.maximum_failures(),
        );
        for failure in discovery.failures {
            failures.push(self.failure(failure.reason, Some(failure.path)));
        }
        let mut scan_limits = discovery.scan_limits;
        let mut truncations = discovery
            .truncations
            .into_iter()
            .map(|truncation| truncation.into_truncation(self.source))
            .collect::<Vec<_>>();
        let mut completed_files = 0_u64;
        let mut next_ordinal = 0_u64;
        for (ordinal, (file, descriptor)) in discovery.files.iter().zip(&descriptors).enumerate() {
            let ordinal = ordinal as u64;
            let action = if resumed && ordinal < resume_files {
                TranscriptFileAction::ReuseSynced
            } else {
                counted.begin_file(descriptor)
            };
            if action == TranscriptFileAction::ReuseSynced {
                completed_files += 1;
                next_ordinal = ordinal + 1;
                continue;
            }
            let failure_count = failures.len();
            let truncation_count = truncations.len();
            self.read_file_lines(
                file,
                &mut counted,
                &mut failures,
                &mut truncations,
                &mut scan_limits,
            );
            let unchanged = TranscriptFileDescriptor::from_path(file, ordinal)
                .is_ok_and(|after| after.fingerprint == descriptor.fingerprint);
            let coverage = TranscriptFileCoverage {
                descriptor: descriptor.clone(),
                completed: unchanged
                    && failures.len() == failure_count
                    && truncations.len() == truncation_count,
            };
            if coverage.completed && counted.complete_file(&coverage) == TranscriptFileSync::Synced
            {
                completed_files += 1;
                next_ordinal = ordinal + 1;
            } else {
                break;
            }
        }
        let failure_outcome = failures.finish();
        truncations.extend(failure_outcome.truncations);
        scan_limits.extend(failure_outcome.scan_limits);
        let outcome = TranscriptRawReadOutcome::with_discovered_file_count(
            self.source,
            source_identifier.clone(),
            Vec::new(),
            truncations,
            failure_outcome.failures,
            descriptors.len() as u64,
        )
        .with_record_count(record_count)
        .with_scan_limits(scan_limits);
        let prefix = TranscriptFileDescriptor::prefix_digest(&descriptors[..next_ordinal as usize]);
        TranscriptResumableScanOutcome {
            outcome,
            cursor: TranscriptScanCursor::new(
                self.source,
                source_identifier,
                request.configured_occurrence,
                request.configuration_signature,
                next_ordinal,
                prefix,
            ),
            completed_files,
            resumed,
        }
    }

    pub fn scan_records<S: TranscriptRecordSink>(&self, sink: &mut S) -> TranscriptRawReadOutcome {
        let mut record_count = 0_u64;
        let mut counted = CountingTranscriptRecordSink::new(sink, &mut record_count);
        let outcome = self.scan_records_into(&mut counted);
        outcome.with_record_count(record_count)
    }

    pub fn scan_records_into<S: TranscriptRecordSink>(
        &self,
        sink: &mut S,
    ) -> TranscriptRawReadOutcome {
        let source_identifier = self.source_identifier();
        if !self.root.exists() {
            return TranscriptRawReadOutcome::with_discovered_file_count(
                self.source,
                source_identifier.clone(),
                Vec::new(),
                Vec::new(),
                vec![self.failure(ReadFailureReason::Missing, Some(self.root.clone()))],
                0,
            );
        }
        let discovery = match TranscriptFileDiscovery::with_limits_and_file_shape(
            self.root.clone(),
            self.limits.clone(),
            self.file_shape.discovery_file_shape(),
        )
        .with_symbolic_link_policy(self.symbolic_link_policy())
        .discover_files()
        {
            Ok(discovery) => discovery,
            Err(error) => {
                return TranscriptRawReadOutcome::with_discovered_file_count(
                    self.source,
                    source_identifier.clone(),
                    Vec::new(),
                    Vec::new(),
                    vec![self.failure_from_io(error, Some(self.root.clone()))],
                    0,
                );
            }
        };

        let mut scan_limits = discovery.scan_limits;
        let mut failures = TranscriptFailureAccumulator::new(
            self.source,
            Some(self.root.clone()),
            self.limits.maximum_failures(),
        );
        for failure in discovery.failures {
            failures.push(self.failure(failure.reason, Some(failure.path)));
        }
        let mut truncations = discovery
            .truncations
            .into_iter()
            .map(|truncation| truncation.into_truncation(self.source))
            .collect::<Vec<_>>();
        let discovered_files = discovery.files.len() as u64;
        for file in discovery.files {
            self.read_file_lines(
                &file,
                sink,
                &mut failures,
                &mut truncations,
                &mut scan_limits,
            );
        }
        let failure_outcome = failures.finish();
        truncations.extend(failure_outcome.truncations);
        scan_limits.extend(failure_outcome.scan_limits);
        TranscriptRawReadOutcome::with_discovered_file_count(
            self.source,
            source_identifier,
            Vec::new(),
            truncations,
            failure_outcome.failures,
            discovered_files,
        )
        .with_scan_limits(scan_limits)
    }

    pub fn read_file_lines<S: TranscriptRecordSink>(
        &self,
        file: &Path,
        sink: &mut S,
        failures: &mut TranscriptFailureAccumulator,
        truncations: &mut Vec<signal_aggregator::Truncation>,
        scan_limits: &mut Vec<signal_aggregator::ScanLimitReport>,
    ) {
        let mut receive = |bounded_line| match bounded_line {
            TranscriptBoundedLine::Text { line_number, text } => {
                match ClaudeJsonlRecord::new(&text).into_transcript_record(
                    self.source,
                    file.to_path_buf(),
                    line_number,
                    self.source_identifier(),
                ) {
                    ClaudeJsonlRecordResult::Record(record) => sink.observe_record(record),
                    ClaudeJsonlRecordResult::Malformed => failures.push(self.failure(
                        ReadFailureReason::Malformed,
                        Some(TranscriptLineLocator::new(file.to_path_buf(), line_number).path()),
                    )),
                }
            }
            TranscriptBoundedLine::Truncated(truncation) => {
                scan_limits.push(truncation.scan_limit_report());
                truncations.push(truncation.clone().into_truncation(self.source));
                failures.push(self.failure(ReadFailureReason::Malformed, truncation.path.clone()));
            }
        };
        match TranscriptBoundedFile::new(file.to_path_buf(), self.limits.clone())
            .read_lines(&mut receive)
        {
            Ok(TranscriptBoundedFileRead::Complete) => {}
            Ok(TranscriptBoundedFileRead::Truncated(truncation)) => {
                scan_limits.push(truncation.scan_limit_report());
                truncations.push(truncation.into_truncation(self.source));
            }
            Err(error) => failures.push(self.failure_from_io(error, Some(file.to_path_buf()))),
        }
    }

    pub fn symbolic_link_policy(&self) -> TranscriptSymbolicLinkPolicy {
        match self.source {
            SourceKind::ClaudeSubagentOutput => TranscriptSymbolicLinkPolicy::FollowOutputFileLinks,
            _ => TranscriptSymbolicLinkPolicy::ConfinedToRoot,
        }
    }

    pub fn source_identifier(&self) -> SourceIdentifier {
        SourceIdentifier::new(format!(
            "{}:{}",
            ClaudeSourceName::new(self.source).as_str(),
            self.root.display()
        ))
    }

    pub fn failure(&self, reason: ReadFailureReason, path: Option<PathBuf>) -> ReadFailure {
        ReadFailure {
            source: self.source,
            path: path.map(|value| FilesystemPath::new(value.display().to_string())),
            source_identifier: Some(self.source_identifier()),
            reason,
        }
    }

    pub fn failure_from_io(&self, error: std::io::Error, path: Option<PathBuf>) -> ReadFailure {
        let reason = match error.kind() {
            std::io::ErrorKind::NotFound => ReadFailureReason::Missing,
            std::io::ErrorKind::PermissionDenied => ReadFailureReason::PermissionDenied,
            _ => ReadFailureReason::IoFailure,
        };
        self.failure(reason, path)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeJsonlRecord<'a> {
    line: &'a str,
}

impl<'a> ClaudeJsonlRecord<'a> {
    pub fn new(line: &'a str) -> Self {
        Self { line }
    }

    pub fn into_transcript_record(
        self,
        source: SourceKind,
        path: PathBuf,
        line_number: u64,
        source_identifier: SourceIdentifier,
    ) -> ClaudeJsonlRecordResult {
        let value = match serde_json::from_str::<Value>(self.line) {
            Ok(value) => value,
            Err(_) => return ClaudeJsonlRecordResult::Malformed,
        };
        let timestamp = match ClaudeJsonValue::new(&value).timestamp() {
            Some(value) => {
                let timestamp = Timestamp::new(value.to_string());
                if CanonicalTimestamp::parse(&timestamp).is_err() {
                    return ClaudeJsonlRecordResult::Malformed;
                }
                Some(timestamp)
            }
            None => None,
        };
        let metadata = TranscriptJsonMetadata::new(&value);
        let context = TranscriptBlockSourceContext::new(
            source,
            source_identifier.clone(),
            path.clone(),
            line_number,
            timestamp.clone(),
        );
        let blocks = ClaudeJsonValue::new(&value).blocks(&context, metadata);
        let Some(text) = ClaudeJsonValue::new(&value)
            .text()
            .or_else(|| TranscriptBlockTextJoiner::new(&blocks).record_text())
        else {
            return ClaudeJsonlRecordResult::Malformed;
        };
        ClaudeJsonlRecordResult::Record(
            TranscriptRecord::new(
                source,
                source_identifier,
                path.clone(),
                line_number,
                timestamp,
                text,
            )
            .with_title(metadata.title())
            .with_subagent_name(metadata.subagent_name())
            .with_authored_status(metadata.authored_status())
            .with_session_identifier(ClaudeSessionIdentifier::new(&path).identifier())
            .with_task_metadata(
                metadata
                    .task_metadata()
                    .or_else(|| ClaudeOutputTaskIdentifier::new(&path).metadata()),
            )
            .with_blocks(blocks),
        )
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaudeJsonlRecordResult {
    Record(TranscriptRecord),
    Malformed,
}

#[derive(Debug, Clone, Copy)]
pub struct ClaudeJsonValue<'a> {
    value: &'a Value,
}

impl<'a> ClaudeJsonValue<'a> {
    pub fn new(value: &'a Value) -> Self {
        Self { value }
    }

    pub fn timestamp(&self) -> Option<&'a str> {
        self.value
            .get("timestamp")
            .and_then(Value::as_str)
            .or_else(|| self.value.get("created_at").and_then(Value::as_str))
    }

    pub fn role(&self) -> Option<&'a str> {
        self.value
            .get("role")
            .and_then(Value::as_str)
            .or_else(|| {
                self.value
                    .get("message")
                    .and_then(|message| message.get("role"))
                    .and_then(Value::as_str)
            })
            .or_else(|| match self.record_type() {
                Some("user" | "assistant" | "system") => self.record_type(),
                _ => None,
            })
    }

    pub fn record_type(&self) -> Option<&'a str> {
        self.value.get("type").and_then(Value::as_str)
    }

    pub fn blocks(
        &self,
        context: &TranscriptBlockSourceContext,
        metadata: TranscriptJsonMetadata<'a>,
    ) -> Vec<crate::adapter::TranscriptBlockRecord> {
        let mut blocks = Vec::new();
        {
            let mut collector = TranscriptBlockCollector::new(context, metadata, &mut blocks);
            match self.record_type().map(str::to_ascii_lowercase).as_deref() {
                Some("queue-operation") => self.push_queue_operation(&mut collector),
                Some("attachment") => collector.push_unavailable(TranscriptBlockKind::Attachment),
                _ => {
                    if let Some(thinking) = self.value.get("thinking").and_then(Value::as_str) {
                        collector.push_readable(TranscriptBlockKind::Inference, thinking);
                    }
                    if let Some(text) = self.value.get("text").and_then(Value::as_str) {
                        collector.push_readable(ClaudeRole::new(self.role()).text_kind(), text);
                    }
                    if let Some(content) = self
                        .value
                        .get("message")
                        .and_then(|message| message.get("content"))
                    {
                        ClaudeJsonContent::new(content, self.role()).push_blocks(&mut collector);
                    }
                    if let Some(content) = self.value.get("content") {
                        ClaudeJsonContent::new(content, self.role()).push_blocks(&mut collector);
                    }
                }
            }
        }
        blocks
    }

    pub fn text(&self) -> Option<String> {
        self.value
            .get("text")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| self.message_content_text())
            .or_else(|| self.content_text())
    }

    pub fn push_queue_operation(&self, collector: &mut TranscriptBlockCollector<'_, 'a>) {
        if let Some(text) = self.value.get("content").and_then(Value::as_str) {
            collector.push_readable(TranscriptBlockKind::SessionEvent, text);
        }
    }

    pub fn message_content_text(&self) -> Option<String> {
        self.value
            .get("message")
            .and_then(|message| message.get("content"))
            .and_then(|content| JsonContent::new(content).text())
    }

    pub fn content_text(&self) -> Option<String> {
        self.value
            .get("content")
            .and_then(|content| JsonContent::new(content).text())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct JsonContent<'a> {
    value: &'a Value,
}

impl<'a> JsonContent<'a> {
    pub fn new(value: &'a Value) -> Self {
        Self { value }
    }

    pub fn text(&self) -> Option<String> {
        if let Some(text) = self.value.as_str() {
            return Some(text.to_string());
        }
        if let Some(array) = self.value.as_array() {
            let texts = array
                .iter()
                .filter_map(|item| {
                    item.get("text")
                        .and_then(Value::as_str)
                        .or_else(|| item.get("thinking").and_then(Value::as_str))
                })
                .collect::<Vec<_>>();
            if texts.is_empty() {
                None
            } else {
                Some(texts.join("\n"))
            }
        } else {
            self.value
                .get("text")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ClaudeRole<'a> {
    role: Option<&'a str>,
}

impl<'a> ClaudeRole<'a> {
    pub fn new(role: Option<&'a str>) -> Self {
        Self { role }
    }

    pub fn text_kind(&self) -> TranscriptBlockKind {
        match self.role.map(str::to_ascii_lowercase).as_deref() {
            Some("user") => TranscriptBlockKind::UserPrompt,
            Some("system") => TranscriptBlockKind::SystemInstruction,
            Some("tool") | Some("tool_result") | Some("toolresult") => {
                TranscriptBlockKind::ToolResult
            }
            Some("assistant") => TranscriptBlockKind::AgentResponse,
            _ => TranscriptBlockKind::Unclassified,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ClaudeJsonContent<'a> {
    value: &'a Value,
    role: Option<&'a str>,
}

impl<'a> ClaudeJsonContent<'a> {
    pub fn new(value: &'a Value, role: Option<&'a str>) -> Self {
        Self { value, role }
    }

    pub fn push_blocks(&self, collector: &mut TranscriptBlockCollector<'_, 'a>) {
        if let Some(text) = self.value.as_str() {
            collector.push_readable(ClaudeRole::new(self.role).text_kind(), text);
            return;
        }
        if let Some(array) = self.value.as_array() {
            for item in array {
                ClaudeContentItem::new(item, self.role).push_block(collector);
            }
            return;
        }
        if self.value.is_object() {
            ClaudeContentItem::new(self.value, self.role).push_block(collector);
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ClaudeContentItem<'a> {
    value: &'a Value,
    role: Option<&'a str>,
}

impl<'a> ClaudeContentItem<'a> {
    pub fn new(value: &'a Value, role: Option<&'a str>) -> Self {
        Self { value, role }
    }

    pub fn push_block(&self, collector: &mut TranscriptBlockCollector<'_, 'a>) {
        match self.item_type().to_ascii_lowercase().as_str() {
            "text" => self.push_text(collector, ClaudeRole::new(self.role).text_kind()),
            "thinking" => self.push_thinking(collector),
            "tool_use" | "tooluse" => {
                self.push_serialized(collector, TranscriptBlockKind::ToolCall)
            }
            "tool_result" | "toolresult" => self.push_tool_result(collector),
            "image" | "document" | "attachment" => {
                collector.push_unavailable(TranscriptBlockKind::Attachment)
            }
            _ => self.push_fallback(collector),
        }
    }

    pub fn item_type(&self) -> &'a str {
        self.value.get("type").and_then(Value::as_str).unwrap_or("")
    }

    pub fn push_text(
        &self,
        collector: &mut TranscriptBlockCollector<'_, 'a>,
        kind: TranscriptBlockKind,
    ) {
        if let Some(text) = self.value.get("text").and_then(Value::as_str) {
            collector.push_readable(kind, text);
        }
    }

    pub fn push_thinking(&self, collector: &mut TranscriptBlockCollector<'_, 'a>) {
        if let Some(text) = self
            .value
            .get("thinking")
            .and_then(Value::as_str)
            .or_else(|| self.value.get("text").and_then(Value::as_str))
        {
            collector.push_readable(TranscriptBlockKind::Inference, text);
        }
    }

    pub fn push_tool_result(&self, collector: &mut TranscriptBlockCollector<'_, 'a>) {
        if let Some(text) = self
            .value
            .get("content")
            .and_then(|content| JsonContent::new(content).text())
            .or_else(|| {
                self.value
                    .get("text")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
        {
            collector.push_readable(TranscriptBlockKind::ToolResult, text);
        } else {
            self.push_serialized(collector, TranscriptBlockKind::ToolResult);
        }
    }

    pub fn push_serialized(
        &self,
        collector: &mut TranscriptBlockCollector<'_, 'a>,
        kind: TranscriptBlockKind,
    ) {
        if let Ok(text) = serde_json::to_string(self.value) {
            collector.push_readable(kind, text);
        }
    }

    pub fn push_fallback(&self, collector: &mut TranscriptBlockCollector<'_, 'a>) {
        if let Some(text) = self.value.get("text").and_then(Value::as_str) {
            collector.push_readable(ClaudeRole::new(self.role).text_kind(), text);
        } else if let Some(text) = self
            .value
            .get("content")
            .and_then(|content| JsonContent::new(content).text())
        {
            collector.push_readable(TranscriptBlockKind::Unclassified, text);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaudeSourceName {
    source: SourceKind,
}

impl ClaudeSourceName {
    pub fn new(source: SourceKind) -> Self {
        Self { source }
    }

    pub fn as_str(&self) -> &'static str {
        match self.source {
            SourceKind::Claude => "claude",
            SourceKind::ClaudeSubagentOutput => "claude-subagent-output",
            SourceKind::Codex => "codex",
            SourceKind::Pi => "pi",
            SourceKind::PiSubagentOutput => "pi-subagent-output",
            SourceKind::Repository => "repository",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeSessionIdentifier<'a> {
    path: &'a Path,
}

impl<'a> ClaudeSessionIdentifier<'a> {
    pub fn new(path: &'a Path) -> Self {
        Self { path }
    }

    pub fn identifier(&self) -> Option<SessionIdentifier> {
        let source = if self
            .path
            .extension()
            .is_some_and(|extension| extension == "output")
        {
            self.path
                .parent()
                .and_then(|parent| parent.file_name())
                .and_then(|value| value.to_str())
        } else {
            self.path.file_stem().and_then(|value| value.to_str())
        }?;
        Some(SessionIdentifier::new(
            source.strip_prefix("claude-").unwrap_or(source),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeOutputTaskIdentifier<'a> {
    path: &'a Path,
}

impl<'a> ClaudeOutputTaskIdentifier<'a> {
    pub fn new(path: &'a Path) -> Self {
        Self { path }
    }

    pub fn metadata(&self) -> Option<SubagentTaskMetadata> {
        let task_identifier = self
            .path
            .file_stem()
            .and_then(|value| value.to_str())?
            .strip_suffix(".output")
            .unwrap_or_else(|| {
                self.path
                    .file_stem()
                    .and_then(|value| value.to_str())
                    .unwrap_or("")
            });
        if task_identifier.is_empty() {
            return None;
        }
        Some(SubagentTaskMetadata {
            task_identifier: TaskIdentifier::new(task_identifier),
            title: None,
            tool_use_identifier: None,
            output_locator: Some(SourceLocator {
                root: FilesystemPath::new(self.path.display().to_string()),
                relative_path: None,
            }),
            source_status: SourceHealthStatus::ReadableIndexed,
            result: None,
            usage: None,
            duration: None,
        })
    }
}
