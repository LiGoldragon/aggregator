use std::path::{Path, PathBuf};

use serde_json::Value;
use signal_aggregator::{
    FilesystemPath, ReadFailure, ReadFailureReason, SourceIdentifier, SourceKind, Timestamp,
    TranscriptBlockKind,
};

use crate::{
    AdapterKind,
    adapter::{
        TranscriptBlockCollector, TranscriptBlockSourceContext, TranscriptBlockTextJoiner,
        TranscriptBoundedFile, TranscriptBoundedFileRead, TranscriptFailureAccumulator,
        TranscriptFileDiscovery, TranscriptJsonMetadata, TranscriptLineLocator, TranscriptLineText,
        TranscriptLineTextOutcome, TranscriptRawReadOutcome, TranscriptReadOutcome,
        TranscriptReadRequest, TranscriptRecord, TranscriptScanLimits,
    },
    configuration::TranscriptRootConfiguration,
    time_model::CanonicalTimestamp,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PiTranscriptAdapter {
    root: TranscriptRootConfiguration,
}

impl PiTranscriptAdapter {
    pub fn new(root: TranscriptRootConfiguration) -> Self {
        Self { root }
    }

    pub fn kind(&self) -> AdapterKind {
        AdapterKind::PiTranscript
    }

    pub fn collect(&self, request: &TranscriptReadRequest) -> TranscriptReadOutcome {
        PiRunHistoryRootReader::new(self.root.path().to_path_buf()).collect(request)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PiRunHistoryRootReader {
    root: PathBuf,
    limits: TranscriptScanLimits,
}

impl PiRunHistoryRootReader {
    pub fn new(root: PathBuf) -> Self {
        Self::with_limits(root, TranscriptScanLimits::default_runtime())
    }

    pub fn with_limits(root: PathBuf, limits: TranscriptScanLimits) -> Self {
        Self { root, limits }
    }

    pub fn collect(&self, request: &TranscriptReadRequest) -> TranscriptReadOutcome {
        let source_identifier = self.source_identifier();
        if let Some(outcome) =
            request.unsupported_relative_window_outcome(SourceKind::Pi, source_identifier.clone())
        {
            return outcome;
        }
        self.read_records().project(request)
    }

    pub fn read_records(&self) -> TranscriptRawReadOutcome {
        let source_identifier = self.source_identifier();
        if !self.root.exists() {
            return TranscriptRawReadOutcome::new(
                SourceKind::Pi,
                source_identifier.clone(),
                Vec::new(),
                Vec::new(),
                vec![self.failure(ReadFailureReason::Missing, Some(self.root.clone()))],
            );
        }
        let discovery =
            match TranscriptFileDiscovery::with_limits(self.root.clone(), self.limits.clone())
                .discover_jsonl_files()
            {
                Ok(discovery) => discovery,
                Err(error) => {
                    return TranscriptRawReadOutcome::new(
                        SourceKind::Pi,
                        source_identifier.clone(),
                        Vec::new(),
                        Vec::new(),
                        vec![self.failure_from_io(error, Some(self.root.clone()))],
                    );
                }
            };
        let mut records = Vec::new();
        let mut failures = TranscriptFailureAccumulator::new(
            SourceKind::Pi,
            Some(self.root.clone()),
            self.limits.maximum_failures(),
        );
        for failure in discovery.failures {
            failures.push(self.failure(failure.reason, Some(failure.path)));
        }
        let mut truncations = discovery
            .truncations
            .into_iter()
            .map(|truncation| truncation.into_truncation(SourceKind::Pi))
            .collect::<Vec<_>>();
        for file in discovery.files {
            match TranscriptBoundedFile::new(file.clone(), self.limits.clone()).read_to_string() {
                Ok(TranscriptBoundedFileRead::Text(text)) => self.read_file_lines(
                    &file,
                    &text,
                    &mut records,
                    &mut failures,
                    &mut truncations,
                ),
                Ok(TranscriptBoundedFileRead::Truncated(truncation)) => {
                    truncations.push(truncation.into_truncation(SourceKind::Pi));
                }
                Err(error) => failures.push(self.failure_from_io(error, Some(file))),
            }
        }
        let failure_outcome = failures.finish();
        truncations.extend(failure_outcome.truncations);
        TranscriptRawReadOutcome::new(
            SourceKind::Pi,
            source_identifier,
            records,
            truncations,
            failure_outcome.failures,
        )
    }

    pub fn read_file_lines(
        &self,
        file: &Path,
        text: &str,
        records: &mut Vec<TranscriptRecord>,
        failures: &mut TranscriptFailureAccumulator,
        truncations: &mut Vec<signal_aggregator::Truncation>,
    ) {
        for (line_index, line) in text.lines().enumerate() {
            let line_number = line_index as u64 + 1;
            let line_text = TranscriptLineText::new(file, line_number, line, self.limits.clone());
            let line = match line_text.bounded_text() {
                TranscriptLineTextOutcome::Text(line) => line,
                TranscriptLineTextOutcome::Truncated(truncation) => {
                    truncations.push(truncation.into_truncation(SourceKind::Pi));
                    failures.push(self.failure(
                        ReadFailureReason::Malformed,
                        Some(TranscriptLineLocator::new(file.to_path_buf(), line_number).path()),
                    ));
                    continue;
                }
            };
            match PiJsonlRecord::new(line).into_transcript_record(
                file.to_path_buf(),
                line_number,
                self.source_identifier(),
            ) {
                PiJsonlRecordResult::Record(record) => records.push(record),
                PiJsonlRecordResult::Malformed => {
                    failures.push(self.failure(
                        ReadFailureReason::Malformed,
                        Some(TranscriptLineLocator::new(file.to_path_buf(), line_number).path()),
                    ));
                }
            }
        }
    }

    pub fn source_identifier(&self) -> SourceIdentifier {
        SourceIdentifier::new(format!("pi:{}", self.root.display()))
    }

    pub fn failure(&self, reason: ReadFailureReason, path: Option<PathBuf>) -> ReadFailure {
        ReadFailure {
            source: SourceKind::Pi,
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
pub struct PiJsonlRecord<'a> {
    line: &'a str,
}

impl<'a> PiJsonlRecord<'a> {
    pub fn new(line: &'a str) -> Self {
        Self { line }
    }

    pub fn into_transcript_record(
        self,
        path: PathBuf,
        line_number: u64,
        source_identifier: SourceIdentifier,
    ) -> PiJsonlRecordResult {
        let value = match serde_json::from_str::<Value>(self.line) {
            Ok(value) => value,
            Err(_) => return PiJsonlRecordResult::Malformed,
        };
        let timestamp = match PiJsonValue::new(&value).timestamp() {
            Some(value) => {
                let timestamp = Timestamp::new(value.to_string());
                if CanonicalTimestamp::parse(&timestamp).is_err() {
                    return PiJsonlRecordResult::Malformed;
                }
                Some(timestamp)
            }
            None => None,
        };
        let metadata = TranscriptJsonMetadata::new(&value);
        let context = TranscriptBlockSourceContext::new(
            SourceKind::Pi,
            source_identifier.clone(),
            path.clone(),
            line_number,
            timestamp.clone(),
        );
        let blocks = PiJsonValue::new(&value).blocks(&context, metadata);
        let Some(text) = PiJsonValue::new(&value)
            .text()
            .or_else(|| TranscriptBlockTextJoiner::new(&blocks).record_text())
        else {
            return PiJsonlRecordResult::Malformed;
        };
        PiJsonlRecordResult::Record(
            TranscriptRecord::new(
                SourceKind::Pi,
                source_identifier,
                path,
                line_number,
                timestamp,
                text,
            )
            .with_title(metadata.title())
            .with_subagent_name(metadata.subagent_name())
            .with_authored_status(metadata.authored_status())
            .with_blocks(blocks),
        )
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PiJsonlRecordResult {
    Record(TranscriptRecord),
    Malformed,
}

#[derive(Debug, Clone, Copy)]
pub struct PiJsonValue<'a> {
    value: &'a Value,
}

impl<'a> PiJsonValue<'a> {
    pub fn new(value: &'a Value) -> Self {
        Self { value }
    }

    pub fn timestamp(&self) -> Option<&'a str> {
        self.value
            .get("timestamp")
            .and_then(Value::as_str)
            .or_else(|| self.value.get("started_at").and_then(Value::as_str))
    }

    pub fn role(&self) -> Option<&'a str> {
        self.value.get("role").and_then(Value::as_str).or_else(|| {
            self.value
                .get("message")
                .and_then(|message| message.get("role"))
                .and_then(Value::as_str)
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
                Some("custom_message") => self.push_custom_message(&mut collector),
                _ => {
                    if let Some(message) = self.value.get("message") {
                        PiMessage::new(message, self.role()).push_blocks(&mut collector);
                    }
                    if let Some(content) = self.value.get("content") {
                        PiContent::new(content, self.role()).push_blocks(&mut collector);
                    }
                    for field in ["text", "output"] {
                        if let Some(text) = self.value.get(field).and_then(Value::as_str) {
                            collector.push_readable(PiRole::new(self.role()).text_kind(), text);
                        }
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
            .or_else(|| self.value.get("output").and_then(Value::as_str))
            .map(ToOwned::to_owned)
            .or_else(|| {
                self.value
                    .get("content")
                    .and_then(|content| PiContent::new(content, self.role()).text())
            })
            .or_else(|| {
                self.value
                    .get("message")
                    .and_then(|message| PiMessage::new(message, self.role()).text())
            })
    }

    pub fn push_custom_message(&self, collector: &mut TranscriptBlockCollector<'_, 'a>) {
        if let Some(text) = self.value.get("content").and_then(Value::as_str) {
            collector.push_readable(TranscriptBlockKind::SessionEvent, text);
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PiRole<'a> {
    role: Option<&'a str>,
}

impl<'a> PiRole<'a> {
    pub fn new(role: Option<&'a str>) -> Self {
        Self { role }
    }

    pub fn text_kind(&self) -> TranscriptBlockKind {
        match self.role.map(str::to_ascii_lowercase).as_deref() {
            Some("user") => TranscriptBlockKind::UserPrompt,
            Some("system") => TranscriptBlockKind::SystemInstruction,
            Some("tool") | Some("toolresult") | Some("tool_result") => {
                TranscriptBlockKind::ToolResult
            }
            Some("assistant") => TranscriptBlockKind::AgentResponse,
            _ => TranscriptBlockKind::Unclassified,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PiMessage<'a> {
    value: &'a Value,
    fallback_role: Option<&'a str>,
}

impl<'a> PiMessage<'a> {
    pub fn new(value: &'a Value, fallback_role: Option<&'a str>) -> Self {
        Self {
            value,
            fallback_role,
        }
    }

    pub fn role(&self) -> Option<&'a str> {
        self.value
            .get("role")
            .and_then(Value::as_str)
            .or(self.fallback_role)
    }

    pub fn push_blocks(&self, collector: &mut TranscriptBlockCollector<'_, 'a>) {
        if let Some(content) = self.value.get("content") {
            PiContent::new(content, self.role()).push_blocks(collector);
        } else if let Some(text) = self.value.get("text").and_then(Value::as_str) {
            collector.push_readable(PiRole::new(self.role()).text_kind(), text);
        }
    }

    pub fn text(&self) -> Option<String> {
        self.value
            .get("content")
            .and_then(|content| PiContent::new(content, self.role()).text())
            .or_else(|| {
                self.value
                    .get("text")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PiContent<'a> {
    value: &'a Value,
    role: Option<&'a str>,
}

impl<'a> PiContent<'a> {
    pub fn new(value: &'a Value, role: Option<&'a str>) -> Self {
        Self { value, role }
    }

    pub fn push_blocks(&self, collector: &mut TranscriptBlockCollector<'_, 'a>) {
        if let Some(text) = self.value.as_str() {
            collector.push_readable(PiRole::new(self.role).text_kind(), text);
            return;
        }
        if let Some(array) = self.value.as_array() {
            for item in array {
                PiContentItem::new(item, self.role).push_block(collector);
            }
            return;
        }
        if self.value.is_object() {
            PiContentItem::new(self.value, self.role).push_block(collector);
        }
    }

    pub fn text(&self) -> Option<String> {
        if let Some(text) = self.value.as_str() {
            return Some(text.to_string());
        }
        if let Some(array) = self.value.as_array() {
            let texts = array
                .iter()
                .filter_map(|item| PiContentItem::new(item, self.role).text())
                .collect::<Vec<_>>();
            if texts.is_empty() {
                None
            } else {
                Some(texts.join("\n"))
            }
        } else {
            PiContentItem::new(self.value, self.role).text()
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PiContentItem<'a> {
    value: &'a Value,
    role: Option<&'a str>,
}

impl<'a> PiContentItem<'a> {
    pub fn new(value: &'a Value, role: Option<&'a str>) -> Self {
        Self { value, role }
    }

    pub fn item_type(&self) -> &'a str {
        self.value
            .get("type")
            .and_then(Value::as_str)
            .or_else(|| self.value.get("kind").and_then(Value::as_str))
            .unwrap_or("")
    }

    pub fn push_block(&self, collector: &mut TranscriptBlockCollector<'_, 'a>) {
        match self.item_type().to_ascii_lowercase().as_str() {
            "text" => self.push_text(collector, PiRole::new(self.role).text_kind()),
            "thinking" => self.push_text(collector, TranscriptBlockKind::Inference),
            "toolcall" | "tool_call" => {
                self.push_serialized(collector, TranscriptBlockKind::ToolCall)
            }
            "toolresult" | "tool_result" => self.push_tool_result(collector),
            "attachment" | "image" | "file" => {
                collector.push_unavailable(TranscriptBlockKind::Attachment)
            }
            _ => self.push_fallback(collector),
        }
    }

    pub fn text(&self) -> Option<String> {
        self.value
            .get("text")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| {
                self.value
                    .get("output")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .or_else(|| {
                self.value
                    .get("content")
                    .and_then(|content| PiContent::new(content, self.role).text())
            })
    }

    pub fn push_text(
        &self,
        collector: &mut TranscriptBlockCollector<'_, 'a>,
        kind: TranscriptBlockKind,
    ) {
        if let Some(text) = self.text() {
            collector.push_readable(kind, text);
        }
    }

    pub fn push_tool_result(&self, collector: &mut TranscriptBlockCollector<'_, 'a>) {
        if let Some(text) = self.text() {
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
        if let Some(text) = self.text() {
            collector.push_readable(PiRole::new(self.role).text_kind(), text);
        }
    }
}
