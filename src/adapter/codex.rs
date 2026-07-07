use std::path::{Component, Path, PathBuf};

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
pub struct CodexTranscriptAdapter {
    root: TranscriptRootConfiguration,
}

impl CodexTranscriptAdapter {
    pub fn new(root: TranscriptRootConfiguration) -> Self {
        Self { root }
    }

    pub fn kind(&self) -> AdapterKind {
        AdapterKind::CodexTranscript
    }

    pub fn collect(&self, request: &TranscriptReadRequest) -> TranscriptReadOutcome {
        CodexSessionRootReader::new(self.root.path().to_path_buf()).collect(request)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexSessionRootReader {
    root: PathBuf,
    limits: TranscriptScanLimits,
}

impl CodexSessionRootReader {
    pub fn new(root: PathBuf) -> Self {
        Self::with_limits(root, TranscriptScanLimits::default_runtime())
    }

    pub fn with_limits(root: PathBuf, limits: TranscriptScanLimits) -> Self {
        Self { root, limits }
    }

    pub fn collect(&self, request: &TranscriptReadRequest) -> TranscriptReadOutcome {
        let source_identifier = self.source_identifier();
        if let Some(outcome) = request
            .unsupported_relative_window_outcome(SourceKind::Codex, source_identifier.clone())
        {
            return outcome;
        }
        self.read_records().project(request)
    }

    pub fn read_records(&self) -> TranscriptRawReadOutcome {
        let source_identifier = self.source_identifier();
        if !self.root.exists() {
            return TranscriptRawReadOutcome::new(
                SourceKind::Codex,
                source_identifier.clone(),
                Vec::new(),
                Vec::new(),
                vec![self.failure(ReadFailureReason::Missing, Some(self.root.clone()))],
            );
        }
        let session_files = match self.session_files() {
            Ok(session_files) => session_files,
            Err(error) => {
                return TranscriptRawReadOutcome::new(
                    SourceKind::Codex,
                    source_identifier.clone(),
                    Vec::new(),
                    Vec::new(),
                    vec![self.failure_from_io(error, Some(self.root.clone()))],
                );
            }
        };
        let mut records = Vec::new();
        let mut failures = TranscriptFailureAccumulator::new(
            SourceKind::Codex,
            Some(self.root.clone()),
            self.limits.maximum_failures(),
        );
        for failure in session_files.read_failures {
            failures.push(failure);
        }
        let mut truncations = session_files.truncations;
        for file in session_files.files {
            match TranscriptBoundedFile::new(file.clone(), self.limits.clone()).read_to_string() {
                Ok(TranscriptBoundedFileRead::Text(text)) => self.read_file_lines(
                    &file,
                    &text,
                    &mut records,
                    &mut failures,
                    &mut truncations,
                ),
                Ok(TranscriptBoundedFileRead::Truncated(truncation)) => {
                    truncations.push(truncation.into_truncation(SourceKind::Codex));
                }
                Err(error) => failures.push(self.failure_from_io(error, Some(file))),
            }
        }
        let failure_outcome = failures.finish();
        truncations.extend(failure_outcome.truncations);
        TranscriptRawReadOutcome::new(
            SourceKind::Codex,
            source_identifier,
            records,
            truncations,
            failure_outcome.failures,
        )
    }

    pub fn session_files(&self) -> std::io::Result<CodexSessionFiles> {
        let index_path = self.root.join("index.jsonl");
        if index_path.exists() {
            return CodexIndex::new(self.root.clone(), index_path, self.limits.clone())
                .session_files();
        }
        let discovery =
            TranscriptFileDiscovery::with_limits(self.root.clone(), self.limits.clone())
                .discover_jsonl_files()?;
        let mut files = discovery.files;
        files.retain(|path| path.file_name().is_none_or(|name| name != "index.jsonl"));
        Ok(CodexSessionFiles {
            files,
            read_failures: Vec::new(),
            truncations: discovery
                .truncations
                .into_iter()
                .map(|truncation| truncation.into_truncation(SourceKind::Codex))
                .collect(),
        })
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
                    truncations.push(truncation.into_truncation(SourceKind::Codex));
                    failures.push(self.failure(
                        ReadFailureReason::Malformed,
                        Some(TranscriptLineLocator::new(file.to_path_buf(), line_number).path()),
                    ));
                    continue;
                }
            };
            match CodexJsonlRecord::new(line).into_transcript_record(
                file.to_path_buf(),
                line_number,
                self.source_identifier(),
            ) {
                CodexJsonlRecordResult::Record(record) => records.push(record),
                CodexJsonlRecordResult::Malformed => {
                    failures.push(self.failure(
                        ReadFailureReason::Malformed,
                        Some(TranscriptLineLocator::new(file.to_path_buf(), line_number).path()),
                    ));
                }
            }
        }
    }

    pub fn source_identifier(&self) -> SourceIdentifier {
        SourceIdentifier::new(format!("codex:{}", self.root.display()))
    }

    pub fn failure(&self, reason: ReadFailureReason, path: Option<PathBuf>) -> ReadFailure {
        ReadFailure {
            source: SourceKind::Codex,
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
pub struct CodexSessionFiles {
    pub files: Vec<PathBuf>,
    pub read_failures: Vec<ReadFailure>,
    pub truncations: Vec<signal_aggregator::Truncation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexIndex {
    root: PathBuf,
    path: PathBuf,
    limits: TranscriptScanLimits,
}

impl CodexIndex {
    pub fn new(root: PathBuf, path: PathBuf, limits: TranscriptScanLimits) -> Self {
        Self { root, path, limits }
    }

    pub fn session_files(&self) -> std::io::Result<CodexSessionFiles> {
        let mut files = Vec::new();
        let mut read_failures = TranscriptFailureAccumulator::new(
            SourceKind::Codex,
            Some(self.path.clone()),
            self.limits.maximum_failures(),
        );
        let mut truncations = Vec::new();
        match TranscriptBoundedFile::new(self.path.clone(), self.limits.clone()).read_to_string()? {
            TranscriptBoundedFileRead::Text(text) => {
                for (line_index, line) in text.lines().enumerate() {
                    let line_number = line_index as u64 + 1;
                    let line_text =
                        TranscriptLineText::new(&self.path, line_number, line, self.limits.clone());
                    let line = match line_text.bounded_text() {
                        TranscriptLineTextOutcome::Text(line) => line,
                        TranscriptLineTextOutcome::Truncated(truncation) => {
                            truncations.push(truncation.into_truncation(SourceKind::Codex));
                            read_failures.push(self.read_failure(
                                ReadFailureReason::Malformed,
                                line_number,
                                None,
                            ));
                            continue;
                        }
                    };
                    let record = CodexIndexRecord::new(line).path();
                    match record {
                        CodexIndexRecordPath::Path(path) => {
                            match CodexIndexPath::new(self.root.clone(), path.clone())
                                .session_path()
                            {
                                Ok(path) => files.push(path),
                                Err(error) => read_failures.push(self.read_failure(
                                    match error.kind() {
                                        std::io::ErrorKind::PermissionDenied => {
                                            ReadFailureReason::PermissionDenied
                                        }
                                        std::io::ErrorKind::NotFound => ReadFailureReason::Missing,
                                        _ => ReadFailureReason::IoFailure,
                                    },
                                    line_number,
                                    Some(path),
                                )),
                            }
                        }
                        CodexIndexRecordPath::Malformed => read_failures.push(self.read_failure(
                            ReadFailureReason::Malformed,
                            line_number,
                            None,
                        )),
                    }
                }
            }
            TranscriptBoundedFileRead::Truncated(truncation) => {
                truncations.push(truncation.into_truncation(SourceKind::Codex));
            }
        }
        files.sort();
        let failure_outcome = read_failures.finish();
        truncations.extend(failure_outcome.truncations);
        Ok(CodexSessionFiles {
            files,
            read_failures: failure_outcome.failures,
            truncations,
        })
    }

    pub fn read_failure(
        &self,
        reason: ReadFailureReason,
        line_number: u64,
        offending_locator: Option<String>,
    ) -> ReadFailure {
        let mut source_identifier = format!(
            "codex:{}|index:{}|line:{}",
            self.root.display(),
            self.path.display(),
            line_number
        );
        if let Some(locator) = offending_locator {
            source_identifier.push_str("|locator:");
            source_identifier.push_str(&locator);
        }
        ReadFailure {
            source: SourceKind::Codex,
            path: Some(FilesystemPath::new(
                TranscriptLineLocator::new(self.path.clone(), line_number)
                    .path()
                    .display()
                    .to_string(),
            )),
            source_identifier: Some(SourceIdentifier::new(source_identifier)),
            reason,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexIndexPath {
    root: PathBuf,
    path: String,
}

impl CodexIndexPath {
    pub fn new(root: PathBuf, path: String) -> Self {
        Self { root, path }
    }

    pub fn session_path(&self) -> std::io::Result<PathBuf> {
        let path = Path::new(&self.path);
        let candidate = if path.is_absolute() {
            if self.has_parent_component(path) || !path.starts_with(&self.root) {
                return Err(self.outside_root_error());
            }
            path.to_path_buf()
        } else {
            if path
                .components()
                .any(|component| matches!(component, Component::ParentDir | Component::RootDir))
            {
                return Err(self.outside_root_error());
            }
            self.root.join(path)
        };
        self.root_bound_session_path(candidate)
    }

    pub fn root_bound_session_path(&self, candidate: PathBuf) -> std::io::Result<PathBuf> {
        if !candidate.exists() {
            return Ok(candidate);
        }
        let canonical_root = self.root.canonicalize()?;
        let canonical_candidate = candidate.canonicalize()?;
        if canonical_candidate.starts_with(canonical_root) {
            Ok(candidate)
        } else {
            Err(self.outside_root_error())
        }
    }

    pub fn has_parent_component(&self, path: &Path) -> bool {
        path.components()
            .any(|component| matches!(component, Component::ParentDir))
    }

    pub fn outside_root_error(&self) -> std::io::Error {
        std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "codex index path escapes configured root",
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexIndexRecord<'a> {
    line: &'a str,
}

impl<'a> CodexIndexRecord<'a> {
    pub fn new(line: &'a str) -> Self {
        Self { line }
    }

    pub fn path(&self) -> CodexIndexRecordPath {
        let Ok(value) = serde_json::from_str::<Value>(self.line) else {
            return CodexIndexRecordPath::Malformed;
        };
        value
            .get("path")
            .and_then(Value::as_str)
            .or_else(|| value.get("session_path").and_then(Value::as_str))
            .map(|path| CodexIndexRecordPath::Path(path.to_owned()))
            .unwrap_or(CodexIndexRecordPath::Malformed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexIndexRecordPath {
    Path(String),
    Malformed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexJsonlRecord<'a> {
    line: &'a str,
}

impl<'a> CodexJsonlRecord<'a> {
    pub fn new(line: &'a str) -> Self {
        Self { line }
    }

    pub fn into_transcript_record(
        self,
        path: PathBuf,
        line_number: u64,
        source_identifier: SourceIdentifier,
    ) -> CodexJsonlRecordResult {
        let value = match serde_json::from_str::<Value>(self.line) {
            Ok(value) => value,
            Err(_) => return CodexJsonlRecordResult::Malformed,
        };
        let timestamp = match CodexJsonValue::new(&value).timestamp() {
            Some(value) => {
                let timestamp = Timestamp::new(value.to_string());
                if CanonicalTimestamp::parse(&timestamp).is_err() {
                    return CodexJsonlRecordResult::Malformed;
                }
                Some(timestamp)
            }
            None => None,
        };
        let metadata = TranscriptJsonMetadata::new(&value);
        let context = TranscriptBlockSourceContext::new(
            SourceKind::Codex,
            source_identifier.clone(),
            path.clone(),
            line_number,
            timestamp.clone(),
        );
        let blocks = CodexJsonValue::new(&value).blocks(&context, metadata);
        let Some(text) = CodexJsonValue::new(&value)
            .text()
            .or_else(|| TranscriptBlockTextJoiner::new(&blocks).record_text())
        else {
            return CodexJsonlRecordResult::Malformed;
        };
        CodexJsonlRecordResult::Record(
            TranscriptRecord::new(
                SourceKind::Codex,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexJsonlRecordResult {
    Record(TranscriptRecord),
    Malformed,
}

#[derive(Debug, Clone, Copy)]
pub struct CodexJsonValue<'a> {
    value: &'a Value,
}

impl<'a> CodexJsonValue<'a> {
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
                    .get("payload")
                    .and_then(|payload| payload.get("role"))
                    .and_then(Value::as_str)
            })
            .or_else(|| {
                self.value
                    .get("payload")
                    .and_then(|payload| payload.get("message"))
                    .and_then(|message| message.get("role"))
                    .and_then(Value::as_str)
            })
    }

    pub fn blocks(
        &self,
        context: &TranscriptBlockSourceContext,
        metadata: TranscriptJsonMetadata<'a>,
    ) -> Vec<crate::adapter::TranscriptBlockRecord> {
        let mut blocks = Vec::new();
        {
            let mut collector = TranscriptBlockCollector::new(context, metadata, &mut blocks);
            if let Some(payload) = self.value.get("payload") {
                CodexPayload::new(payload, self.role()).push_blocks(&mut collector);
            }
            if let Some(item) = self.value.get("item") {
                CodexPayload::new(item, self.role()).push_blocks(&mut collector);
            }
            if let Some(content) = self.value.get("content") {
                CodexContent::new(content)
                    .push_readable(&mut collector, CodexRole::new(self.role()).text_kind());
            }
            if let Some(message) = self.value.get("message") {
                CodexContent::new(message)
                    .push_readable(&mut collector, CodexRole::new(self.role()).text_kind());
            }
        }
        blocks
    }

    pub fn text(&self) -> Option<String> {
        self.value
            .get("content")
            .and_then(|content| CodexContent::new(content).text())
            .or_else(|| {
                self.value
                    .get("message")
                    .and_then(|message| CodexContent::new(message).text())
            })
            .or_else(|| self.item_content())
            .or_else(|| self.payload_text())
    }

    pub fn item_content(&self) -> Option<String> {
        self.value
            .get("item")
            .and_then(|item| item.get("content"))
            .and_then(|content| CodexContent::new(content).text())
    }

    pub fn payload_text(&self) -> Option<String> {
        self.value
            .get("payload")
            .and_then(|payload| CodexPayload::new(payload, self.role()).text())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CodexRole<'a> {
    role: Option<&'a str>,
}

impl<'a> CodexRole<'a> {
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
pub struct CodexPayload<'a> {
    value: &'a Value,
    role: Option<&'a str>,
}

impl<'a> CodexPayload<'a> {
    pub fn new(value: &'a Value, role: Option<&'a str>) -> Self {
        Self { value, role }
    }

    pub fn payload_type(&self) -> &'a str {
        self.value.get("type").and_then(Value::as_str).unwrap_or("")
    }

    pub fn push_blocks(&self, collector: &mut TranscriptBlockCollector<'_, 'a>) {
        match self.payload_type().to_ascii_lowercase().as_str() {
            "message" => self.push_message(collector),
            "user_message" | "usermessage" => self.push_user_message(collector),
            "agent_message" | "agentmessage" => self.push_agent_message(collector),
            "function_call" | "functioncall" | "custom_tool_call" | "customtoolcall"
            | "tool_search_call" | "toolsearchcall" => {
                self.push_serialized(collector, TranscriptBlockKind::ToolCall)
            }
            "function_call_output"
            | "functioncalloutput"
            | "custom_tool_call_output"
            | "customtoolcalloutput"
            | "tool_search_output"
            | "toolsearchoutput" => self.push_tool_result(collector),
            "reasoning" => self.push_reasoning(collector),
            _ => self.push_fallback(collector),
        }
    }

    pub fn text(&self) -> Option<String> {
        match self.payload_type().to_ascii_lowercase().as_str() {
            "message" => self.message_text(),
            "user_message" | "usermessage" => self.user_message_text(),
            "agent_message" | "agentmessage" => self.agent_message_text(),
            "function_call_output"
            | "functioncalloutput"
            | "custom_tool_call_output"
            | "customtoolcalloutput"
            | "tool_search_output"
            | "toolsearchoutput" => self.tool_result_text(),
            "reasoning" => self.reasoning_text(),
            _ => self
                .value
                .get("content")
                .and_then(|content| CodexContent::new(content).text()),
        }
    }

    pub fn push_message(&self, collector: &mut TranscriptBlockCollector<'_, 'a>) {
        let kind = CodexRole::new(self.message_role()).text_kind();
        if let Some(content) = self.message_content() {
            CodexContent::new(content).push_readable(collector, kind);
        }
    }

    pub fn push_user_message(&self, collector: &mut TranscriptBlockCollector<'_, 'a>) {
        if let Some(text) = self.user_message_text() {
            collector.push_readable(TranscriptBlockKind::UserPrompt, text);
        }
        self.push_user_attachments(collector);
    }

    pub fn push_agent_message(&self, collector: &mut TranscriptBlockCollector<'_, 'a>) {
        if let Some(text) = self.agent_message_text() {
            collector.push_readable(TranscriptBlockKind::AgentResponse, text);
        }
    }

    pub fn push_tool_result(&self, collector: &mut TranscriptBlockCollector<'_, 'a>) {
        if let Some(text) = self.tool_result_text() {
            collector.push_readable(TranscriptBlockKind::ToolResult, text);
        } else {
            self.push_serialized(collector, TranscriptBlockKind::ToolResult);
        }
    }

    pub fn push_reasoning(&self, collector: &mut TranscriptBlockCollector<'_, 'a>) {
        if let Some(text) = self.reasoning_text() {
            collector.push_readable(TranscriptBlockKind::Inference, text);
        }
    }

    pub fn push_fallback(&self, collector: &mut TranscriptBlockCollector<'_, 'a>) {
        if let Some(text) = self.text() {
            collector.push_readable(CodexRole::new(self.role).text_kind(), text);
        } else if !self.payload_type().is_empty() {
            self.push_serialized(collector, TranscriptBlockKind::SessionEvent);
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

    pub fn message_role(&self) -> Option<&'a str> {
        self.value
            .get("role")
            .and_then(Value::as_str)
            .or(self.role)
            .or_else(|| {
                self.value
                    .get("message")
                    .and_then(|message| message.get("role"))
                    .and_then(Value::as_str)
            })
    }

    pub fn message_content(&self) -> Option<&'a Value> {
        self.value.get("content").or_else(|| {
            self.value
                .get("message")
                .and_then(|message| message.get("content"))
        })
    }

    pub fn message_text(&self) -> Option<String> {
        self.message_content()
            .and_then(|content| CodexContent::new(content).text())
    }

    pub fn user_message_text(&self) -> Option<String> {
        self.value
            .get("message")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| {
                self.value
                    .get("text")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .or_else(|| {
                self.value
                    .get("text_elements")
                    .and_then(|value| CodexContent::new(value).text())
            })
            .or_else(|| self.message_text())
    }

    pub fn agent_message_text(&self) -> Option<String> {
        self.value
            .get("message")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| self.message_text())
    }

    pub fn push_user_attachments(&self, collector: &mut TranscriptBlockCollector<'_, 'a>) {
        if ["images", "local_images"].iter().any(|field| {
            self.value
                .get(field)
                .and_then(Value::as_array)
                .is_some_and(|items| !items.is_empty())
        }) {
            collector.push_unavailable(TranscriptBlockKind::Attachment);
        }
    }

    pub fn tool_result_text(&self) -> Option<String> {
        self.value
            .get("output")
            .and_then(|value| CodexContent::new(value).text())
            .or_else(|| {
                self.value
                    .get("content")
                    .and_then(|value| CodexContent::new(value).text())
            })
    }

    pub fn reasoning_text(&self) -> Option<String> {
        self.value
            .get("summary")
            .and_then(|value| CodexContent::new(value).text())
            .or_else(|| {
                self.value
                    .get("content")
                    .and_then(|value| CodexContent::new(value).text())
            })
            .or_else(|| {
                self.value
                    .get("text")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CodexContent<'a> {
    value: &'a Value,
}

impl<'a> CodexContent<'a> {
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
                .filter_map(|item| CodexContent::new(item).text())
                .collect::<Vec<_>>();
            if texts.is_empty() {
                None
            } else {
                Some(texts.join("\n"))
            }
        } else if let Some(text) = self.value.get("text").and_then(Value::as_str) {
            Some(text.to_string())
        } else {
            self.value
                .get("content")
                .and_then(|content| CodexContent::new(content).text())
        }
    }

    pub fn push_readable(
        &self,
        collector: &mut TranscriptBlockCollector<'_, 'a>,
        kind: TranscriptBlockKind,
    ) {
        if let Some(text) = self.text() {
            collector.push_readable(kind, text);
        }
    }
}
