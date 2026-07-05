use std::path::{Component, Path, PathBuf};

use serde_json::Value;
use signal_aggregator::{
    FilesystemPath, ReadFailure, ReadFailureReason, SourceIdentifier, SourceKind, Timestamp,
};

use crate::{
    AdapterKind,
    adapter::{
        TranscriptBoundedFile, TranscriptBoundedFileRead, TranscriptFailureAccumulator,
        TranscriptFileDiscovery, TranscriptLineLocator, TranscriptLineText,
        TranscriptLineTextOutcome, TranscriptReadOutcome, TranscriptReadRequest, TranscriptRecord,
        TranscriptScanLimits,
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
        if !self.root.exists() {
            return TranscriptReadOutcome::from_records(
                SourceKind::Codex,
                source_identifier.clone(),
                Vec::new(),
                vec![self.failure(ReadFailureReason::Missing, Some(self.root.clone()))],
                request,
            );
        }
        let session_files = match self.session_files() {
            Ok(session_files) => session_files,
            Err(error) => {
                return TranscriptReadOutcome::from_records(
                    SourceKind::Codex,
                    source_identifier.clone(),
                    Vec::new(),
                    vec![self.failure_from_io(error, Some(self.root.clone()))],
                    request,
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
        TranscriptReadOutcome::from_records_and_truncations(
            SourceKind::Codex,
            source_identifier,
            records,
            failure_outcome.failures,
            truncations,
            request,
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
        let Some(text) = CodexJsonValue::new(&value).text() else {
            return CodexJsonlRecordResult::Malformed;
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
        CodexJsonlRecordResult::Record(TranscriptRecord::new(
            SourceKind::Codex,
            source_identifier,
            path,
            line_number,
            timestamp,
            text,
        ))
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

    pub fn text(&self) -> Option<String> {
        self.value
            .get("content")
            .and_then(Value::as_str)
            .or_else(|| self.value.get("message").and_then(Value::as_str))
            .map(ToOwned::to_owned)
            .or_else(|| self.item_content())
    }

    pub fn item_content(&self) -> Option<String> {
        self.value
            .get("item")
            .and_then(|item| item.get("content"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    }
}
