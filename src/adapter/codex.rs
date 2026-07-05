use std::path::{Component, Path, PathBuf};

use serde_json::Value;
use signal_aggregator::{
    FilesystemPath, ReadFailure, ReadFailureReason, SourceIdentifier, SourceKind, Timestamp,
};

use crate::{
    AdapterKind,
    adapter::{
        TranscriptFileDiscovery, TranscriptReadOutcome, TranscriptReadRequest, TranscriptRecord,
    },
    configuration::TranscriptRootConfiguration,
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
}

impl CodexSessionRootReader {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
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
        let files = match self.session_files() {
            Ok(files) => files,
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
        let mut failures = Vec::new();
        for file in files {
            match std::fs::read_to_string(&file) {
                Ok(text) => self.read_file_lines(&file, &text, &mut records, &mut failures),
                Err(error) => failures.push(self.failure_from_io(error, Some(file))),
            }
        }
        TranscriptReadOutcome::from_records(
            SourceKind::Codex,
            source_identifier,
            records,
            failures,
            request,
        )
    }

    pub fn session_files(&self) -> std::io::Result<Vec<PathBuf>> {
        let index_path = self.root.join("index.jsonl");
        if index_path.exists() {
            return CodexIndex::new(self.root.clone(), index_path).session_files();
        }
        let mut files = TranscriptFileDiscovery::new(self.root.clone()).jsonl_files()?;
        files.retain(|path| path.file_name().is_none_or(|name| name != "index.jsonl"));
        Ok(files)
    }

    pub fn read_file_lines(
        &self,
        file: &Path,
        text: &str,
        records: &mut Vec<TranscriptRecord>,
        failures: &mut Vec<ReadFailure>,
    ) {
        for (line_index, line) in text.lines().enumerate() {
            let line_number = line_index as u64 + 1;
            match CodexJsonlRecord::new(line).into_transcript_record(
                file.to_path_buf(),
                line_number,
                self.source_identifier(),
            ) {
                CodexJsonlRecordResult::Record(record) => records.push(record),
                CodexJsonlRecordResult::Malformed => {
                    failures
                        .push(self.failure(ReadFailureReason::Malformed, Some(file.to_path_buf())));
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
pub struct CodexIndex {
    root: PathBuf,
    path: PathBuf,
}

impl CodexIndex {
    pub fn new(root: PathBuf, path: PathBuf) -> Self {
        Self { root, path }
    }

    pub fn session_files(&self) -> std::io::Result<Vec<PathBuf>> {
        let text = std::fs::read_to_string(&self.path)?;
        let mut files = Vec::new();
        for line in text.lines() {
            if let Some(path) = CodexIndexRecord::new(line).path() {
                files.push(CodexIndexPath::new(self.root.clone(), path).session_path()?);
            }
        }
        files.sort();
        Ok(files)
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
        if path.is_absolute() {
            return if path.starts_with(&self.root) {
                Ok(path.to_path_buf())
            } else {
                Err(self.outside_root_error())
            };
        }
        if path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::RootDir))
        {
            return Err(self.outside_root_error());
        }
        Ok(self.root.join(path))
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

    pub fn path(&self) -> Option<String> {
        serde_json::from_str::<Value>(self.line)
            .ok()
            .and_then(|value| {
                value
                    .get("path")
                    .and_then(Value::as_str)
                    .or_else(|| value.get("session_path").and_then(Value::as_str))
                    .map(ToOwned::to_owned)
            })
    }
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
        let timestamp = CodexJsonValue::new(&value)
            .timestamp()
            .map(|value| Timestamp::new(value.to_string()));
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
