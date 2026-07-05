use std::path::{Path, PathBuf};

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
}

impl PiRunHistoryRootReader {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn collect(&self, request: &TranscriptReadRequest) -> TranscriptReadOutcome {
        let source_identifier = self.source_identifier();
        if let Some(outcome) =
            request.unsupported_relative_window_outcome(SourceKind::Pi, source_identifier.clone())
        {
            return outcome;
        }
        if !self.root.exists() {
            return TranscriptReadOutcome::from_records(
                SourceKind::Pi,
                source_identifier.clone(),
                Vec::new(),
                vec![self.failure(ReadFailureReason::Missing, Some(self.root.clone()))],
                request,
            );
        }
        let files = match TranscriptFileDiscovery::new(self.root.clone()).jsonl_files() {
            Ok(files) => files,
            Err(error) => {
                return TranscriptReadOutcome::from_records(
                    SourceKind::Pi,
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
            SourceKind::Pi,
            source_identifier,
            records,
            failures,
            request,
        )
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
            match PiJsonlRecord::new(line).into_transcript_record(
                file.to_path_buf(),
                line_number,
                self.source_identifier(),
            ) {
                PiJsonlRecordResult::Record(record) => records.push(record),
                PiJsonlRecordResult::Malformed => {
                    failures
                        .push(self.failure(ReadFailureReason::Malformed, Some(file.to_path_buf())));
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
        let Some(text) = PiJsonValue::new(&value).text() else {
            return PiJsonlRecordResult::Malformed;
        };
        let timestamp = PiJsonValue::new(&value)
            .timestamp()
            .map(|value| Timestamp::new(value.to_string()));
        PiJsonlRecordResult::Record(TranscriptRecord::new(
            SourceKind::Pi,
            source_identifier,
            path,
            line_number,
            timestamp,
            text,
        ))
    }
}

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

    pub fn text(&self) -> Option<String> {
        self.value
            .get("text")
            .and_then(Value::as_str)
            .or_else(|| self.value.get("output").and_then(Value::as_str))
            .or_else(|| self.value.get("content").and_then(Value::as_str))
            .or_else(|| self.value.get("message").and_then(Value::as_str))
            .map(ToOwned::to_owned)
    }
}
