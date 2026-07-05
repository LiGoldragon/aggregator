use std::path::{Path, PathBuf};

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
        let reader = ClaudeJsonlRootReader::new(self.root.path().to_path_buf());
        reader.collect(request)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeJsonlRootReader {
    root: PathBuf,
    limits: TranscriptScanLimits,
}

impl ClaudeJsonlRootReader {
    pub fn new(root: PathBuf) -> Self {
        Self::with_limits(root, TranscriptScanLimits::default_runtime())
    }

    pub fn with_limits(root: PathBuf, limits: TranscriptScanLimits) -> Self {
        Self { root, limits }
    }

    pub fn collect(&self, request: &TranscriptReadRequest) -> TranscriptReadOutcome {
        let source_identifier = self.source_identifier();
        if let Some(outcome) = request
            .unsupported_relative_window_outcome(SourceKind::Claude, source_identifier.clone())
        {
            return outcome;
        }
        if !self.root.exists() {
            return TranscriptReadOutcome::from_records(
                SourceKind::Claude,
                source_identifier.clone(),
                Vec::new(),
                vec![self.failure(ReadFailureReason::Missing, Some(self.root.clone()))],
                request,
            );
        }
        let discovery =
            match TranscriptFileDiscovery::with_limits(self.root.clone(), self.limits.clone())
                .discover_jsonl_files()
            {
                Ok(discovery) => discovery,
                Err(error) => {
                    return TranscriptReadOutcome::from_records(
                        SourceKind::Claude,
                        source_identifier.clone(),
                        Vec::new(),
                        vec![self.failure_from_io(error, Some(self.root.clone()))],
                        request,
                    );
                }
            };
        let mut records = Vec::new();
        let mut failures = TranscriptFailureAccumulator::new(
            SourceKind::Claude,
            Some(self.root.clone()),
            self.limits.maximum_failures(),
        );
        let mut truncations = discovery
            .truncations
            .into_iter()
            .map(|truncation| truncation.into_truncation(SourceKind::Claude))
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
                    truncations.push(truncation.into_truncation(SourceKind::Claude));
                }
                Err(error) => failures.push(self.failure_from_io(error, Some(file))),
            }
        }
        let failure_outcome = failures.finish();
        truncations.extend(failure_outcome.truncations);
        TranscriptReadOutcome::from_records_and_truncations(
            SourceKind::Claude,
            source_identifier,
            records,
            failure_outcome.failures,
            truncations,
            request,
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
                    truncations.push(truncation.into_truncation(SourceKind::Claude));
                    failures.push(self.failure(
                        ReadFailureReason::Malformed,
                        Some(TranscriptLineLocator::new(file.to_path_buf(), line_number).path()),
                    ));
                    continue;
                }
            };
            let parsed = ClaudeJsonlRecord::new(line).into_transcript_record(
                file.to_path_buf(),
                line_number,
                self.source_identifier(),
            );
            match parsed {
                ClaudeJsonlRecordResult::Record(record) => records.push(record),
                ClaudeJsonlRecordResult::Malformed => {
                    failures.push(self.failure(
                        ReadFailureReason::Malformed,
                        Some(TranscriptLineLocator::new(file.to_path_buf(), line_number).path()),
                    ));
                }
            }
        }
    }

    pub fn source_identifier(&self) -> SourceIdentifier {
        SourceIdentifier::new(format!("claude:{}", self.root.display()))
    }

    pub fn failure(&self, reason: ReadFailureReason, path: Option<PathBuf>) -> ReadFailure {
        ReadFailure {
            source: SourceKind::Claude,
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
        path: PathBuf,
        line_number: u64,
        source_identifier: SourceIdentifier,
    ) -> ClaudeJsonlRecordResult {
        let value = match serde_json::from_str::<Value>(self.line) {
            Ok(value) => value,
            Err(_) => return ClaudeJsonlRecordResult::Malformed,
        };
        let Some(text) = ClaudeJsonValue::new(&value).text() else {
            return ClaudeJsonlRecordResult::Malformed;
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
        ClaudeJsonlRecordResult::Record(TranscriptRecord::new(
            SourceKind::Claude,
            source_identifier,
            path,
            line_number,
            timestamp,
            text,
        ))
    }
}

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

    pub fn text(&self) -> Option<String> {
        self.value
            .get("text")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| self.message_content_text())
            .or_else(|| self.content_text())
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
        let array = self.value.as_array()?;
        let texts = array
            .iter()
            .filter_map(|item| item.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>();
        if texts.is_empty() {
            None
        } else {
            Some(texts.join("\n"))
        }
    }
}
