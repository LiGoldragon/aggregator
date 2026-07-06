use std::path::{Path, PathBuf};

use serde_json::Value;
use signal_aggregator::{
    FilesystemPath, ReadFailure, ReadFailureReason, SourceIdentifier, SourceKind, Timestamp,
};

use crate::{
    AdapterKind,
    adapter::{
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
        let Some(text) = PiJsonValue::new(&value).text() else {
            return PiJsonlRecordResult::Malformed;
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
            .with_authored_status(metadata.authored_status()),
        )
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
