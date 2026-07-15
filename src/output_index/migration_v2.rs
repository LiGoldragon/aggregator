//! Streaming import of the JSON v2 compatibility index.
//!
//! This is deliberately the only v3 migration boundary that speaks JSON.  It never decodes a
//! top-level value or retains a corpus: arrays feed one record at a time into bounded chunks.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{self, Read},
    path::{Path, PathBuf},
};

use serde::de::{DeserializeSeed, Error as _, MapAccess, SeqAccess, Visitor};

use crate::{Error, Result, error::IndexStoreError};

use super::{
    build::SourceKey,
    limits::IndexStoreLimits,
    schema::{IndexChunk, IndexFieldDto, IndexFileKind, IndexRecordDto},
    store::IndexStaging,
};

/// The on-disk state discovered before migration is attempted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexFormat {
    Missing,
    ObsoleteV1,
    MigratableV2,
    CurrentV3,
    Unsupported,
}

/// Probes only a short prefix and never decodes an index body.
#[derive(Debug, Clone, Copy)]
pub struct IndexFormatProbe<'a> {
    prefix: &'a [u8],
}

impl<'a> IndexFormatProbe<'a> {
    pub fn new(prefix: &'a [u8]) -> Self {
        Self { prefix }
    }

    pub fn format(&self) -> IndexFormat {
        let compact = self
            .prefix
            .iter()
            .copied()
            .filter(|byte| !byte.is_ascii_whitespace())
            .collect::<Vec<_>>();
        if compact.is_empty() {
            return IndexFormat::Missing;
        }
        if compact.starts_with(b"AGGIDX03") || Self::field_equals(&compact, b"format_version", b'3')
        {
            return IndexFormat::CurrentV3;
        }
        match Self::field_number(&compact, b"version") {
            Some(1) => IndexFormat::ObsoleteV1,
            Some(2) => IndexFormat::MigratableV2,
            _ => IndexFormat::Unsupported,
        }
    }

    fn field_equals(bytes: &[u8], field: &[u8], expected: u8) -> bool {
        Self::field_number(bytes, field) == Some((expected - b'0') as u64)
    }

    fn field_number(bytes: &[u8], field: &[u8]) -> Option<u64> {
        let mut marker = Vec::with_capacity(field.len() + 4);
        marker.extend_from_slice(b"\"");
        marker.extend_from_slice(field);
        marker.extend_from_slice(b"\":");
        let position = bytes
            .windows(marker.len())
            .position(|window| window == marker)?;
        let digits = &bytes[position + marker.len()..];
        let length = digits
            .iter()
            .take_while(|byte| byte.is_ascii_digit())
            .count();
        (length > 0)
            .then(|| std::str::from_utf8(&digits[..length]).ok()?.parse().ok())
            .flatten()
    }
}

/// One configured occurrence eligible to receive matching legacy evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationSource {
    source_kind: String,
    source_identifier: String,
    configured_occurrence: u64,
}

impl MigrationSource {
    pub fn new(source_kind: String, source_identifier: String, configured_occurrence: u64) -> Self {
        Self {
            source_kind,
            source_identifier,
            configured_occurrence,
        }
    }

    pub fn from_source_key(source_key: &SourceKey) -> Self {
        Self::new(
            source_key.source_kind().to_owned(),
            source_key.source_identifier().to_owned(),
            source_key.configured_occurrence(),
        )
    }

    pub fn configured_occurrence(&self) -> u64 {
        self.configured_occurrence
    }

    pub fn configuration_signature(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.source_kind.as_bytes());
        hasher.update(&[0]);
        hasher.update(self.source_identifier.as_bytes());
        hasher.update(&self.configured_occurrence.to_le_bytes());
        *hasher.finalize().as_bytes()
    }

    pub fn source_kind_code(&self) -> u8 {
        match Self::normalized(&self.source_kind).as_str() {
            "claude" => 1,
            "claudesubagentoutput" => 2,
            "codex" => 3,
            "pi" => 4,
            "pisubagentoutput" => 5,
            _ => 0,
        }
    }

    fn matches(&self, source_kind: &str, source_identifier: Option<&str>) -> bool {
        Self::normalized(&self.source_kind) == Self::normalized(source_kind)
            && source_identifier.is_none_or(|identifier| identifier == self.source_identifier)
    }

    fn normalized(value: &str) -> String {
        value
            .bytes()
            .filter(u8::is_ascii_alphanumeric)
            .map(|byte| byte.to_ascii_lowercase() as char)
            .collect()
    }
}

/// A bounded source run produced by the v2 importer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigratedSourceRun {
    pub source: MigrationSource,
    pub chunk_count: u64,
    pub record_count: u64,
    pub logical_bytes: u64,
    pub chunk_locators: Vec<String>,
}

/// The complete, independently attributable result of importing one v2 file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V2MigrationResult {
    pub source_runs: Vec<MigratedSourceRun>,
    pub collection_counts: BTreeMap<String, u64>,
}

/// Owns a lexical guard and a bounded staging writer for one legacy file.
#[derive(Debug, Clone)]
pub struct V2Migration {
    limits: IndexStoreLimits,
    sources: Vec<MigrationSource>,
}

impl V2Migration {
    pub fn new(limits: IndexStoreLimits, sources: Vec<MigrationSource>) -> Self {
        Self { limits, sources }
    }

    /// Imports a regular v2 JSON file.  The caller retains the source unchanged until this succeeds.
    pub fn import_into_staging(
        &self,
        source: &Path,
        staging: &IndexStaging,
    ) -> Result<V2MigrationResult> {
        let metadata = fs::symlink_metadata(source).map_err(|error| {
            Error::index_store(IndexStoreError::io("inspecting v2 migration input", error))
        })?;
        if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
            return Err(Error::index_store(IndexStoreError::UnsafePath));
        }
        let mut writer =
            MigrationChunkWriter::new(staging.clone(), self.sources.clone(), self.limits)?;
        let ledger = ReferenceLedger::new(staging.path().join("migration-reference-ledger"))?;
        let input = File::open(source).map_err(|error| {
            Error::index_store(IndexStoreError::io("opening v2 migration input", error))
        })?;
        let guarded = LexicalRead::new(input, self.limits.maximum_string_bytes);
        let mut deserializer = serde_json::Deserializer::from_reader(guarded);
        let imported = V2TopLevelSeed::new(&mut writer, &ledger)
            .deserialize(&mut deserializer)
            .map_err(|error| {
                let detail = error.to_string();
                if detail.contains("fragile-reference collision") {
                    Error::index_store(IndexStoreError::ReferenceCollision)
                } else {
                    Error::index_store(IndexStoreError::MigrationFailure { detail })
                }
            });
        let cleanup = ledger.remove();
        imported?;
        cleanup?;
        writer.finish()
    }
}

/// Bounds JSON lexical tokens before serde receives bytes that could make it allocate a string.
#[derive(Debug)]
pub struct LexicalRead<R> {
    input: R,
    maximum_token_bytes: u64,
    in_string: bool,
    escaped: bool,
    token_bytes: u64,
}

impl<R> LexicalRead<R> {
    pub fn new(input: R, maximum_token_bytes: u64) -> Self {
        Self {
            input,
            maximum_token_bytes,
            in_string: false,
            escaped: false,
            token_bytes: 0,
        }
    }
}

impl<R: Read> LexicalRead<R> {
    fn inspect(&mut self, byte: u8) -> io::Result<()> {
        if self.in_string {
            if self.escaped {
                self.escaped = false;
                self.token_bytes = self.token_bytes.saturating_add(1);
            } else if byte == b'\\' {
                self.escaped = true;
                self.token_bytes = self.token_bytes.saturating_add(1);
            } else if byte == b'"' {
                self.in_string = false;
                self.token_bytes = 0;
            } else {
                self.token_bytes = self.token_bytes.saturating_add(1);
            }
        } else if byte == b'"' {
            self.in_string = true;
            self.token_bytes = 0;
        } else if byte.is_ascii_whitespace()
            || matches!(byte, b'{' | b'}' | b'[' | b']' | b',' | b':')
        {
            self.token_bytes = 0;
        } else {
            self.token_bytes = self.token_bytes.saturating_add(1);
        }
        if self.token_bytes > self.maximum_token_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "v2 JSON lexical token exceeds migration limit",
            ));
        }
        Ok(())
    }
}

impl<R: Read> Read for LexicalRead<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let count = self.input.read(buffer)?;
        for byte in &buffer[..count] {
            self.inspect(*byte)?;
        }
        Ok(count)
    }
}

#[derive(Debug)]
struct MigrationChunkWriter {
    staging: IndexStaging,
    limits: IndexStoreLimits,
    runs: Vec<MigrationRunWriter>,
    collection_counts: BTreeMap<String, u64>,
}

impl MigrationChunkWriter {
    fn new(
        staging: IndexStaging,
        sources: Vec<MigrationSource>,
        limits: IndexStoreLimits,
    ) -> Result<Self> {
        if sources.is_empty() {
            return Err(Error::index_store(IndexStoreError::MigrationFailure {
                detail: "v2 migration has no configured source slots".to_owned(),
            }));
        }
        Ok(Self {
            staging,
            limits,
            runs: sources.into_iter().map(MigrationRunWriter::new).collect(),
            collection_counts: BTreeMap::new(),
        })
    }

    fn accept(&mut self, record: V2Record) -> Result<()> {
        let matches = self.matching_runs(&record)?;
        let dto = record.dto()?;
        let bytes = MigrationRecordSize::new(&dto).bytes();
        if bytes > self.limits.maximum_record_bytes {
            return Err(Error::index_store(IndexStoreError::OversizedRecord));
        }
        for index in matches {
            self.runs[index].push(dto.clone(), bytes, &self.staging, self.limits)?;
        }
        *self
            .collection_counts
            .entry(record.collection.name().to_owned())
            .or_default() += 1;
        Ok(())
    }

    fn matching_runs(&self, record: &V2Record) -> Result<Vec<usize>> {
        let source_kind = record.string("source");
        let source_identifier = record.string("source_identifier");
        let matches = self
            .runs
            .iter()
            .enumerate()
            .filter_map(|(index, run)| match source_kind.as_deref() {
                Some(kind) => run
                    .source
                    .matches(kind, source_identifier.as_deref())
                    .then_some(index),
                // v2 subagent records have no source identity. They remain attributable to every
                // possible configured occurrence; later global deduplication keeps one reference.
                None => Some(index),
            })
            .collect::<Vec<_>>();
        if matches.is_empty() {
            return Err(Error::index_store(IndexStoreError::MigrationFailure {
                detail: format!(
                    "{} record does not match a configured source",
                    record.collection.name()
                ),
            }));
        }
        Ok(matches)
    }

    fn finish(mut self) -> Result<V2MigrationResult> {
        for run in &mut self.runs {
            run.flush(&self.staging, self.limits)?;
        }
        Ok(V2MigrationResult {
            source_runs: self
                .runs
                .into_iter()
                .map(MigrationRunWriter::into_run)
                .collect(),
            collection_counts: self.collection_counts,
        })
    }
}

#[derive(Debug)]
struct MigrationRunWriter {
    source: MigrationSource,
    records: Vec<IndexRecordDto>,
    logical_bytes: u64,
    total_logical_bytes: u64,
    record_count: u64,
    chunk_count: u64,
    locators: Vec<String>,
}

impl MigrationRunWriter {
    fn new(source: MigrationSource) -> Self {
        Self {
            source,
            records: Vec::new(),
            logical_bytes: 0,
            total_logical_bytes: 0,
            record_count: 0,
            chunk_count: 0,
            locators: Vec::new(),
        }
    }

    fn push(
        &mut self,
        record: IndexRecordDto,
        bytes: u64,
        staging: &IndexStaging,
        limits: IndexStoreLimits,
    ) -> Result<()> {
        if !self.records.is_empty()
            && !limits.accepts_chunk(
                self.logical_bytes.saturating_add(bytes),
                self.records.len() as u64 + 1,
            )
        {
            self.flush(staging, limits)?;
        }
        if !limits.accepts_chunk(bytes, 1) {
            return Err(Error::index_store(IndexStoreError::OversizedRecord));
        }
        self.logical_bytes = self.logical_bytes.saturating_add(bytes);
        self.records.push(record);
        self.record_count += 1;
        Ok(())
    }

    fn flush(&mut self, staging: &IndexStaging, limits: IndexStoreLimits) -> Result<()> {
        if self.records.is_empty() {
            return Ok(());
        }
        let locator = format!(
            "migration-{}-{:016x}",
            self.source.configured_occurrence(),
            self.chunk_count
        );
        let chunk = IndexChunk {
            schema_version: 1,
            records: std::mem::take(&mut self.records),
        };
        staging.write_chunk(
            &super::store::IndexLocator::new(locator.clone()),
            IndexFileKind::Chunk,
            &chunk,
        )?;
        self.total_logical_bytes = self.total_logical_bytes.saturating_add(self.logical_bytes);
        self.logical_bytes = 0;
        self.chunk_count += 1;
        self.locators.push(locator);
        let _ = limits;
        Ok(())
    }

    fn into_run(self) -> MigratedSourceRun {
        MigratedSourceRun {
            source: self.source,
            chunk_count: self.chunk_count,
            record_count: self.record_count,
            logical_bytes: self.total_logical_bytes,
            chunk_locators: self.locators,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum V2Collection {
    Sessions,
    Subagents,
    Outputs,
    Segments,
    TranscriptBlocks,
}

impl V2Collection {
    fn name(self) -> &'static str {
        match self {
            Self::Sessions => "sessions",
            Self::Subagents => "subagents",
            Self::Outputs => "outputs",
            Self::Segments => "segments",
            Self::TranscriptBlocks => "transcript_blocks",
        }
    }

    fn from_name(name: &str) -> Option<Self> {
        match name {
            "sessions" => Some(Self::Sessions),
            "subagents" => Some(Self::Subagents),
            "outputs" => Some(Self::Outputs),
            "segments" => Some(Self::Segments),
            "transcript_blocks" => Some(Self::TranscriptBlocks),
            _ => None,
        }
    }

    fn required(self) -> &'static [&'static str] {
        match self {
            Self::Sessions => &[
                "reference",
                "source",
                "source_identifier",
                "path",
                "fingerprint",
                "size",
                "subagent_references",
                "output_references",
            ],
            Self::Subagents => &[
                "reference",
                "session_reference",
                "name",
                "authored_status",
                "size",
                "output_references",
            ],
            Self::Outputs => &[
                "reference",
                "session_reference",
                "source",
                "source_identifier",
                "authored_status",
                "path",
                "fingerprint",
                "source_line_number",
                "text_hash",
                "size",
                "preview_text",
                "preview_original_bytes",
            ],
            Self::Segments => &[
                "reference",
                "output_reference",
                "segment_index",
                "size",
                "preview_text",
                "preview_original_bytes",
                "source",
                "path",
            ],
            Self::TranscriptBlocks => &[
                "reference",
                "session_reference",
                "kind",
                "block_index",
                "source",
                "source_identifier",
                "authored_status",
                "path",
                "fingerprint",
                "source_line_number",
                "text_hash",
                "size",
                "text_availability",
                "preview_text",
                "preview_original_bytes",
            ],
        }
    }

    fn allowed(self, name: &str) -> bool {
        self.required().contains(&name)
            || matches!(
                name,
                "started_at"
                    | "last_observed_at"
                    | "producer_session_identifier"
                    | "task_identifier"
                    | "subagent_reference"
                    | "title"
                    | "produced_at"
                    | "byte_start"
                    | "byte_end"
                    | "line_start"
                    | "line_end"
                    | "first_observed_at"
                    | "observed_at"
            )
    }

    fn is_reference_array(self, name: &str) -> bool {
        matches!(
            (self, name),
            (Self::Sessions, "subagent_references")
                | (Self::Sessions, "output_references")
                | (Self::Subagents, "output_references")
        )
    }
}

#[derive(Debug)]
struct V2TopLevelSeed<'a> {
    writer: &'a mut MigrationChunkWriter,
    ledger: &'a ReferenceLedger,
}

impl<'a> V2TopLevelSeed<'a> {
    fn new(writer: &'a mut MigrationChunkWriter, ledger: &'a ReferenceLedger) -> Self {
        Self { writer, ledger }
    }
}

impl<'de> DeserializeSeed<'de> for V2TopLevelSeed<'_> {
    type Value = ();

    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<(), D::Error> {
        deserializer.deserialize_map(V2TopLevelVisitor {
            writer: self.writer,
            ledger: self.ledger,
        })
    }
}

struct V2TopLevelVisitor<'a> {
    writer: &'a mut MigrationChunkWriter,
    ledger: &'a ReferenceLedger,
}

impl<'de> Visitor<'de> for V2TopLevelVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a v2 fragile index object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<(), A::Error> {
        let mut seen = BTreeSet::new();
        while let Some(name) = map.next_key::<String>()? {
            if !seen.insert(name.clone()) {
                return Err(A::Error::custom("duplicate v2 top-level field"));
            }
            if name == "version" {
                let version = map.next_value::<u64>()?;
                if version != 2 {
                    return Err(A::Error::custom("v2 migration requires version 2"));
                }
            } else if let Some(collection) = V2Collection::from_name(&name) {
                map.next_value_seed(V2CollectionSeed {
                    collection,
                    writer: self.writer,
                    ledger: self.ledger,
                })?;
            } else {
                return Err(A::Error::custom("unknown v2 top-level field"));
            }
        }
        let expected = [
            "version",
            "sessions",
            "subagents",
            "outputs",
            "segments",
            "transcript_blocks",
        ];
        if expected.iter().any(|name| !seen.contains(*name)) {
            return Err(A::Error::custom(
                "v2 index is missing a required collection",
            ));
        }
        Ok(())
    }
}

struct V2CollectionSeed<'a> {
    collection: V2Collection,
    writer: &'a mut MigrationChunkWriter,
    ledger: &'a ReferenceLedger,
}

impl<'de> DeserializeSeed<'de> for V2CollectionSeed<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<(), D::Error> {
        deserializer.deserialize_seq(V2CollectionVisitor {
            collection: self.collection,
            writer: self.writer,
            ledger: self.ledger,
        })
    }
}

struct V2CollectionVisitor<'a> {
    collection: V2Collection,
    writer: &'a mut MigrationChunkWriter,
    ledger: &'a ReferenceLedger,
}

impl<'de> Visitor<'de> for V2CollectionVisitor<'_> {
    type Value = ();
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a v2 index collection array")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> std::result::Result<(), A::Error> {
        while let Some(record) = sequence.next_element_seed(V2RecordSeed {
            collection: self.collection,
        })? {
            if self.ledger.record(&record).map_err(A::Error::custom)? == LedgerEntry::New {
                self.writer.accept(record).map_err(A::Error::custom)?;
            }
        }
        Ok(())
    }
}

struct V2RecordSeed {
    collection: V2Collection,
}

impl<'de> DeserializeSeed<'de> for V2RecordSeed {
    type Value = V2Record;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<V2Record, D::Error> {
        deserializer.deserialize_map(V2RecordVisitor {
            collection: self.collection,
        })
    }
}

struct V2RecordVisitor {
    collection: V2Collection,
}

impl<'de> Visitor<'de> for V2RecordVisitor {
    type Value = V2Record;
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a valid v2 collection record")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<V2Record, A::Error> {
        let mut fields = BTreeMap::new();
        while let Some(name) = map.next_key::<String>()? {
            if !self.collection.allowed(&name) || fields.contains_key(&name) {
                return Err(A::Error::custom("unknown or duplicate v2 record field"));
            }
            let value = if self.collection.is_reference_array(&name) {
                V2Field::ReferenceCount(map.next_value_seed(ReferenceCountSeed)?)
            } else {
                map.next_value_seed(ScalarOrObjectSeed)?
            };
            fields.insert(name, value);
        }
        if self
            .collection
            .required()
            .iter()
            .any(|name| !fields.contains_key(*name))
        {
            return Err(A::Error::custom("v2 record is missing required fields"));
        }
        let record = V2Record {
            collection: self.collection,
            fields,
        };
        record.validate().map_err(A::Error::custom)?;
        Ok(record)
    }
}

struct ReferenceCountSeed;
impl<'de> DeserializeSeed<'de> for ReferenceCountSeed {
    type Value = u64;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<u64, D::Error> {
        deserializer.deserialize_seq(ReferenceCountVisitor)
    }
}
struct ReferenceCountVisitor;
impl<'de> Visitor<'de> for ReferenceCountVisitor {
    type Value = u64;
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an array of reference strings")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> std::result::Result<u64, A::Error> {
        let mut count = 0_u64;
        while sequence.next_element::<String>()?.is_some() {
            count = count
                .checked_add(1)
                .ok_or_else(|| A::Error::custom("v2 reference count overflow"))?;
        }
        Ok(count)
    }
}

struct ScalarOrObjectSeed;
impl<'de> DeserializeSeed<'de> for ScalarOrObjectSeed {
    type Value = V2Field;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<V2Field, D::Error> {
        deserializer.deserialize_any(ScalarOrObjectVisitor)
    }
}
struct ScalarOrObjectVisitor;
impl<'de> Visitor<'de> for ScalarOrObjectVisitor {
    type Value = V2Field;
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a scalar or bounded object")
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> std::result::Result<V2Field, E> {
        Ok(V2Field::Scalar(
            serde_json::to_vec(value).map_err(E::custom)?,
        ))
    }
    fn visit_string<E: serde::de::Error>(self, value: String) -> std::result::Result<V2Field, E> {
        self.visit_str(&value)
    }
    fn visit_u64<E: serde::de::Error>(self, value: u64) -> std::result::Result<V2Field, E> {
        Ok(V2Field::Scalar(value.to_string().into_bytes()))
    }
    fn visit_i64<E: serde::de::Error>(self, value: i64) -> std::result::Result<V2Field, E> {
        Ok(V2Field::Scalar(value.to_string().into_bytes()))
    }
    fn visit_bool<E: serde::de::Error>(self, value: bool) -> std::result::Result<V2Field, E> {
        Ok(V2Field::Scalar(value.to_string().into_bytes()))
    }
    fn visit_none<E: serde::de::Error>(self) -> std::result::Result<V2Field, E> {
        Ok(V2Field::Scalar(b"null".to_vec()))
    }
    fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<V2Field, E> {
        self.visit_none()
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<V2Field, A::Error> {
        let mut fields = BTreeMap::new();
        while let Some(name) = map.next_key::<String>()? {
            if fields.len() >= 32 || fields.contains_key(&name) {
                return Err(A::Error::custom("oversized or duplicate nested v2 object"));
            }
            fields.insert(name, map.next_value_seed(NestedScalarSeed)?);
        }
        Ok(V2Field::Object(fields))
    }
}

struct NestedScalarSeed;
impl<'de> DeserializeSeed<'de> for NestedScalarSeed {
    type Value = V2Field;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<V2Field, D::Error> {
        deserializer.deserialize_any(NestedScalarVisitor)
    }
}
struct NestedScalarVisitor;
impl<'de> Visitor<'de> for NestedScalarVisitor {
    type Value = V2Field;
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a nested v2 scalar")
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> std::result::Result<V2Field, E> {
        Ok(V2Field::Scalar(
            serde_json::to_vec(value).map_err(E::custom)?,
        ))
    }
    fn visit_string<E: serde::de::Error>(self, value: String) -> std::result::Result<V2Field, E> {
        self.visit_str(&value)
    }
    fn visit_u64<E: serde::de::Error>(self, value: u64) -> std::result::Result<V2Field, E> {
        Ok(V2Field::Scalar(value.to_string().into_bytes()))
    }
    fn visit_i64<E: serde::de::Error>(self, value: i64) -> std::result::Result<V2Field, E> {
        Ok(V2Field::Scalar(value.to_string().into_bytes()))
    }
    fn visit_bool<E: serde::de::Error>(self, value: bool) -> std::result::Result<V2Field, E> {
        Ok(V2Field::Scalar(value.to_string().into_bytes()))
    }
    fn visit_none<E: serde::de::Error>(self) -> std::result::Result<V2Field, E> {
        Ok(V2Field::Scalar(b"null".to_vec()))
    }
    fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<V2Field, E> {
        self.visit_none()
    }
}

#[derive(Debug, Clone)]
enum V2Field {
    Scalar(Vec<u8>),
    Object(BTreeMap<String, V2Field>),
    ReferenceCount(u64),
}

impl V2Field {
    fn string(&self) -> Option<String> {
        let Self::Scalar(value) = self else {
            return None;
        };
        serde_json::from_slice::<String>(value).ok()
    }

    fn is_json_string(&self) -> bool {
        matches!(self, Self::Scalar(value) if value.starts_with(b"\"") && value.ends_with(b"\""))
    }

    fn is_unsigned_number(&self) -> bool {
        matches!(self, Self::Scalar(value) if !value.is_empty() && value.iter().all(u8::is_ascii_digit))
    }

    fn is_object(&self) -> bool {
        matches!(self, Self::Object(_))
    }

    fn bytes(&self) -> Result<Vec<u8>> {
        match self {
            Self::Scalar(value) => Ok(value.clone()),
            Self::ReferenceCount(count) => Ok(count.to_string().into_bytes()),
            Self::Object(fields) => CompactJsonObject::new(fields).bytes(),
        }
    }
}

#[derive(Debug, Clone)]
struct V2Record {
    collection: V2Collection,
    fields: BTreeMap<String, V2Field>,
}
impl V2Record {
    fn string(&self, name: &str) -> Option<String> {
        self.fields.get(name)?.string()
    }

    fn validate(&self) -> std::result::Result<(), String> {
        for name in [
            "reference",
            "path",
            "source",
            "source_identifier",
            "authored_status",
            "kind",
            "text_hash",
            "preview_text",
            "name",
        ] {
            if let Some(value) = self.fields.get(name)
                && !value.is_json_string()
            {
                return Err(format!(
                    "v2 {} field {name} must be a string",
                    self.collection.name()
                ));
            }
        }
        for name in [
            "source_line_number",
            "preview_original_bytes",
            "segment_index",
            "block_index",
        ] {
            if let Some(value) = self.fields.get(name)
                && !value.is_unsigned_number()
            {
                return Err(format!(
                    "v2 {} field {name} must be an unsigned number",
                    self.collection.name()
                ));
            }
        }
        for name in ["fingerprint", "size"] {
            if let Some(value) = self.fields.get(name)
                && !value.is_object()
            {
                return Err(format!(
                    "v2 {} field {name} must be an object",
                    self.collection.name()
                ));
            }
        }
        if let Some(source) = self.string("source")
            && !matches!(
                source.as_str(),
                "Claude"
                    | "ClaudeSubagentOutput"
                    | "Codex"
                    | "Pi"
                    | "PiSubagentOutput"
                    | "Repository"
            )
        {
            return Err("v2 record has an unknown source kind".to_owned());
        }
        if let Some(status) = self.string("authored_status")
            && !matches!(
                status.as_str(),
                "AgentAuthored" | "HumanAuthored" | "MixedAuthorship" | "UnknownAuthorship"
            )
        {
            return Err("v2 record has an unknown authored status".to_owned());
        }
        if let Some(kind) = self.string("kind")
            && !matches!(
                kind.as_str(),
                "UserPrompt"
                    | "AgentResponse"
                    | "ToolCall"
                    | "ToolResult"
                    | "Inference"
                    | "SystemInstruction"
                    | "Attachment"
                    | "SessionEvent"
                    | "Unclassified"
            )
        {
            return Err("v2 record has an unknown transcript-block kind".to_owned());
        }
        if let Some(availability) = self.string("text_availability")
            && !matches!(
                availability.as_str(),
                "ReadableText" | "UnavailableText" | "EncryptedText"
            )
        {
            return Err("v2 record has an unknown text availability".to_owned());
        }
        Ok(())
    }

    fn dto(&self) -> Result<IndexRecordDto> {
        let mut fields = Vec::with_capacity(self.fields.len() + 1);
        fields.push(IndexFieldDto {
            name: "collection".to_owned(),
            bytes: self.collection.name().as_bytes().to_vec(),
        });
        for (name, value) in &self.fields {
            let name = match value {
                V2Field::ReferenceCount(_) => format!("{name}-count"),
                _ => name.clone(),
            };
            fields.push(IndexFieldDto {
                name,
                bytes: value.bytes()?,
            });
        }
        Ok(IndexRecordDto {
            schema_version: 1,
            record_kind: self.collection as u8 + 10,
            fields,
        })
    }
    fn identity(&self) -> Result<Vec<u8>> {
        MigrationRecordIdentity::new(self).bytes()
    }
}

struct CompactJsonObject<'a> {
    fields: &'a BTreeMap<String, V2Field>,
}
impl<'a> CompactJsonObject<'a> {
    fn new(fields: &'a BTreeMap<String, V2Field>) -> Self {
        Self { fields }
    }
    fn bytes(&self) -> Result<Vec<u8>> {
        let mut output = Vec::new();
        output.push(b'{');
        for (index, (name, value)) in self.fields.iter().enumerate() {
            if index > 0 {
                output.push(b',');
            }
            output.extend(serde_json::to_vec(name).map_err(|error| {
                Error::index_store(IndexStoreError::MigrationFailure {
                    detail: error.to_string(),
                })
            })?);
            output.push(b':');
            output.extend(value.bytes()?);
        }
        output.push(b'}');
        Ok(output)
    }
}

struct MigrationRecordSize<'a> {
    record: &'a IndexRecordDto,
}
impl<'a> MigrationRecordSize<'a> {
    fn new(record: &'a IndexRecordDto) -> Self {
        Self { record }
    }
    fn bytes(&self) -> u64 {
        self.record
            .fields
            .iter()
            .map(|field| (field.name.len() + field.bytes.len()) as u64)
            .sum()
    }
}
struct MigrationRecordIdentity<'a> {
    record: &'a V2Record,
}
impl<'a> MigrationRecordIdentity<'a> {
    fn new(record: &'a V2Record) -> Self {
        Self { record }
    }
    fn bytes(&self) -> Result<Vec<u8>> {
        let dto = self.record.dto()?;
        let mut bytes = Vec::new();
        for field in dto.fields {
            bytes.extend_from_slice(field.name.as_bytes());
            bytes.push(0);
            bytes.extend(field.bytes);
            bytes.push(0);
        }
        Ok(bytes)
    }
}

/// Disk-backed exact-reference collision detection; it retains one bounded entry per reference,
/// never a corpus-sized in-memory set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LedgerEntry {
    New,
    Duplicate,
}

#[derive(Debug)]
struct ReferenceLedger {
    path: PathBuf,
}
impl ReferenceLedger {
    fn new(path: PathBuf) -> Result<Self> {
        fs::create_dir(&path).map_err(|error| {
            Error::index_store(IndexStoreError::io(
                "creating migration reference ledger",
                error,
            ))
        })?;
        Ok(Self { path })
    }
    fn record(&self, record: &V2Record) -> Result<LedgerEntry> {
        let reference = record.string("reference").ok_or_else(|| {
            Error::index_store(IndexStoreError::MigrationFailure {
                detail: "v2 record lacks reference".to_owned(),
            })
        })?;
        let identity = record.identity()?;
        let locator = self
            .path
            .join(blake3::hash(reference.as_bytes()).to_hex().to_string());
        let payload = [
            reference.as_bytes(),
            b"\n",
            blake3::hash(&identity).as_bytes(),
        ]
        .concat();
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&locator)
        {
            Ok(mut file) => {
                std::io::Write::write_all(&mut file, &payload).map_err(|error| {
                    Error::index_store(IndexStoreError::io(
                        "writing migration reference ledger",
                        error,
                    ))
                })?;
                Ok(LedgerEntry::New)
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let existing = fs::read(&locator).map_err(|error| {
                    Error::index_store(IndexStoreError::io(
                        "reading migration reference ledger",
                        error,
                    ))
                })?;
                if existing == payload {
                    Ok(LedgerEntry::Duplicate)
                } else {
                    Err(Error::index_store(IndexStoreError::ReferenceCollision))
                }
            }
            Err(error) => Err(Error::index_store(IndexStoreError::io(
                "creating migration reference ledger",
                error,
            ))),
        }
    }
    fn remove(&self) -> Result<()> {
        fs::remove_dir_all(&self.path).map_err(|error| {
            Error::index_store(IndexStoreError::io(
                "removing migration reference ledger",
                error,
            ))
        })
    }
}
