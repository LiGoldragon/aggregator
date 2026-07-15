//! Versioned, primitive-only disk records for the v3 typed index.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexStoreFormatVersion {
    V3Chunked = 3,
}
impl IndexStoreFormatVersion {
    pub fn from_u8(value: u8) -> Option<Self> {
        (value == 3).then_some(Self::V3Chunked)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexFileKind {
    Manifest = 1,
    Chunk = 2,
    Checkpoint = 3,
    IndexNode = 4,
    Coverage = 5,
    Projection = 6,
    ReferenceIndex = 7,
    RelationshipIndex = 8,
    OrderIndex = 9,
}
impl IndexFileKind {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Manifest),
            2 => Some(Self::Chunk),
            3 => Some(Self::Checkpoint),
            4 => Some(Self::IndexNode),
            5 => Some(Self::Coverage),
            6 => Some(Self::Projection),
            7 => Some(Self::ReferenceIndex),
            8 => Some(Self::RelationshipIndex),
            9 => Some(Self::OrderIndex),
            _ => None,
        }
    }
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DiskPath {
    pub schema_version: u32,
    pub unix_bytes: Vec<u8>,
    pub display: String,
}
impl DiskPath {
    pub fn new(unix_bytes: Vec<u8>, display: String) -> Self {
        Self {
            schema_version: 1,
            unix_bytes,
            display,
        }
    }
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CurrentPointer {
    pub schema_version: u32,
    pub format_version: u32,
    pub manifest_locator: String,
    pub snapshot_identity: [u8; 32],
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ChunkDescriptor {
    pub schema_version: u32,
    pub opaque_locator: String,
    pub file_kind: u8,
    pub byte_count: u64,
    pub record_count: u64,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SourceSlot {
    pub schema_version: u32,
    pub source_kind: u8,
    pub configured_occurrence: u64,
    pub configuration_signature: [u8; 32],
    pub last_complete: Option<String>,
    pub visible_generation: Option<String>,
    pub provisional_checkpoint: Option<String>,
    pub coverage_status: u8,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SourceCoverageStatus {
    Complete = 1,
    Incomplete = 2,
    Failed = 3,
}
impl SourceCoverageStatus {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Complete),
            2 => Some(Self::Incomplete),
            3 => Some(Self::Failed),
            _ => None,
        }
    }
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct IndexManifest {
    pub schema_version: u32,
    pub source_slots: Vec<SourceSlot>,
    pub global_index_roots: Vec<ChunkDescriptor>,
    pub aggregate_counts: Vec<u64>,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SourceGenerationManifest {
    pub schema_version: u32,
    pub generation_state: u8,
    pub source_metadata_locator: String,
    pub collection_roots: Vec<ChunkDescriptor>,
    pub coverage_root: Option<ChunkDescriptor>,
    pub aggregate_facts: Vec<u64>,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct BuildCheckpoint {
    pub schema_version: u32,
    pub source_slot: u64,
    pub adapter_version: u32,
    pub configuration_signature: [u8; 32],
    pub discovery_cursor: Vec<u8>,
    pub coverage_locator: Option<String>,
    pub run_roots: Vec<ChunkDescriptor>,
    pub accumulated_scan_facts: Vec<u64>,
}

/// Compatibility-only normalized observation record; projection chunks use `projection` below.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct IndexRecordDto {
    pub schema_version: u32,
    pub record_kind: u8,
    pub fields: Vec<IndexFieldDto>,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct IndexFieldDto {
    pub name: String,
    pub bytes: Vec<u8>,
}

/// Schema version of every persisted projection DTO. Fields are append-only.
pub const TYPED_PROJECTION_DTO_VERSION: u32 = 1;
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ProjectionSizeDto {
    pub byte_count: Option<u64>,
    pub line_count: Option<u64>,
    pub segment_count: Option<u64>,
    pub certainty: u8,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ProjectionTaskDto {
    pub task_identifier: String,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ProjectionSessionDto {
    pub reference: String,
    pub source: u8,
    pub source_identifier: String,
    pub path: DiskPath,
    pub fingerprint_bytes: u64,
    pub fingerprint_seconds: i64,
    pub fingerprint_nanoseconds: u32,
    pub started_at: Option<String>,
    pub last_observed_at: Option<String>,
    pub producer_session_identifier: Option<String>,
    pub subagent_count: u64,
    pub output_count: u64,
    pub size: ProjectionSizeDto,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ProjectionSubagentDto {
    pub reference: String,
    pub session_reference: String,
    pub name: String,
    pub authored_status: u8,
    pub task: Option<ProjectionTaskDto>,
    pub output_count: u64,
    pub size: ProjectionSizeDto,
    pub first_observed_at: Option<String>,
    pub last_observed_at: Option<String>,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ProjectionOutputDto {
    pub reference: String,
    pub session_reference: String,
    pub subagent_reference: Option<String>,
    pub title: Option<String>,
    pub task: Option<ProjectionTaskDto>,
    pub source: u8,
    pub source_identifier: String,
    pub authored_status: u8,
    pub produced_at: Option<String>,
    pub path: DiskPath,
    pub fingerprint_bytes: u64,
    pub fingerprint_seconds: i64,
    pub fingerprint_nanoseconds: u32,
    pub source_line_number: u64,
    pub text_hash: String,
    pub size: ProjectionSizeDto,
    pub preview_text: String,
    pub preview_original_bytes: u64,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ProjectionSegmentDto {
    pub reference: String,
    pub output_reference: String,
    pub segment_index: u64,
    pub byte_range: Option<(u64, u64)>,
    pub line_range: Option<(u64, u64)>,
    pub size: ProjectionSizeDto,
    pub preview_text: String,
    pub preview_original_bytes: u64,
    pub source: u8,
    pub path: DiskPath,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ProjectionTranscriptBlockDto {
    pub reference: String,
    pub session_reference: String,
    pub subagent_reference: Option<String>,
    pub kind: u8,
    pub block_index: u64,
    pub task: Option<ProjectionTaskDto>,
    pub source: u8,
    pub source_identifier: String,
    pub authored_status: u8,
    pub observed_at: Option<String>,
    pub path: DiskPath,
    pub fingerprint_bytes: u64,
    pub fingerprint_seconds: i64,
    pub fingerprint_nanoseconds: u32,
    pub source_line_number: u64,
    pub text_hash: String,
    pub size: ProjectionSizeDto,
    pub text_availability: u8,
    pub preview_text: String,
    pub preview_original_bytes: u64,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ProjectionRecordDto {
    Session(ProjectionSessionDto),
    Subagent(ProjectionSubagentDto),
    Output(ProjectionOutputDto),
    Segment(ProjectionSegmentDto),
    TranscriptBlock(ProjectionTranscriptBlockDto),
}
/// Parent records carry counts and relationship keys only; children are resolved through indexes.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TypedReferenceIndexEntry {
    pub schema_version: u32,
    pub record_kind: u8,
    pub key_hash: [u8; 32],
    pub exact_reference: String,
    pub projection_locator: String,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TypedRelationshipIndexEntry {
    pub schema_version: u32,
    pub parent_reference: String,
    pub child_kind: u8,
    pub child_key_hash: [u8; 32],
    pub exact_child_reference: String,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TypedOrderIndexEntry {
    pub schema_version: u32,
    pub collection_kind: u8,
    pub order_kind: u8,
    pub exact_tuple: Vec<u8>,
    pub reference_hash: [u8; 32],
    pub exact_reference: String,
}
/// Fixed fanout nodes verify the complete key after hash routing, so collisions cannot alias data.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TypedIndexNode {
    pub schema_version: u32,
    pub fanout: u16,
    pub entries: Vec<TypedReferenceIndexEntry>,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct IndexChunk {
    pub schema_version: u32,
    pub records: Vec<IndexRecordDto>,
    pub projection: Option<ProjectionRecordDto>,
}
