//! Versioned, primitive-only disk records for the v3 typed index.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexStoreFormatVersion {
    V3Chunked = 3,
}

impl IndexStoreFormatVersion {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            3 => Some(Self::V3Chunked),
            _ => None,
        }
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
}

impl IndexFileKind {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Manifest),
            2 => Some(Self::Chunk),
            3 => Some(Self::Checkpoint),
            4 => Some(Self::IndexNode),
            5 => Some(Self::Coverage),
            _ => None,
        }
    }
}

/// Raw Unix path bytes reopen a backing file; `display` preserves legacy reference material.
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

#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Deserialize,
    serde::Serialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
)]
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
    pub coverage_status: u8,
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

/// A bounded primitive record envelope. Runtime signal types never cross this archive boundary.
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

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct IndexChunk {
    pub schema_version: u32,
    pub records: Vec<IndexRecordDto>,
}
