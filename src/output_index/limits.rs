//! Fixed working-set limits for the typed fragile-index store.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexStoreLimits {
    pub maximum_logical_chunk_bytes: u64,
    pub maximum_serialized_chunk_bytes: u64,
    pub maximum_records_per_chunk: u64,
    pub maximum_manifest_bytes: u64,
    pub maximum_checkpoint_bytes: u64,
    pub maximum_cursor_bytes: u64,
    pub maximum_record_bytes: u64,
    pub maximum_string_bytes: u64,
    pub maximum_merge_fan_in: u64,
    pub maximum_query_candidates: u64,
    pub staging_generations_retained: u64,
}

impl IndexStoreLimits {
    pub const fn bounded_runtime() -> Self {
        Self {
            maximum_logical_chunk_bytes: 512 * 1024,
            maximum_serialized_chunk_bytes: 1024 * 1024,
            maximum_records_per_chunk: 1024,
            maximum_manifest_bytes: 256 * 1024,
            maximum_checkpoint_bytes: 256 * 1024,
            maximum_cursor_bytes: 4096,
            maximum_record_bytes: 256 * 1024,
            maximum_string_bytes: 64 * 1024,
            maximum_merge_fan_in: 16,
            maximum_query_candidates: 4096,
            staging_generations_retained: 2,
        }
    }

    pub fn accepts_chunk(&self, logical_bytes: u64, record_count: u64) -> bool {
        logical_bytes <= self.maximum_logical_chunk_bytes
            && record_count <= self.maximum_records_per_chunk
    }

    pub fn accepts_serialized_chunk(&self, bytes: u64) -> bool {
        bytes <= self.maximum_serialized_chunk_bytes
    }
}

impl Default for IndexStoreLimits {
    fn default() -> Self {
        Self::bounded_runtime()
    }
}
