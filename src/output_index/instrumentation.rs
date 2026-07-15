//! Private-safe bounded-work instrumentation.

use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexWorkCategory {
    InputBuffer,
    LogicalChunk,
    SerializedChunk,
    DecodedChunk,
    MergeHead,
    PageResult,
    FixedMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexCoverageClass {
    Complete,
    Incomplete,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexWorkEvent {
    Reservation {
        category: IndexWorkCategory,
        live_bytes: u64,
        high_water_bytes: u64,
    },
    SourceScan {
        source_kind: u8,
        coverage: IndexCoverageClass,
    },
    ChunkProcessed {
        record_count: u64,
        bytes: u64,
    },
    QueryCandidates {
        count: u64,
    },
}

pub trait IndexWorkObserver: Send + Sync {
    fn observe(&self, event: IndexWorkEvent);
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexResourceCounters {
    pub live_bytes: u64,
    pub high_water_bytes: u64,
    pub source_scans: u64,
    pub files_processed: u64,
    pub records_processed: u64,
    pub chunks_processed: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub checkpoint_resumes: u64,
    pub checkpoint_restarts: u64,
    pub query_candidates: u64,
    pub backing_lines_read: u64,
    pub cards_returned: u64,
}

#[derive(Clone, Default)]
pub struct IndexResourceMeter {
    counters: Arc<Mutex<IndexResourceCounters>>,
    observer: Option<Arc<dyn IndexWorkObserver>>,
}

impl std::fmt::Debug for IndexResourceMeter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IndexResourceMeter")
            .finish_non_exhaustive()
    }
}

impl IndexResourceMeter {
    pub fn new(observer: Option<Arc<dyn IndexWorkObserver>>) -> Self {
        Self {
            counters: Arc::new(Mutex::new(IndexResourceCounters::default())),
            observer,
        }
    }

    pub fn reserve(&self, category: IndexWorkCategory, bytes: u64) -> IndexReservation {
        let (live_bytes, high_water_bytes) = self.with_counters(|counters| {
            counters.live_bytes = counters.live_bytes.saturating_add(bytes);
            counters.high_water_bytes = counters.high_water_bytes.max(counters.live_bytes);
            (counters.live_bytes, counters.high_water_bytes)
        });
        self.observe(IndexWorkEvent::Reservation {
            category,
            live_bytes,
            high_water_bytes,
        });
        IndexReservation {
            meter: self.clone(),
            bytes,
        }
    }

    pub fn snapshot(&self) -> IndexResourceCounters {
        self.with_counters(|counters| counters.clone())
    }

    pub fn observe_source_scan(&self, source_kind: u8, coverage: IndexCoverageClass) {
        self.with_counters(|counters| counters.source_scans += 1);
        self.observe(IndexWorkEvent::SourceScan {
            source_kind,
            coverage,
        });
    }

    pub fn observe_chunk(&self, records: u64, bytes: u64) {
        self.with_counters(|counters| {
            counters.chunks_processed += 1;
            counters.records_processed += records;
        });
        self.observe(IndexWorkEvent::ChunkProcessed {
            record_count: records,
            bytes,
        });
    }

    pub fn observe_query_candidates(&self, count: u64) {
        self.with_counters(|counters| counters.query_candidates += count);
        self.observe(IndexWorkEvent::QueryCandidates { count });
    }

    fn release(&self, bytes: u64) {
        self.with_counters(|counters| {
            counters.live_bytes = counters.live_bytes.saturating_sub(bytes)
        });
    }

    fn observe(&self, event: IndexWorkEvent) {
        if let Some(observer) = &self.observer {
            observer.observe(event);
        }
    }

    fn with_counters<T>(&self, transform: impl FnOnce(&mut IndexResourceCounters) -> T) -> T {
        let mut counters = self.counters.lock().expect("index resource meter lock");
        transform(&mut counters)
    }
}

#[derive(Debug)]
pub struct IndexReservation {
    meter: IndexResourceMeter,
    bytes: u64,
}

impl Drop for IndexReservation {
    fn drop(&mut self) {
        self.meter.release(self.bytes);
    }
}
