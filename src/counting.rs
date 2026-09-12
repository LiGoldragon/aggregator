//! Counts measured by the runtime and counts carried by the contract.
//!
//! The contract carries every count as a signed integer, which is what the
//! Datom text form holds. The runtime measures bytes, lines and items as
//! unsigned. The two meet here, explicitly, and never by a silent cast.

/// The projection between a measured count and a contract count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeasuredCount;

impl MeasuredCount {
    /// Projects a measured count into the contract's signed count. A measure
    /// past the signed ceiling is reported at the ceiling rather than wrapped.
    pub fn contract_count(value: u64) -> i64 {
        i64::try_from(value).unwrap_or(i64::MAX)
    }

    /// Reads a contract count back as a measure. A negative count is not a
    /// measure and reads as zero.
    pub fn measured_count(value: i64) -> u64 {
        u64::try_from(value).unwrap_or(0)
    }

    /// Reads a contract count as a length for indexing, refusing a negative.
    pub fn indexable_count(value: i64) -> Option<usize> {
        usize::try_from(value).ok()
    }
}
