use std::fs;

use aggregator::output_index::{BoundedLineReadFailure, BoundedLineReader};
use tempfile::TempDir;

#[test]
fn backing_line_reader_rejects_a_line_above_its_fixed_buffer_cap() {
    let root = TempDir::new().expect("temporary backing root");
    let path = root.path().join("transcript.jsonl");
    fs::write(&path, "x".repeat(4_098)).expect("write oversized line");

    let failure = BoundedLineReader::new(path, 1, 4_097)
        .read_line()
        .expect_err("line cannot grow the bounded backing buffer");
    assert_eq!(failure, BoundedLineReadFailure::Oversized);
}
