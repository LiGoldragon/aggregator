use std::sync::{Arc, Mutex};

use aggregator::output_index::instrumentation::{
    IndexResourceMeter, IndexWorkCategory, IndexWorkEvent, IndexWorkObserver,
};

#[derive(Default)]
struct EventCollector {
    events: Mutex<Vec<IndexWorkEvent>>,
}

impl IndexWorkObserver for EventCollector {
    fn observe(&self, event: IndexWorkEvent) {
        self.events.lock().expect("event lock").push(event);
    }
}

#[test]
fn reservation_high_water_is_independent_of_total_work() {
    let collector = Arc::new(EventCollector::default());
    let meter = IndexResourceMeter::new(Some(collector.clone()));
    for _ in 0..100 {
        let reservation = meter.reserve(IndexWorkCategory::LogicalChunk, 512);
        drop(reservation);
    }
    let counters = meter.snapshot();
    assert_eq!(counters.live_bytes, 0);
    assert_eq!(counters.high_water_bytes, 512);
    assert!(
        collector
            .events
            .lock()
            .expect("event lock")
            .iter()
            .all(|event| !format!("{event:?}").contains("secret"))
    );
}
