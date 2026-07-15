//! Truthful independent-source generation reconciliation.
//!
//! A source slot names the only two views that can be queried: a last complete generation and an
//! explicitly provisional generation.  Deletion authority is carried by completed scopes, never
//! inferred from a malformed or unvisited scope.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    migration_v2::MigrationSource,
    schema::{SourceCoverageStatus, SourceSlot},
};

/// Immutable source generation metadata.  Records live in typed chunks referenced by `locator`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceGeneration {
    pub locator: String,
    pub completed_scopes: BTreeSet<String>,
    pub record_count: u64,
}

impl SourceGeneration {
    pub fn new(locator: String, completed_scopes: BTreeSet<String>, record_count: u64) -> Self {
        Self {
            locator,
            completed_scopes,
            record_count,
        }
    }

    /// An empty complete generation is a real replacement, not an absent update.
    pub fn complete_empty(locator: String) -> Self {
        Self::new(locator, BTreeSet::new(), 0)
    }
}

/// Fresh coverage facts for one configured source occurrence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceRefreshFact {
    Complete {
        generation: SourceGeneration,
    },
    Incomplete {
        generation: SourceGeneration,
        checkpoint: String,
    },
    Failed,
}

/// Persisted independently reconcilable state for one configured occurrence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSlotState {
    pub source: MigrationSource,
    pub last_complete: Option<SourceGeneration>,
    pub provisional_visible: Option<SourceGeneration>,
    pub checkpoint: Option<String>,
    pub coverage: SourceCoverageStatus,
}

impl SourceSlotState {
    pub fn uninitialized(source: MigrationSource) -> Self {
        Self {
            source,
            last_complete: None,
            provisional_visible: None,
            checkpoint: None,
            coverage: SourceCoverageStatus::Failed,
        }
    }

    /// The view selected for this source contains no implied deletion authority.
    pub fn visible_generation(&self) -> Option<&SourceGeneration> {
        self.provisional_visible
            .as_ref()
            .or(self.last_complete.as_ref())
    }

    /// Projects the complete/provisional/checkpoint facts into the versioned typed manifest.
    pub fn disk_slot(&self) -> SourceSlot {
        SourceSlot {
            schema_version: 1,
            source_kind: self.source.source_kind_code(),
            configured_occurrence: self.source.configured_occurrence(),
            configuration_signature: self.source.configuration_signature(),
            last_complete: self
                .last_complete
                .as_ref()
                .map(|generation| generation.locator.clone()),
            visible_generation: self
                .provisional_visible
                .as_ref()
                .map(|generation| generation.locator.clone()),
            provisional_checkpoint: self.checkpoint.clone(),
            coverage_status: self.coverage as u8,
        }
    }
}

/// A scoped deletion is valid only because that exact scan scope completed.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ScopedTombstone {
    pub source_occurrence: u64,
    pub scope: String,
}

/// Reconciles source slots without allowing an incomplete source to affect a different source.
#[derive(Debug, Clone, Default)]
pub struct SourceReconciler;

impl SourceReconciler {
    pub fn reconcile(
        &self,
        existing: Vec<SourceSlotState>,
        configured_sources: Vec<MigrationSource>,
        facts: BTreeMap<u64, SourceRefreshFact>,
    ) -> ReconciliationResult {
        let existing = existing
            .into_iter()
            .map(|slot| (slot.source.configured_occurrence(), slot))
            .collect::<BTreeMap<_, _>>();
        let mut slots = Vec::with_capacity(configured_sources.len());
        let mut tombstones = BTreeSet::new();
        for source in configured_sources {
            let occurrence = source.configured_occurrence();
            let prior = existing
                .get(&occurrence)
                .cloned()
                .unwrap_or_else(|| SourceSlotState::uninitialized(source.clone()));
            let fact = facts.get(&occurrence);
            let (slot, authorized) = SourceSlotTransition::new(prior, fact).apply();
            for scope in authorized {
                tombstones.insert(ScopedTombstone {
                    source_occurrence: occurrence,
                    scope,
                });
            }
            slots.push(slot);
        }
        ReconciliationResult {
            slots,
            tombstones: tombstones.into_iter().collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationResult {
    pub slots: Vec<SourceSlotState>,
    pub tombstones: Vec<ScopedTombstone>,
}

#[derive(Debug, Clone)]
struct SourceSlotTransition<'a> {
    prior: SourceSlotState,
    fact: Option<&'a SourceRefreshFact>,
}
impl<'a> SourceSlotTransition<'a> {
    fn new(prior: SourceSlotState, fact: Option<&'a SourceRefreshFact>) -> Self {
        Self { prior, fact }
    }
    fn apply(self) -> (SourceSlotState, BTreeSet<String>) {
        match self.fact {
            Some(SourceRefreshFact::Complete { generation }) => (
                SourceSlotState {
                    source: self.prior.source,
                    last_complete: Some(generation.clone()),
                    provisional_visible: None,
                    checkpoint: None,
                    coverage: SourceCoverageStatus::Complete,
                },
                generation.completed_scopes.clone(),
            ),
            Some(SourceRefreshFact::Incomplete {
                generation,
                checkpoint,
            }) => (
                SourceSlotState {
                    source: self.prior.source,
                    last_complete: self.prior.last_complete,
                    provisional_visible: Some(generation.clone()),
                    checkpoint: Some(checkpoint.clone()),
                    coverage: SourceCoverageStatus::Incomplete,
                },
                // A provisional generation may upsert exact locators, but no unmatched deletion
                // escapes it. Completed file/discovery scopes are the only tombstone authority.
                generation.completed_scopes.clone(),
            ),
            Some(SourceRefreshFact::Failed) | None => (
                SourceSlotState {
                    source: self.prior.source,
                    last_complete: self.prior.last_complete,
                    provisional_visible: self.prior.provisional_visible,
                    checkpoint: self.prior.checkpoint,
                    coverage: SourceCoverageStatus::Failed,
                },
                BTreeSet::new(),
            ),
        }
    }
}

/// Bounded compaction input: one current child per reference after last-complete/provisional merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildContribution {
    pub reference: String,
    pub parent_reference: String,
    pub source_occurrence: u64,
}

/// Receives one compacted scalar parent summary at a time.
pub trait ParentSummarySink {
    fn observe_parent_summary(&mut self, parent_reference: String, child_count: u64);
}

/// Rebuilds parent scalar summaries from a source-sorted merged child view.  The upstream external
/// merge orders by `(parent_reference, child_reference, source_occurrence)`; this compactor retains
/// only the current parent and reference, so base and provisional summaries cannot double-count.
#[derive(Debug, Clone, Default)]
pub struct ParentSummaryCompactor;

impl ParentSummaryCompactor {
    pub fn compact_sorted<S: ParentSummarySink>(
        &self,
        children: impl IntoIterator<Item = ChildContribution>,
        sink: &mut S,
    ) {
        let mut parent: Option<String> = None;
        let mut last_reference: Option<String> = None;
        let mut count = 0_u64;
        for child in children {
            if parent.as_deref() != Some(child.parent_reference.as_str()) {
                if let Some(previous) = parent.replace(child.parent_reference.clone()) {
                    sink.observe_parent_summary(previous, count);
                }
                count = 0;
                last_reference = None;
            }
            if last_reference.as_deref() != Some(child.reference.as_str()) {
                count += 1;
                last_reference = Some(child.reference);
            }
        }
        if let Some(parent) = parent {
            sink.observe_parent_summary(parent, count);
        }
    }
}
