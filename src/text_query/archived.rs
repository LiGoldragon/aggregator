//! The contract trees still archived: measured against the projection budget
//! before a received frame is decoded.
//!
//! A frame's validation is already bounded by its pointer nesting (see
//! `wire::MAXIMUM_FRAME_NESTING`), so this walk recurses no deeper than that
//! ceiling; it then applies the exact bounds the projection applies, without
//! allocating, so a tree past them is refused before any of it becomes a Rust
//! value.

use signal_aggregator::{ArchivedMatchEvidence, ArchivedTextQuery};

use crate::text_query::{ProjectionBudget, TextQueryProjectionFault};

/// An archived contract tree that spends a projection budget node by node.
pub trait ArchivedTree {
    /// Enters this node and every node under it.
    fn spend(&self, budget: &mut ProjectionBudget) -> Result<(), TextQueryProjectionFault>;

    /// Measures the whole tree against a fresh budget.
    fn measure(&self) -> Result<(), TextQueryProjectionFault> {
        self.spend(&mut ProjectionBudget::default())
    }
}

impl ArchivedTree for ArchivedTextQuery {
    fn spend(&self, budget: &mut ProjectionBudget) -> Result<(), TextQueryProjectionFault> {
        budget.enter()?;
        match self {
            ArchivedTextQuery::Contains(_) | ArchivedTextQuery::Near(_) => {}
            ArchivedTextQuery::AllOf(children) | ArchivedTextQuery::AnyOf(children) => {
                for child in children.iter() {
                    child.spend(budget)?;
                }
            }
            ArchivedTextQuery::Not(child) => child.get().spend(budget)?,
        }
        budget.leave();
        Ok(())
    }
}

impl ArchivedTree for ArchivedMatchEvidence {
    fn spend(&self, budget: &mut ProjectionBudget) -> Result<(), TextQueryProjectionFault> {
        budget.enter()?;
        match self {
            ArchivedMatchEvidence::Contains(_)
            | ArchivedMatchEvidence::Near(_)
            | ArchivedMatchEvidence::Not => {}
            ArchivedMatchEvidence::AllOf(children) | ArchivedMatchEvidence::AnyOf(children) => {
                for child in children.iter() {
                    child.spend(budget)?;
                }
            }
        }
        budget.leave();
        Ok(())
    }
}
