//! Projection between the contract's text-query trees and the matching engine's
//! trees.
//!
//! The contract carries a transcript-block text query as the recursive
//! `TextQuery` and matching evidence as the recursive `MatchEvidence`.
//! `dotos-text-query` is the matching engine and speaks its own recursive
//! shapes; this module is the only place where the two meet, and the engine's
//! types never reach the wire.

pub mod evidence;
pub mod query;

pub use evidence::{ContractEvidenceProjection, EngineEvidenceProjection};
pub use query::{ContractQueryProjection, EngineQueryProjection};

/// The greatest nesting a projected contract tree may reach.
///
/// The tree is peer input and its depth is bounded only by the frame that
/// carried it, so a walk over it is bounded as it recurses.
pub const MAXIMUM_PROJECTION_DEPTH: usize = 32;

/// The greatest number of nodes a projected contract tree may hold.
pub const MAXIMUM_PROJECTION_NODES: usize = 256;

/// A contract tree that is not a bounded query, or a value the engine cannot
/// hold.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TextQueryProjectionFault {
    #[error("tree nesting exceeds {MAXIMUM_PROJECTION_DEPTH}")]
    TooDeep,
    #[error("tree exceeds {MAXIMUM_PROJECTION_NODES} nodes")]
    TooLarge,
    #[error("word distance {distance} is outside the representable range")]
    DistanceOutsideRange { distance: i64 },
    #[error("word position {position} is outside the representable range")]
    PositionOutsideRange { position: i64 },
}

/// What a walk over one contract tree has spent: the nesting it is inside and
/// the nodes it has entered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ProjectionBudget {
    depth: usize,
    nodes: usize,
}

impl ProjectionBudget {
    /// Enters one node, refusing it when the tree grows past either bound. A
    /// node is refused before its children are read.
    pub fn enter(&mut self) -> Result<(), TextQueryProjectionFault> {
        if self.depth >= MAXIMUM_PROJECTION_DEPTH {
            return Err(TextQueryProjectionFault::TooDeep);
        }
        if self.nodes >= MAXIMUM_PROJECTION_NODES {
            return Err(TextQueryProjectionFault::TooLarge);
        }
        self.depth += 1;
        self.nodes += 1;
        Ok(())
    }

    /// Leaves the node last entered; its nodes stay spent.
    pub fn leave(&mut self) {
        self.depth -= 1;
    }
}

/// A contract word distance, which the wire carries wider than the engine does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContractWordDistance {
    distance: i64,
}

impl ContractWordDistance {
    pub fn new(distance: i64) -> Self {
        Self { distance }
    }

    pub fn engine_distance(
        &self,
    ) -> Result<dotos_text_query::WordDistance, TextQueryProjectionFault> {
        u32::try_from(self.distance)
            .map(dotos_text_query::WordDistance)
            .map_err(|_| TextQueryProjectionFault::DistanceOutsideRange {
                distance: self.distance,
            })
    }
}

/// A contract word position, which the wire carries wider than the engine does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContractWordPosition {
    position: i64,
}

impl ContractWordPosition {
    pub fn new(position: i64) -> Self {
        Self { position }
    }

    pub fn engine_position(&self) -> Result<u32, TextQueryProjectionFault> {
        u32::try_from(self.position).map_err(|_| TextQueryProjectionFault::PositionOutsideRange {
            position: self.position,
        })
    }
}
