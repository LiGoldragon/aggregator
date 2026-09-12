//! Projection between the contract's flat text-query arenas and the matching
//! engine's recursive shapes.
//!
//! A Signal root is an rkyv archive, and rkyv's derive cannot close the trait
//! bounds of a self-reaching type, so no recursive type crosses the wire. The
//! contract therefore carries a text query as a vector of nodes plus a root
//! index, and matching evidence the same way. `dotos-text-query` is the
//! matching engine and speaks the recursive shapes; this module is the only
//! place where the two meet, and the engine's types never reach the wire.

pub mod evidence;
pub mod query;

pub use evidence::{ContractEvidenceProjection, EngineEvidenceProjection};
pub use query::{ContractQueryProjection, EngineQueryProjection};

/// The greatest nesting a projected arena may reach.
///
/// The arena is peer input and its edges are unconstrained by the wire type, so
/// a walk over it is bounded before it recurses rather than after.
pub const MAXIMUM_PROJECTION_DEPTH: usize = 32;

/// The greatest number of nodes a projected arena may hold.
pub const MAXIMUM_PROJECTION_NODES: usize = 256;

/// A flat arena that does not describe a finite tree.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TextQueryProjectionFault {
    #[error("node index {index} is outside the {length} node arena")]
    IndexOutsideArena { index: i64, length: usize },
    #[error("node index {index} reaches itself")]
    Cycle { index: i64 },
    #[error("arena nesting exceeds {MAXIMUM_PROJECTION_DEPTH}")]
    TooDeep,
    #[error("arena of {length} nodes exceeds {MAXIMUM_PROJECTION_NODES}")]
    TooLarge { length: usize },
    #[error("word distance {distance} is outside the representable range")]
    DistanceOutsideRange { distance: i64 },
    #[error("word position {position} is outside the representable range")]
    PositionOutsideRange { position: i64 },
}

/// An arena index paired with the arena it indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArenaIndex {
    index: i64,
    length: usize,
}

impl ArenaIndex {
    pub fn new(index: i64, length: usize) -> Self {
        Self { index, length }
    }

    /// Resolves the index, refusing anything the arena does not hold. A count
    /// that must index memory is converted explicitly; a negative index is a
    /// fault, never a wrapped cast.
    pub fn resolve(&self) -> Result<usize, TextQueryProjectionFault> {
        let resolved = usize::try_from(self.index).map_err(|_| {
            TextQueryProjectionFault::IndexOutsideArena {
                index: self.index,
                length: self.length,
            }
        })?;
        if resolved < self.length {
            Ok(resolved)
        } else {
            Err(TextQueryProjectionFault::IndexOutsideArena {
                index: self.index,
                length: self.length,
            })
        }
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
