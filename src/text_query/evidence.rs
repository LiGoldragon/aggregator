//! Matching evidence in both shapes: the engine's tree and the contract's tree.

use dotos_text_query::{
    CompositeEvidence, ContainsEvidence, MatchEvidence, NearEvidence, Occurrence,
    evidence::NearOccurrencePair,
};
use signal_aggregator::{
    ContainsEvidence as ContractContainsEvidence, MatchEvidence as ContractMatchEvidence,
    NearEvidence as ContractNearEvidence, NearOccurrencePair as ContractNearOccurrencePair,
    Occurrence as ContractOccurrence, TranscriptBlockSearchEvidence,
};

use crate::text_query::{
    ContractWordDistance, ContractWordPosition, ProjectionBudget, TextQueryProjectionFault,
    query::{ContractQueryTerm, EngineQueryTerm},
};

/// Reads the engine's evidence tree and yields a contract tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineEvidenceProjection<'a> {
    evidence: &'a MatchEvidence,
}

impl<'a> EngineEvidenceProjection<'a> {
    pub fn new(evidence: &'a MatchEvidence) -> Self {
        Self { evidence }
    }

    pub fn project(&self) -> TranscriptBlockSearchEvidence {
        Self::node(self.evidence)
    }

    fn node(evidence: &MatchEvidence) -> ContractMatchEvidence {
        match evidence {
            MatchEvidence::Contains(contains) => {
                ContractMatchEvidence::Contains(ContractContainsEvidence {
                    text_query_term: EngineQueryTerm::new(&contains.term).project(),
                    occurrences: contains
                        .occurrences
                        .iter()
                        .map(EngineOccurrence::project)
                        .collect(),
                })
            }
            MatchEvidence::AllOf(composite) => {
                ContractMatchEvidence::AllOf(Self::children(composite))
            }
            MatchEvidence::AnyOf(composite) => {
                ContractMatchEvidence::AnyOf(Self::children(composite))
            }
            MatchEvidence::Not => ContractMatchEvidence::Not,
            MatchEvidence::Near(near) => ContractMatchEvidence::Near(ContractNearEvidence {
                left_text_query_term: EngineQueryTerm::new(&near.left).project(),
                right_text_query_term: EngineQueryTerm::new(&near.right).project(),
                word_distance: i64::from(near.distance.0),
                near_occurrence_pairs: near
                    .pairs
                    .iter()
                    .map(|pair| ContractNearOccurrencePair {
                        left_occurrence: EngineOccurrence::project(&pair.left),
                        right_occurrence: EngineOccurrence::project(&pair.right),
                        occurrence_gap: i64::from(pair.gap.0),
                    })
                    .collect(),
            }),
        }
    }

    fn children(composite: &CompositeEvidence) -> Vec<ContractMatchEvidence> {
        composite.matches.iter().map(Self::node).collect()
    }
}

/// Reads a contract evidence tree and yields the engine's tree.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ContractEvidenceProjection<'a> {
    evidence: &'a TranscriptBlockSearchEvidence,
}

impl<'a> ContractEvidenceProjection<'a> {
    pub fn new(evidence: &'a TranscriptBlockSearchEvidence) -> Self {
        Self { evidence }
    }

    pub fn project(&self) -> Result<MatchEvidence, TextQueryProjectionFault> {
        Self::node(self.evidence, &mut ProjectionBudget::default())
    }

    fn node(
        evidence: &ContractMatchEvidence,
        budget: &mut ProjectionBudget,
    ) -> Result<MatchEvidence, TextQueryProjectionFault> {
        budget.enter()?;
        let projected = match evidence {
            ContractMatchEvidence::Contains(contains) => Self::contains(contains),
            ContractMatchEvidence::AllOf(children) => Self::children(children, budget)
                .map(|matches| MatchEvidence::AllOf(CompositeEvidence::new(matches))),
            ContractMatchEvidence::AnyOf(children) => Self::children(children, budget)
                .map(|matches| MatchEvidence::AnyOf(CompositeEvidence::new(matches))),
            ContractMatchEvidence::Not => Ok(MatchEvidence::Not),
            ContractMatchEvidence::Near(near) => Self::near(near),
        };
        budget.leave();
        projected
    }

    fn children(
        children: &[ContractMatchEvidence],
        budget: &mut ProjectionBudget,
    ) -> Result<Vec<MatchEvidence>, TextQueryProjectionFault> {
        children
            .iter()
            .map(|child| Self::node(child, budget))
            .collect()
    }

    fn contains(
        contains: &ContractContainsEvidence,
    ) -> Result<MatchEvidence, TextQueryProjectionFault> {
        Ok(MatchEvidence::Contains(ContainsEvidence::new(
            ContractQueryTerm::new(&contains.text_query_term).project(),
            contains
                .occurrences
                .iter()
                .map(ContractOccurrenceProjection::project)
                .collect::<Result<Vec<_>, _>>()?,
        )))
    }

    fn near(near: &ContractNearEvidence) -> Result<MatchEvidence, TextQueryProjectionFault> {
        Ok(MatchEvidence::Near(NearEvidence::new(
            ContractQueryTerm::new(&near.left_text_query_term).project(),
            ContractQueryTerm::new(&near.right_text_query_term).project(),
            ContractWordDistance::new(near.word_distance).engine_distance()?,
            near.near_occurrence_pairs
                .iter()
                .map(|pair| {
                    Ok(NearOccurrencePair::new(
                        ContractOccurrenceProjection::project(&pair.left_occurrence)?,
                        ContractOccurrenceProjection::project(&pair.right_occurrence)?,
                        ContractWordDistance::new(pair.occurrence_gap).engine_distance()?,
                    ))
                })
                .collect::<Result<Vec<_>, TextQueryProjectionFault>>()?,
        )))
    }
}

/// An engine occurrence in the position a contract occurrence occupies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineOccurrence;

impl EngineOccurrence {
    pub fn project(occurrence: &Occurrence) -> ContractOccurrence {
        ContractOccurrence {
            start_word_position: i64::from(occurrence.start),
            end_word_position: i64::from(occurrence.end),
        }
    }
}

/// A contract occurrence in the position an engine occurrence occupies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContractOccurrenceProjection;

impl ContractOccurrenceProjection {
    pub fn project(
        occurrence: &ContractOccurrence,
    ) -> Result<Occurrence, TextQueryProjectionFault> {
        Ok(Occurrence::new(
            ContractWordPosition::new(occurrence.start_word_position).engine_position()?,
            ContractWordPosition::new(occurrence.end_word_position).engine_position()?,
        ))
    }
}
