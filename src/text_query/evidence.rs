//! Matching evidence in both shapes: the engine's tree and the contract's flat arena.

use dotos_text_query::{
    CompositeEvidence, ContainsEvidence, MatchEvidence, NearEvidence, Occurrence,
    evidence::NearOccurrencePair,
};
use signal_aggregator::{
    ContainsEvidence as ContractContainsEvidence, MatchEvidenceNode,
    NearEvidence as ContractNearEvidence, NearOccurrencePair as ContractNearOccurrencePair,
    Occurrence as ContractOccurrence, TranscriptBlockSearchEvidence,
};

use crate::text_query::{
    ArenaIndex, ContractWordDistance, ContractWordPosition, MAXIMUM_PROJECTION_DEPTH,
    MAXIMUM_PROJECTION_NODES, TextQueryProjectionFault,
    query::{ContractQueryTerm, EngineQueryTerm},
};

/// Reads the engine's evidence tree and yields a contract arena.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineEvidenceProjection<'a> {
    evidence: &'a MatchEvidence,
}

impl<'a> EngineEvidenceProjection<'a> {
    pub fn new(evidence: &'a MatchEvidence) -> Self {
        Self { evidence }
    }

    pub fn project(&self) -> TranscriptBlockSearchEvidence {
        let mut nodes = Vec::new();
        let root = Self::append(self.evidence, &mut nodes);
        TranscriptBlockSearchEvidence {
            match_evidence_nodes: nodes,
            match_evidence_root: root,
        }
    }

    fn append(evidence: &MatchEvidence, nodes: &mut Vec<MatchEvidenceNode>) -> i64 {
        let node = match evidence {
            MatchEvidence::Contains(contains) => {
                MatchEvidenceNode::Contains(ContractContainsEvidence {
                    text_query_term: EngineQueryTerm::new(&contains.term).project(),
                    occurrences: contains
                        .occurrences
                        .iter()
                        .map(EngineOccurrence::project)
                        .collect(),
                })
            }
            MatchEvidence::AllOf(composite) => {
                MatchEvidenceNode::AllOf(Self::append_children(composite, nodes))
            }
            MatchEvidence::AnyOf(composite) => {
                MatchEvidenceNode::AnyOf(Self::append_children(composite, nodes))
            }
            MatchEvidence::Not => MatchEvidenceNode::Not,
            MatchEvidence::Near(near) => MatchEvidenceNode::Near(ContractNearEvidence {
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
        };
        nodes.push(node);
        (nodes.len() - 1) as i64
    }

    fn append_children(
        composite: &CompositeEvidence,
        nodes: &mut Vec<MatchEvidenceNode>,
    ) -> Vec<i64> {
        composite
            .matches
            .iter()
            .map(|child| Self::append(child, nodes))
            .collect()
    }
}

/// Reads a contract evidence arena and yields the engine's tree.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ContractEvidenceProjection<'a> {
    evidence: &'a TranscriptBlockSearchEvidence,
}

impl<'a> ContractEvidenceProjection<'a> {
    pub fn new(evidence: &'a TranscriptBlockSearchEvidence) -> Self {
        Self { evidence }
    }

    pub fn project(&self) -> Result<MatchEvidence, TextQueryProjectionFault> {
        let length = self.evidence.match_evidence_nodes.len();
        if length > MAXIMUM_PROJECTION_NODES {
            return Err(TextQueryProjectionFault::TooLarge { length });
        }
        self.node(self.evidence.match_evidence_root, &mut Vec::new())
    }

    fn node(
        &self,
        index: i64,
        reaching: &mut Vec<i64>,
    ) -> Result<MatchEvidence, TextQueryProjectionFault> {
        if reaching.contains(&index) {
            return Err(TextQueryProjectionFault::Cycle { index });
        }
        if reaching.len() >= MAXIMUM_PROJECTION_DEPTH {
            return Err(TextQueryProjectionFault::TooDeep);
        }
        let nodes = &self.evidence.match_evidence_nodes;
        let resolved = ArenaIndex::new(index, nodes.len()).resolve()?;
        reaching.push(index);
        let projected = match &nodes[resolved] {
            MatchEvidenceNode::Contains(contains) => Self::contains(contains),
            MatchEvidenceNode::AllOf(children) => self
                .children(children, reaching)
                .map(|matches| MatchEvidence::AllOf(CompositeEvidence::new(matches))),
            MatchEvidenceNode::AnyOf(children) => self
                .children(children, reaching)
                .map(|matches| MatchEvidence::AnyOf(CompositeEvidence::new(matches))),
            MatchEvidenceNode::Not => Ok(MatchEvidence::Not),
            MatchEvidenceNode::Near(near) => Self::near(near),
        };
        reaching.pop();
        projected
    }

    fn children(
        &self,
        children: &[i64],
        reaching: &mut Vec<i64>,
    ) -> Result<Vec<MatchEvidence>, TextQueryProjectionFault> {
        children
            .iter()
            .map(|child| self.node(*child, reaching))
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
