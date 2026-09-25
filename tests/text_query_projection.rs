//! The tree the contract carries and the tree the matching engine runs.
//!
//! Expected values are written here as engine trees and as contract trees by
//! hand; no expectation is computed through the projection under test.

use aggregator::{
    ContractEvidenceProjection, ContractQueryProjection, EngineEvidenceProjection,
    EngineQueryProjection, TextQueryProjectionFault,
};
use dotos_text_query::{
    CompositeEvidence, ContainsEvidence, MatchEvidence, NearEvidence, Occurrence, Query, QueryTerm,
    SearchText, WordDistance, evidence::NearOccurrencePair,
};
use signal_aggregator::{
    ContainsEvidence as ContractContainsEvidence, MatchEvidence as ContractMatchEvidence,
    NearEvidence as ContractNearEvidence, NearOccurrencePair as ContractNearOccurrencePair,
    NearTextQuery, Occurrence as ContractOccurrence, SearchPhrase, TextQuery, TextQueryTerm,
};

fn nested_query() -> Query {
    Query::all_of(vec![
        Query::contains(QueryTerm::word("quota")),
        Query::any_of(vec![
            Query::contains(QueryTerm::phrase(vec![
                String::from("rate"),
                String::from("limit"),
            ])),
            Query::negated(Query::contains(QueryTerm::word("draft"))),
        ]),
        Query::near(
            QueryTerm::word("quota"),
            QueryTerm::word("reset"),
            WordDistance::new(6),
        ),
    ])
}

#[test]
fn a_nested_query_survives_the_contract_tree_unchanged() {
    let tree = EngineQueryProjection::new(&nested_query()).project();
    assert_eq!(
        ContractQueryProjection::new(&tree)
            .project()
            .expect("the tree must project"),
        nested_query()
    );
}

#[test]
fn a_hand_written_tree_carries_its_children_in_place() {
    let tree = TextQuery::AnyOf(vec![
        TextQuery::Contains(TextQueryTerm::Word(String::from("alpha"))),
        TextQuery::Contains(TextQueryTerm::Phrase(SearchPhrase {
            search_words: vec![String::from("bounded"), String::from("text")],
        })),
    ]);
    assert_eq!(
        ContractQueryProjection::new(&tree)
            .project()
            .expect("the tree must project"),
        Query::any_of(vec![
            Query::contains(QueryTerm::word("alpha")),
            Query::contains(QueryTerm::phrase(vec![
                String::from("bounded"),
                String::from("text"),
            ])),
        ])
    );
}

#[test]
fn the_engine_tree_projects_to_the_hand_written_contract_tree() {
    assert_eq!(
        EngineQueryProjection::new(&nested_query()).project(),
        TextQuery::AllOf(vec![
            TextQuery::Contains(TextQueryTerm::Word(String::from("quota"))),
            TextQuery::AnyOf(vec![
                TextQuery::Contains(TextQueryTerm::Phrase(SearchPhrase {
                    search_words: vec![String::from("rate"), String::from("limit")],
                })),
                TextQuery::Not(Box::new(TextQuery::Contains(TextQueryTerm::Word(
                    String::from("draft"),
                )))),
            ]),
            TextQuery::Near(NearTextQuery {
                left_text_query_term: TextQueryTerm::Word(String::from("quota")),
                right_text_query_term: TextQueryTerm::Word(String::from("reset")),
                word_distance: 6,
            }),
        ])
    );
}

#[test]
fn a_word_distance_the_engine_cannot_hold_is_a_fault() {
    let tree = TextQuery::Near(NearTextQuery {
        left_text_query_term: TextQueryTerm::Word(String::from("quota")),
        right_text_query_term: TextQueryTerm::Word(String::from("reset")),
        word_distance: -1,
    });
    assert_eq!(
        ContractQueryProjection::new(&tree).project(),
        Err(TextQueryProjectionFault::DistanceOutsideRange { distance: -1 })
    );
}

#[test]
fn a_tree_wider_than_the_node_bound_is_a_fault() {
    let leaf = TextQuery::Contains(TextQueryTerm::Word(String::from("leaf")));
    let tree = TextQuery::AnyOf(vec![leaf; 256]);
    assert_eq!(
        ContractQueryProjection::new(&tree).project(),
        Err(TextQueryProjectionFault::TooLarge)
    );
}

#[test]
fn a_tree_at_the_node_bound_projects() {
    let leaf = TextQuery::Contains(TextQueryTerm::Word(String::from("leaf")));
    let tree = TextQuery::AnyOf(vec![leaf; 255]);
    assert_eq!(
        ContractQueryProjection::new(&tree)
            .project()
            .expect("255 leaves and their parent are 256 nodes"),
        Query::any_of(vec![Query::contains(QueryTerm::word("leaf")); 255])
    );
}

#[test]
fn a_tree_deeper_than_the_bound_is_a_fault() {
    let mut tree = TextQuery::Contains(TextQueryTerm::Word(String::from("leaf")));
    for _ in 0..64 {
        tree = TextQuery::Not(Box::new(tree));
    }
    assert_eq!(
        ContractQueryProjection::new(&tree).project(),
        Err(TextQueryProjectionFault::TooDeep)
    );
}

#[test]
fn evidence_from_a_real_match_survives_the_contract_tree_unchanged() {
    let text = SearchText::new("quota reset is pending, the rate limit holds");
    let evidence = nested_query()
        .find_in(&text)
        .evidence()
        .cloned()
        .expect("the corpus must match the query");
    let tree = EngineEvidenceProjection::new(&evidence).project();
    assert_eq!(
        ContractEvidenceProjection::new(&tree)
            .project()
            .expect("the evidence tree must project"),
        evidence
    );
}

#[test]
fn hand_written_evidence_carries_its_occurrences() {
    let evidence = MatchEvidence::AllOf(CompositeEvidence::new(vec![
        MatchEvidence::Contains(ContainsEvidence::new(
            QueryTerm::word("quota"),
            vec![Occurrence::new(0, 0)],
        )),
        MatchEvidence::Not,
        MatchEvidence::Near(NearEvidence::new(
            QueryTerm::word("quota"),
            QueryTerm::word("reset"),
            WordDistance::new(6),
            vec![NearOccurrencePair::new(
                Occurrence::new(0, 0),
                Occurrence::new(1, 1),
                WordDistance::new(0),
            )],
        )),
    ]));
    let tree = EngineEvidenceProjection::new(&evidence).project();
    assert_eq!(
        tree,
        ContractMatchEvidence::AllOf(vec![
            ContractMatchEvidence::Contains(ContractContainsEvidence {
                text_query_term: TextQueryTerm::Word(String::from("quota")),
                occurrences: vec![ContractOccurrence {
                    start_word_position: 0,
                    end_word_position: 0,
                }],
            }),
            ContractMatchEvidence::Not,
            ContractMatchEvidence::Near(ContractNearEvidence {
                left_text_query_term: TextQueryTerm::Word(String::from("quota")),
                right_text_query_term: TextQueryTerm::Word(String::from("reset")),
                word_distance: 6,
                near_occurrence_pairs: vec![ContractNearOccurrencePair {
                    left_occurrence: ContractOccurrence {
                        start_word_position: 0,
                        end_word_position: 0,
                    },
                    right_occurrence: ContractOccurrence {
                        start_word_position: 1,
                        end_word_position: 1,
                    },
                    occurrence_gap: 0,
                }],
            }),
        ])
    );
    assert_eq!(
        ContractEvidenceProjection::new(&tree)
            .project()
            .expect("the evidence tree must project"),
        evidence
    );
}

#[test]
fn an_evidence_position_the_engine_cannot_hold_is_a_fault() {
    let tree = ContractMatchEvidence::AnyOf(vec![ContractMatchEvidence::Contains(
        ContractContainsEvidence {
            text_query_term: TextQueryTerm::Word(String::from("quota")),
            occurrences: vec![ContractOccurrence {
                start_word_position: -3,
                end_word_position: 0,
            }],
        },
    )]);
    assert_eq!(
        ContractEvidenceProjection::new(&tree).project(),
        Err(TextQueryProjectionFault::PositionOutsideRange { position: -3 })
    );
}

#[test]
fn an_evidence_tree_deeper_than_the_bound_is_a_fault() {
    let mut tree = ContractMatchEvidence::Not;
    for _ in 0..64 {
        tree = ContractMatchEvidence::AllOf(vec![tree]);
    }
    assert_eq!(
        ContractEvidenceProjection::new(&tree).project(),
        Err(TextQueryProjectionFault::TooDeep)
    );
}
