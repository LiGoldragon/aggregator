//! The flat arena the contract carries and the tree the matching engine runs.
//!
//! Expected values are written here as engine trees and as arenas by hand; no
//! expectation is computed through the projection under test.

use aggregator::{
    ContractEvidenceProjection, ContractQueryProjection, EngineEvidenceProjection,
    EngineQueryProjection, TextQueryProjectionFault,
};
use dotos_text_query::{
    CompositeEvidence, ContainsEvidence, MatchEvidence, NearEvidence, Occurrence, Query, QueryTerm,
    SearchText, WordDistance, evidence::NearOccurrencePair,
};
use signal_aggregator::{
    MatchEvidenceNode, SearchPhrase, TextQueryNode, TextQueryTerm, TranscriptBlockSearchEvidence,
    TranscriptBlockTextQuery,
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
fn a_nested_query_survives_the_arena_unchanged() {
    let arena = EngineQueryProjection::new(&nested_query()).project();
    assert_eq!(
        ContractQueryProjection::new(&arena)
            .project()
            .expect("the arena must project"),
        nested_query()
    );
}

#[test]
fn a_hand_written_arena_names_its_children_by_index() {
    let arena = TranscriptBlockTextQuery {
        text_query_nodes: vec![
            TextQueryNode::Contains(TextQueryTerm::Word(String::from("alpha"))),
            TextQueryNode::Contains(TextQueryTerm::Phrase(SearchPhrase {
                search_words: vec![String::from("bounded"), String::from("text")],
            })),
            TextQueryNode::AnyOf(vec![0, 1]),
        ],
        text_query_root: 2,
    };
    assert_eq!(
        ContractQueryProjection::new(&arena)
            .project()
            .expect("the arena must project"),
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
fn an_index_outside_the_arena_is_a_fault() {
    let arena = TranscriptBlockTextQuery {
        text_query_nodes: vec![TextQueryNode::AllOf(vec![7])],
        text_query_root: 0,
    };
    assert_eq!(
        ContractQueryProjection::new(&arena).project(),
        Err(TextQueryProjectionFault::IndexOutsideArena {
            index: 7,
            length: 1,
        })
    );
}

#[test]
fn a_negative_index_is_a_fault() {
    let arena = TranscriptBlockTextQuery {
        text_query_nodes: vec![TextQueryNode::Not(-1)],
        text_query_root: 0,
    };
    assert_eq!(
        ContractQueryProjection::new(&arena).project(),
        Err(TextQueryProjectionFault::IndexOutsideArena {
            index: -1,
            length: 1,
        })
    );
}

#[test]
fn a_node_that_reaches_itself_is_a_fault() {
    let arena = TranscriptBlockTextQuery {
        text_query_nodes: vec![TextQueryNode::Not(1), TextQueryNode::AllOf(vec![0])],
        text_query_root: 0,
    };
    assert_eq!(
        ContractQueryProjection::new(&arena).project(),
        Err(TextQueryProjectionFault::Cycle { index: 0 })
    );
}

#[test]
fn an_arena_deeper_than_the_bound_is_a_fault() {
    let depth = 64;
    let mut text_query_nodes = vec![TextQueryNode::Contains(TextQueryTerm::Word(String::from(
        "leaf",
    )))];
    for index in 0..depth {
        text_query_nodes.push(TextQueryNode::Not(index));
    }
    let arena = TranscriptBlockTextQuery {
        text_query_root: depth,
        text_query_nodes,
    };
    assert_eq!(
        ContractQueryProjection::new(&arena).project(),
        Err(TextQueryProjectionFault::TooDeep)
    );
}

#[test]
fn evidence_from_a_real_match_survives_the_arena_unchanged() {
    let text = SearchText::new("quota reset is pending, the rate limit holds");
    let evidence = nested_query()
        .find_in(&text)
        .evidence()
        .cloned()
        .expect("the corpus must match the query");
    let arena = EngineEvidenceProjection::new(&evidence).project();
    assert_eq!(
        ContractEvidenceProjection::new(&arena)
            .project()
            .expect("the evidence arena must project"),
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
    let arena = EngineEvidenceProjection::new(&evidence).project();
    assert_eq!(arena.match_evidence_nodes.len(), 4);
    assert_eq!(arena.match_evidence_root, 3);
    assert!(matches!(
        arena.match_evidence_nodes[1],
        MatchEvidenceNode::Not
    ));
    assert_eq!(
        ContractEvidenceProjection::new(&arena)
            .project()
            .expect("the evidence arena must project"),
        evidence
    );
}

#[test]
fn an_evidence_index_outside_the_arena_is_a_fault() {
    let arena = TranscriptBlockSearchEvidence {
        match_evidence_nodes: vec![MatchEvidenceNode::AnyOf(vec![3])],
        match_evidence_root: 0,
    };
    assert_eq!(
        ContractEvidenceProjection::new(&arena).project(),
        Err(TextQueryProjectionFault::IndexOutsideArena {
            index: 3,
            length: 1,
        })
    );
}
