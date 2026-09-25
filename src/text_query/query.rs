//! The text query in both shapes: the contract's tree and the engine's tree.

use dotos_text_query::{NearQuery, Query, QueryTerm, SearchPhrase, SearchWord};
use signal_aggregator::{
    NearTextQuery, SearchPhrase as ContractSearchPhrase, TextQuery, TextQueryTerm,
    TranscriptBlockTextQuery,
};

use crate::text_query::{ContractWordDistance, ProjectionBudget, TextQueryProjectionFault};

/// Reads a contract tree and yields the engine's tree.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ContractQueryProjection<'a> {
    query: &'a TranscriptBlockTextQuery,
}

impl<'a> ContractQueryProjection<'a> {
    pub fn new(query: &'a TranscriptBlockTextQuery) -> Self {
        Self { query }
    }

    pub fn project(&self) -> Result<Query, TextQueryProjectionFault> {
        Self::node(self.query, &mut ProjectionBudget::default())
    }

    fn node(
        query: &TextQuery,
        budget: &mut ProjectionBudget,
    ) -> Result<Query, TextQueryProjectionFault> {
        budget.enter()?;
        let projected = match query {
            TextQuery::Contains(term) => {
                Ok(Query::Contains(ContractQueryTerm::new(term).project()))
            }
            TextQuery::AllOf(children) => Self::children(children, budget).map(Query::AllOf),
            TextQuery::AnyOf(children) => Self::children(children, budget).map(Query::AnyOf),
            TextQuery::Not(child) => {
                Self::node(child, budget).map(|child| Query::Not(Box::new(child)))
            }
            TextQuery::Near(near) => Self::near(near),
        };
        budget.leave();
        projected
    }

    fn children(
        children: &[TextQuery],
        budget: &mut ProjectionBudget,
    ) -> Result<Vec<Query>, TextQueryProjectionFault> {
        children
            .iter()
            .map(|child| Self::node(child, budget))
            .collect()
    }

    fn near(near: &NearTextQuery) -> Result<Query, TextQueryProjectionFault> {
        Ok(Query::Near(NearQuery::new(
            ContractQueryTerm::new(&near.left_text_query_term).project(),
            ContractQueryTerm::new(&near.right_text_query_term).project(),
            ContractWordDistance::new(near.word_distance).engine_distance()?,
        )))
    }
}

/// Reads the engine's tree and yields a contract tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineQueryProjection<'a> {
    query: &'a Query,
}

impl<'a> EngineQueryProjection<'a> {
    pub fn new(query: &'a Query) -> Self {
        Self { query }
    }

    pub fn project(&self) -> TranscriptBlockTextQuery {
        Self::node(self.query)
    }

    fn node(query: &Query) -> TextQuery {
        match query {
            Query::Contains(term) => TextQuery::Contains(EngineQueryTerm::new(term).project()),
            Query::AllOf(children) => TextQuery::AllOf(Self::children(children)),
            Query::AnyOf(children) => TextQuery::AnyOf(Self::children(children)),
            Query::Not(child) => TextQuery::Not(Box::new(Self::node(child))),
            Query::Near(near) => TextQuery::Near(NearTextQuery {
                left_text_query_term: EngineQueryTerm::new(&near.left).project(),
                right_text_query_term: EngineQueryTerm::new(&near.right).project(),
                word_distance: i64::from(near.distance.0),
            }),
        }
    }

    fn children(children: &[Query]) -> Vec<TextQuery> {
        children.iter().map(Self::node).collect()
    }
}

/// A contract term in the position an engine term occupies.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ContractQueryTerm<'a> {
    term: &'a TextQueryTerm,
}

impl<'a> ContractQueryTerm<'a> {
    pub fn new(term: &'a TextQueryTerm) -> Self {
        Self { term }
    }

    pub fn project(&self) -> QueryTerm {
        match self.term {
            TextQueryTerm::Word(word) => QueryTerm::Word(SearchWord::new(word.clone())),
            TextQueryTerm::Phrase(phrase) => {
                QueryTerm::Phrase(SearchPhrase::new(phrase.search_words.clone()))
            }
        }
    }
}

/// An engine term in the position a contract term occupies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineQueryTerm<'a> {
    term: &'a QueryTerm,
}

impl<'a> EngineQueryTerm<'a> {
    pub fn new(term: &'a QueryTerm) -> Self {
        Self { term }
    }

    pub fn project(&self) -> TextQueryTerm {
        match self.term {
            QueryTerm::Word(word) => TextQueryTerm::Word(word.value.clone()),
            QueryTerm::Phrase(phrase) => TextQueryTerm::Phrase(ContractSearchPhrase {
                search_words: phrase.words.clone(),
            }),
        }
    }
}
