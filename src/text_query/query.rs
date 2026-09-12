//! The text query in both shapes: the contract's flat arena and the engine's tree.

use dotos_text_query::{NearQuery, Query, QueryTerm, SearchPhrase, SearchWord};
use signal_aggregator::{
    NearTextQuery, SearchPhrase as ContractSearchPhrase, TextQueryNode, TextQueryTerm,
    TranscriptBlockTextQuery,
};

use crate::text_query::{
    ArenaIndex, ContractWordDistance, MAXIMUM_PROJECTION_DEPTH, MAXIMUM_PROJECTION_NODES,
    TextQueryProjectionFault,
};

/// Reads a contract arena and yields the engine's tree.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ContractQueryProjection<'a> {
    query: &'a TranscriptBlockTextQuery,
}

impl<'a> ContractQueryProjection<'a> {
    pub fn new(query: &'a TranscriptBlockTextQuery) -> Self {
        Self { query }
    }

    pub fn project(&self) -> Result<Query, TextQueryProjectionFault> {
        let length = self.query.text_query_nodes.len();
        if length > MAXIMUM_PROJECTION_NODES {
            return Err(TextQueryProjectionFault::TooLarge { length });
        }
        self.node(self.query.text_query_root, &mut Vec::new())
    }

    fn node(&self, index: i64, reaching: &mut Vec<i64>) -> Result<Query, TextQueryProjectionFault> {
        if reaching.contains(&index) {
            return Err(TextQueryProjectionFault::Cycle { index });
        }
        if reaching.len() >= MAXIMUM_PROJECTION_DEPTH {
            return Err(TextQueryProjectionFault::TooDeep);
        }
        let nodes = &self.query.text_query_nodes;
        let resolved = ArenaIndex::new(index, nodes.len()).resolve()?;
        reaching.push(index);
        let projected = match &nodes[resolved] {
            TextQueryNode::Contains(term) => {
                Ok(Query::Contains(ContractQueryTerm::new(term).project()))
            }
            TextQueryNode::AllOf(children) => self.children(children, reaching).map(Query::AllOf),
            TextQueryNode::AnyOf(children) => self.children(children, reaching).map(Query::AnyOf),
            TextQueryNode::Not(child) => self
                .node(*child, reaching)
                .map(|child| Query::Not(Box::new(child))),
            TextQueryNode::Near(near) => self.near(near),
        };
        reaching.pop();
        projected
    }

    fn children(
        &self,
        children: &[i64],
        reaching: &mut Vec<i64>,
    ) -> Result<Vec<Query>, TextQueryProjectionFault> {
        children
            .iter()
            .map(|child| self.node(*child, reaching))
            .collect()
    }

    fn near(&self, near: &NearTextQuery) -> Result<Query, TextQueryProjectionFault> {
        Ok(Query::Near(NearQuery::new(
            ContractQueryTerm::new(&near.left_text_query_term).project(),
            ContractQueryTerm::new(&near.right_text_query_term).project(),
            ContractWordDistance::new(near.word_distance).engine_distance()?,
        )))
    }
}

/// Reads the engine's tree and yields a contract arena.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineQueryProjection<'a> {
    query: &'a Query,
}

impl<'a> EngineQueryProjection<'a> {
    pub fn new(query: &'a Query) -> Self {
        Self { query }
    }

    pub fn project(&self) -> TranscriptBlockTextQuery {
        let mut nodes = Vec::new();
        let root = Self::append(self.query, &mut nodes);
        TranscriptBlockTextQuery {
            text_query_nodes: nodes,
            text_query_root: root,
        }
    }

    /// Appends a subtree in post-order, so a child always sits below its parent
    /// and an arena written here never reaches forward.
    fn append(query: &Query, nodes: &mut Vec<TextQueryNode>) -> i64 {
        let node = match query {
            Query::Contains(term) => TextQueryNode::Contains(EngineQueryTerm::new(term).project()),
            Query::AllOf(children) => TextQueryNode::AllOf(Self::append_children(children, nodes)),
            Query::AnyOf(children) => TextQueryNode::AnyOf(Self::append_children(children, nodes)),
            Query::Not(child) => TextQueryNode::Not(Self::append(child, nodes)),
            Query::Near(near) => TextQueryNode::Near(NearTextQuery {
                left_text_query_term: EngineQueryTerm::new(&near.left).project(),
                right_text_query_term: EngineQueryTerm::new(&near.right).project(),
                word_distance: i64::from(near.distance.0),
            }),
        };
        nodes.push(node);
        (nodes.len() - 1) as i64
    }

    fn append_children(children: &[Query], nodes: &mut Vec<TextQueryNode>) -> Vec<i64> {
        children
            .iter()
            .map(|child| Self::append(child, nodes))
            .collect()
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
