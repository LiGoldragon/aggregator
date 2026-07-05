use crate::AdapterKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepositoryAdapter;

impl RepositoryAdapter {
    pub fn kind(&self) -> AdapterKind {
        AdapterKind::Repository
    }
}
