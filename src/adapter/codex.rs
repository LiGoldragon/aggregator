use crate::AdapterKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodexTranscriptAdapter;

impl CodexTranscriptAdapter {
    pub fn kind(&self) -> AdapterKind {
        AdapterKind::CodexTranscript
    }
}
