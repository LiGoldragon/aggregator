use crate::AdapterKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaudeTranscriptAdapter;

impl ClaudeTranscriptAdapter {
    pub fn kind(&self) -> AdapterKind {
        AdapterKind::ClaudeTranscript
    }
}
