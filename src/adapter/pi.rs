use crate::AdapterKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PiTranscriptAdapter;

impl PiTranscriptAdapter {
    pub fn kind(&self) -> AdapterKind {
        AdapterKind::PiTranscript
    }
}
