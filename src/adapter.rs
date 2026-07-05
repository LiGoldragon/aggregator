pub mod claude;
pub mod codex;
pub mod pi;
pub mod repository;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AdapterKind {
    ClaudeTranscript,
    CodexTranscript,
    PiTranscript,
    Repository,
}

impl AdapterKind {
    pub fn source_name(self) -> &'static str {
        match self {
            Self::ClaudeTranscript => "claude-transcript",
            Self::CodexTranscript => "codex-transcript",
            Self::PiTranscript => "pi-transcript",
            Self::Repository => "repository",
        }
    }
}
