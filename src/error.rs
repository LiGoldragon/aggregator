use crate::adapter::AdapterKind;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum IndexStoreError {
    #[error("unsupported index-store version {version}")]
    UnsupportedVersion { version: u32 },
    #[error("oversized index-store {kind}: {bytes} bytes exceeds {limit}")]
    OversizedEnvelope {
        kind: &'static str,
        bytes: u64,
        limit: u64,
    },
    #[error("oversized index-store record")]
    OversizedRecord,
    #[error("oversized index-store string")]
    OversizedString,
    #[error("invalid index-store checksum")]
    InvalidChecksum,
    #[error("corrupt index-store archive")]
    CorruptArchive,
    #[error("unexpected index-store file kind {actual}; expected {expected}")]
    UnexpectedFileKind { expected: u8, actual: u8 },
    #[error("fragile-reference collision")]
    ReferenceCollision,
    #[error("unsafe index-store path")]
    UnsafePath,
    #[error("index-store publication was interrupted")]
    InterruptedPublication,
    #[error("index-store writer conflict")]
    WriterConflict,
    #[error("index-store migration failed: {detail}")]
    MigrationFailure { detail: String },
    #[error("index-store serialization failed: {detail}")]
    Serialization { detail: String },
    #[error("index-store I/O {context} failed: {source}")]
    Io {
        context: &'static str,
        #[source]
        source: std::io::Error,
    },
}

impl IndexStoreError {
    pub fn io(context: &'static str, source: std::io::Error) -> Self {
        Self::Io { context, source }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{binary} transport is not implemented in the scaffold")]
    TransportNotImplemented { binary: &'static str },

    #[error("aggregator daemon runtime is not implemented in the scaffold")]
    DaemonRuntimeNotImplemented,

    #[error("configuration storage is not implemented in the scaffold")]
    ConfigurationStorageNotImplemented,

    #[error("configuration is unavailable")]
    ConfigurationUnavailable,

    #[error("{adapter:?} collection is not implemented in the scaffold")]
    CollectionNotImplemented { adapter: AdapterKind },

    #[error("argument error: {detail}")]
    Argument { detail: String },

    #[error("clock error: {detail}")]
    Clock { detail: String },

    #[error("NOTA {context} failed: {detail}")]
    Nota {
        context: &'static str,
        detail: String,
    },

    #[error("frame {context} failed: {source}")]
    Frame {
        context: &'static str,
        #[source]
        source: signal_frame::FrameError,
    },

    #[error("protocol {context} failed: {detail}")]
    Protocol {
        context: &'static str,
        detail: String,
    },

    #[error("startup configuration error: {detail}")]
    StartupConfiguration { detail: String },

    #[error("I/O {context} failed: {source}")]
    Io {
        context: &'static str,
        #[source]
        source: std::io::Error,
    },

    #[error("index store failed: {source}")]
    IndexStore {
        #[source]
        source: IndexStoreError,
    },
}

impl Error {
    pub fn argument(detail: impl Into<String>) -> Self {
        Self::Argument {
            detail: detail.into(),
        }
    }

    pub fn nota(context: &'static str, detail: impl Into<String>) -> Self {
        Self::Nota {
            context,
            detail: detail.into(),
        }
    }

    pub fn frame(context: &'static str, source: signal_frame::FrameError) -> Self {
        Self::Frame { context, source }
    }

    pub fn protocol(context: &'static str, detail: impl Into<String>) -> Self {
        Self::Protocol {
            context,
            detail: detail.into(),
        }
    }

    pub fn startup_configuration(detail: impl Into<String>) -> Self {
        Self::StartupConfiguration {
            detail: detail.into(),
        }
    }

    pub fn io(context: &'static str, source: std::io::Error) -> Self {
        Self::Io { context, source }
    }

    pub fn index_store(source: IndexStoreError) -> Self {
        Self::IndexStore { source }
    }
}
