use std::path::PathBuf;

use meta_signal_aggregator::{
    ActiveRepository, AggregatorConfiguration, FilesystemPath, RepositoryName, SocketMode,
    TranscriptFormat, TranscriptSource,
};
use signal_aggregator::{ByteLimit, LimitPolicy, Projection, SegmentLimit, SourceKind};

use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationStore {
    path: Option<PathBuf>,
}

impl ConfigurationStore {
    pub fn in_memory() -> Self {
        Self { path: None }
    }

    pub fn at_path(path: impl Into<PathBuf>) -> Self {
        Self {
            path: Some(path.into()),
        }
    }

    pub fn configured_path(&self) -> Option<&PathBuf> {
        self.path.as_ref()
    }

    pub fn read_configuration(&self) -> Result<AggregatorConfiguration> {
        Err(Error::ConfigurationStorageNotImplemented)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigurationFixture;

impl ConfigurationFixture {
    pub fn minimal() -> AggregatorConfiguration {
        AggregatorConfiguration {
            ordinary_socket_path: FilesystemPath::new("/run/aggregator/aggregator.sock"),
            ordinary_socket_mode: SocketMode::new(0o660),
            meta_socket_path: FilesystemPath::new("/run/aggregator/aggregator-meta.sock"),
            meta_socket_mode: SocketMode::new(0o600),
            store_path: FilesystemPath::new("/var/lib/aggregator/aggregator.sema"),
            active_repositories: vec![ActiveRepository {
                name: RepositoryName::new("primary"),
                path: FilesystemPath::new("/home/li/primary"),
            }],
            transcript_sources: vec![TranscriptSource {
                source_kind: SourceKind::Claude,
                path: FilesystemPath::new("/home/li/.claude/projects"),
                format: TranscriptFormat::ClaudeJsonl,
            }],
            default_projection: Projection::MetadataOnly,
            default_limit_policy: LimitPolicy {
                maximum_segments: SegmentLimit::new(32),
                maximum_bytes: ByteLimit::new(4096),
            },
        }
    }
}
