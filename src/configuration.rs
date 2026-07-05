use std::path::{Path, PathBuf};

use meta_signal_aggregator::{
    ActiveRepository, AggregatorConfiguration, ConfigurationValidationIssue,
    ConfigurationValidationIssueKind, ConfigurationValidationOutcome,
    ConfigurationValidationReport, FilesystemPath, RepositoryName, SocketMode, TranscriptRoot,
    TranscriptSource, ValidationIssueDetail,
};
use signal_aggregator::{
    ByteLimit, LimitPolicy, Projection, RepositoryIdentifier, SegmentLimit, SelectedSources,
    SourceKind, SourceSelection,
};

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
            transcript_sources: vec![TranscriptSource::Claude(TranscriptRoot {
                path: FilesystemPath::new("/home/li/.claude/projects"),
            })],
            default_projection: Projection::MetadataOnly,
            default_limit_policy: LimitPolicy {
                maximum_segments: SegmentLimit::new(32),
                maximum_bytes: ByteLimit::new(4096),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeConfigurationValidation {
    Accepted(RuntimeConfiguration),
    Rejected(ConfigurationValidationReport),
}

impl RuntimeConfigurationValidation {
    pub fn outcome(&self) -> ConfigurationValidationOutcome {
        match self {
            Self::Accepted(_) => ConfigurationValidationOutcome::Accepted,
            Self::Rejected(report) => ConfigurationValidationOutcome::Rejected(report.clone()),
        }
    }

    pub fn accepted_configuration(&self) -> Option<&RuntimeConfiguration> {
        match self {
            Self::Accepted(configuration) => Some(configuration),
            Self::Rejected(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfiguration {
    transcript_sources: Vec<TranscriptAdapterConfiguration>,
    repositories: Vec<RepositoryAdapterConfiguration>,
    default_projection: Projection,
    default_limit_policy: LimitPolicy,
}

impl RuntimeConfiguration {
    pub fn validate_from_meta(
        configuration: &AggregatorConfiguration,
    ) -> RuntimeConfigurationValidation {
        RuntimeConfigurationValidator::new(configuration).validate()
    }

    pub fn transcript_sources(&self) -> &[TranscriptAdapterConfiguration] {
        &self.transcript_sources
    }

    pub fn repositories(&self) -> &[RepositoryAdapterConfiguration] {
        &self.repositories
    }

    pub fn default_projection(&self) -> &Projection {
        &self.default_projection
    }

    pub fn default_limit_policy(&self) -> &LimitPolicy {
        &self.default_limit_policy
    }

    pub fn select_sources(&self, selection: &SourceSelection) -> RuntimeSourceSelection {
        RuntimeSourceSelector::new(self).select(selection)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TranscriptAdapterConfiguration {
    Claude(TranscriptRootConfiguration),
    Codex(TranscriptRootConfiguration),
    Pi(TranscriptRootConfiguration),
}

impl TranscriptAdapterConfiguration {
    pub fn kind(&self) -> SourceKind {
        match self {
            Self::Claude(_) => SourceKind::Claude,
            Self::Codex(_) => SourceKind::Codex,
            Self::Pi(_) => SourceKind::Pi,
        }
    }

    pub fn root(&self) -> &TranscriptRootConfiguration {
        match self {
            Self::Claude(root) | Self::Codex(root) | Self::Pi(root) => root,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptRootConfiguration {
    path: PathBuf,
}

impl TranscriptRootConfiguration {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryAdapterConfiguration {
    name: RepositoryName,
    path: PathBuf,
}

impl RepositoryAdapterConfiguration {
    pub fn new(name: RepositoryName, path: PathBuf) -> Self {
        Self { name, path }
    }

    pub fn name(&self) -> &RepositoryName {
        &self.name
    }

    pub fn identifier(&self) -> RepositoryIdentifier {
        RepositoryIdentifier::new(self.name.as_str().to_string())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSourceSelection {
    pub transcript_sources: Vec<TranscriptAdapterConfiguration>,
    pub repositories: Vec<RepositoryAdapterConfiguration>,
}

impl RuntimeSourceSelection {
    pub fn empty() -> Self {
        Self {
            transcript_sources: Vec::new(),
            repositories: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSourceSelector<'a> {
    configuration: &'a RuntimeConfiguration,
}

impl<'a> RuntimeSourceSelector<'a> {
    pub fn new(configuration: &'a RuntimeConfiguration) -> Self {
        Self { configuration }
    }

    pub fn select(&self, selection: &SourceSelection) -> RuntimeSourceSelection {
        match selection {
            SourceSelection::AllConfigured => RuntimeSourceSelection {
                transcript_sources: self.configuration.transcript_sources.clone(),
                repositories: self.configuration.repositories.clone(),
            },
            SourceSelection::Only(SelectedSources { sources }) => self.select_only(sources),
        }
    }

    pub fn select_only(&self, sources: &[SourceKind]) -> RuntimeSourceSelection {
        RuntimeSourceSelection {
            transcript_sources: self
                .configuration
                .transcript_sources
                .iter()
                .filter(|source| sources.contains(&source.kind()))
                .cloned()
                .collect(),
            repositories: if sources.contains(&SourceKind::Repository) {
                self.configuration.repositories.clone()
            } else {
                Vec::new()
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfigurationValidator<'a> {
    configuration: &'a AggregatorConfiguration,
    issues: Vec<ConfigurationValidationIssue>,
}

impl<'a> RuntimeConfigurationValidator<'a> {
    pub fn new(configuration: &'a AggregatorConfiguration) -> Self {
        Self {
            configuration,
            issues: Vec::new(),
        }
    }

    pub fn validate(mut self) -> RuntimeConfigurationValidation {
        self.validate_socket_modes();
        let transcript_sources = self.transcript_sources();
        let repositories = self.repositories();
        if transcript_sources.is_empty() {
            self.issues
                .push(ConfigurationIssue::missing_transcript_source());
        }
        if repositories.is_empty() {
            self.issues.push(ConfigurationIssue::missing_repository());
        }
        if self.issues.is_empty() {
            RuntimeConfigurationValidation::Accepted(RuntimeConfiguration {
                transcript_sources,
                repositories,
                default_projection: self.configuration.default_projection.clone(),
                default_limit_policy: self.configuration.default_limit_policy.clone(),
            })
        } else {
            RuntimeConfigurationValidation::Rejected(ConfigurationValidationReport {
                issues: self.issues,
            })
        }
    }

    pub fn validate_socket_modes(&mut self) {
        for (path, mode) in [
            (
                &self.configuration.ordinary_socket_path,
                self.configuration.ordinary_socket_mode,
            ),
            (
                &self.configuration.meta_socket_path,
                self.configuration.meta_socket_mode,
            ),
        ] {
            if mode.into_u32() > 0o777 {
                self.issues
                    .push(ConfigurationIssue::invalid_socket_mode(path.clone(), mode));
            }
        }
    }

    pub fn transcript_sources(&mut self) -> Vec<TranscriptAdapterConfiguration> {
        self.configuration
            .transcript_sources
            .iter()
            .filter_map(|source| self.transcript_source(source))
            .collect()
    }

    pub fn transcript_source(
        &mut self,
        source: &TranscriptSource,
    ) -> Option<TranscriptAdapterConfiguration> {
        match source {
            TranscriptSource::Claude(root) => self
                .transcript_root(root)
                .map(TranscriptAdapterConfiguration::Claude),
            TranscriptSource::Codex(root) => self
                .transcript_root(root)
                .map(TranscriptAdapterConfiguration::Codex),
            TranscriptSource::Pi(root) => self
                .transcript_root(root)
                .map(TranscriptAdapterConfiguration::Pi),
        }
    }

    pub fn transcript_root(
        &mut self,
        root: &TranscriptRoot,
    ) -> Option<TranscriptRootConfiguration> {
        let path = PathBuf::from(root.path.as_str());
        if path.is_dir() {
            Some(TranscriptRootConfiguration::new(path))
        } else {
            self.issues.push(ConfigurationIssue::unreadable_path(
                root.path.clone(),
                "transcript root must exist and be a directory",
            ));
            None
        }
    }

    pub fn repositories(&mut self) -> Vec<RepositoryAdapterConfiguration> {
        self.configuration
            .active_repositories
            .iter()
            .filter_map(|repository| self.repository(repository))
            .collect()
    }

    pub fn repository(
        &mut self,
        repository: &ActiveRepository,
    ) -> Option<RepositoryAdapterConfiguration> {
        let path = PathBuf::from(repository.path.as_str());
        if path.is_dir() {
            Some(RepositoryAdapterConfiguration::new(
                repository.name.clone(),
                path,
            ))
        } else {
            self.issues.push(ConfigurationIssue::unreadable_path(
                repository.path.clone(),
                "repository root must exist and be a directory",
            ));
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationIssue;

impl ConfigurationIssue {
    pub fn missing_transcript_source() -> ConfigurationValidationIssue {
        ConfigurationValidationIssue {
            path: None,
            kind: ConfigurationValidationIssueKind::MissingTranscriptSource,
            detail: Some(ValidationIssueDetail::new(
                "no readable transcript source configured",
            )),
        }
    }

    pub fn missing_repository() -> ConfigurationValidationIssue {
        ConfigurationValidationIssue {
            path: None,
            kind: ConfigurationValidationIssueKind::MissingRepository,
            detail: Some(ValidationIssueDetail::new(
                "no readable active repository configured",
            )),
        }
    }

    pub fn unreadable_path(
        path: FilesystemPath,
        detail: impl Into<String>,
    ) -> ConfigurationValidationIssue {
        ConfigurationValidationIssue {
            path: Some(path),
            kind: ConfigurationValidationIssueKind::UnreadablePath,
            detail: Some(ValidationIssueDetail::new(detail.into())),
        }
    }

    pub fn invalid_socket_mode(
        path: FilesystemPath,
        mode: SocketMode,
    ) -> ConfigurationValidationIssue {
        ConfigurationValidationIssue {
            path: Some(path),
            kind: ConfigurationValidationIssueKind::InvalidSocketMode,
            detail: Some(ValidationIssueDetail::new(format!(
                "socket mode {:#o} is outside permission bits",
                mode.into_u32()
            ))),
        }
    }
}
