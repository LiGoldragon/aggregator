use std::path::{Path, PathBuf};

use meta_signal_aggregator::{
    ActiveRepository, AggregatorConfiguration, ConfigurationValidationIssue,
    ConfigurationValidationIssueKind, ConfigurationValidationOutcome,
    ConfigurationValidationReport, FilesystemPath, LegacyRecoveryRoot, LegacyRecoverySource,
    OutputInterfaceConfiguration, OutputInterfaceLimitPolicy, RepositoryName, SocketMode,
    TranscriptRoot, TranscriptSource, ValidationIssueDetail,
};
use nota::{NotaDecode, NotaEncode, NotaSource};
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
        let path = self
            .path
            .as_ref()
            .ok_or(Error::ConfigurationStorageNotImplemented)?;
        let text = std::fs::read_to_string(path)
            .map_err(|error| Error::io("reading configuration", error))?;
        match NotaSource::new(&text).parse::<AggregatorConfiguration>() {
            Ok(configuration) => Ok(configuration),
            Err(current_error) => NotaSource::new(&text)
                .parse::<LegacyAggregatorConfiguration>()
                .map(LegacyAggregatorConfiguration::into_current)
                .map_err(|legacy_error| {
                    Error::nota(
                        "configuration decode",
                        format!(
                            "current shape failed: {current_error}; legacy 0.1 migration failed: {legacy_error}"
                        ),
                    )
                }),
        }
    }

    pub fn write_configuration(&self, configuration: &AggregatorConfiguration) -> Result<()> {
        let path = self
            .path
            .as_ref()
            .ok_or(Error::ConfigurationStorageNotImplemented)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| Error::io("creating configuration directory", error))?;
        }
        let temporary_path = self.temporary_path(path);
        std::fs::write(&temporary_path, configuration.to_nota())
            .map_err(|error| Error::io("writing temporary configuration", error))?;
        std::fs::rename(&temporary_path, path)
            .map_err(|error| Error::io("committing configuration", error))
    }

    pub fn temporary_path(&self, path: &Path) -> PathBuf {
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("configuration.nota");
        path.with_file_name(format!(".{file_name}.{}.tmp", std::process::id()))
    }
}

#[derive(NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq)]
pub struct LegacyAggregatorConfiguration {
    pub ordinary_socket_path: FilesystemPath,
    pub ordinary_socket_mode: SocketMode,
    pub meta_socket_path: FilesystemPath,
    pub meta_socket_mode: SocketMode,
    pub store_path: FilesystemPath,
    pub active_repositories: Vec<ActiveRepository>,
    pub transcript_sources: Vec<TranscriptSource>,
    pub default_projection: Projection,
    pub default_limit_policy: LimitPolicy,
}

impl LegacyAggregatorConfiguration {
    pub fn into_current(self) -> AggregatorConfiguration {
        AggregatorConfiguration {
            ordinary_socket_path: self.ordinary_socket_path,
            ordinary_socket_mode: self.ordinary_socket_mode,
            meta_socket_path: self.meta_socket_path,
            meta_socket_mode: self.meta_socket_mode,
            store_path: self.store_path,
            active_repositories: self.active_repositories,
            transcript_sources: self.transcript_sources,
            default_projection: self.default_projection,
            default_limit_policy: self.default_limit_policy,
            output_interfaces: OutputInterfaceConfiguration::default(),
        }
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
                name: RepositoryName::new("example-repository"),
                path: FilesystemPath::new("/srv/aggregator/repositories/example"),
            }],
            transcript_sources: vec![TranscriptSource::Claude(TranscriptRoot {
                path: FilesystemPath::new("/srv/aggregator/transcripts/claude"),
            })],
            default_projection: Projection::MetadataOnly,
            default_limit_policy: LimitPolicy {
                maximum_segments: SegmentLimit::new(32),
                maximum_bytes: ByteLimit::new(4096),
            },
            output_interfaces: OutputInterfaceConfiguration::default(),
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
    store_path: PathBuf,
    transcript_sources: Vec<TranscriptAdapterConfiguration>,
    repositories: Vec<RepositoryAdapterConfiguration>,
    default_projection: Projection,
    default_limit_policy: LimitPolicy,
    output_interfaces: RuntimeOutputInterfaceConfiguration,
}

impl RuntimeConfiguration {
    pub fn validate_from_meta(
        configuration: &AggregatorConfiguration,
    ) -> RuntimeConfigurationValidation {
        RuntimeConfigurationValidator::new(configuration).validate()
    }

    pub fn store_path(&self) -> &Path {
        &self.store_path
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

    pub fn output_interfaces(&self) -> &RuntimeOutputInterfaceConfiguration {
        &self.output_interfaces
    }

    pub fn select_sources(&self, selection: &SourceSelection) -> RuntimeSourceSelection {
        RuntimeSourceSelector::new(self).select(selection)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeOutputInterfaceConfiguration {
    limits: OutputInterfaceLimitPolicy,
    legacy_recovery_roots: Vec<RuntimeLegacyRecoveryRoot>,
}

impl RuntimeOutputInterfaceConfiguration {
    pub fn new(
        limits: OutputInterfaceLimitPolicy,
        legacy_recovery_roots: Vec<RuntimeLegacyRecoveryRoot>,
    ) -> Self {
        Self {
            limits,
            legacy_recovery_roots,
        }
    }

    pub fn limits(&self) -> &OutputInterfaceLimitPolicy {
        &self.limits
    }

    pub fn legacy_recovery_roots(&self) -> &[RuntimeLegacyRecoveryRoot] {
        &self.legacy_recovery_roots
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeLegacyRecoveryRoot {
    kind: LegacyRecoveryKind,
    path: PathBuf,
}

impl RuntimeLegacyRecoveryRoot {
    pub fn new(kind: LegacyRecoveryKind, path: PathBuf) -> Self {
        Self { kind, path }
    }

    pub fn kind(&self) -> LegacyRecoveryKind {
        self.kind
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyRecoveryKind {
    LegacyReports,
    LegacyAgentOutputs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TranscriptAdapterConfiguration {
    Claude(TranscriptRootConfiguration),
    ClaudeSubagentOutput(TranscriptRootConfiguration),
    Codex(TranscriptRootConfiguration),
    Pi(TranscriptRootConfiguration),
}

impl TranscriptAdapterConfiguration {
    pub fn kind(&self) -> SourceKind {
        match self {
            Self::Claude(_) => SourceKind::Claude,
            Self::ClaudeSubagentOutput(_) => SourceKind::ClaudeSubagentOutput,
            Self::Codex(_) => SourceKind::Codex,
            Self::Pi(_) => SourceKind::Pi,
        }
    }

    pub fn root(&self) -> &TranscriptRootConfiguration {
        match self {
            Self::Claude(root)
            | Self::ClaudeSubagentOutput(root)
            | Self::Codex(root)
            | Self::Pi(root) => root,
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
        self.validate_output_interface_limits(&self.configuration.output_interfaces.limits);
        self.validate_fragile_index_storage_parent();
        let transcript_sources = self.transcript_sources();
        let repositories = self.repositories();
        let legacy_recovery_roots = self.legacy_recovery_roots();
        self.validate_fragile_index_location(&repositories, &legacy_recovery_roots);
        if transcript_sources.is_empty() {
            self.issues
                .push(ConfigurationIssue::missing_transcript_source());
        }
        if self.issues.is_empty() {
            RuntimeConfigurationValidation::Accepted(RuntimeConfiguration {
                store_path: PathBuf::from(self.configuration.store_path.as_str()),
                transcript_sources,
                repositories,
                default_projection: self.configuration.default_projection.clone(),
                default_limit_policy: self.configuration.default_limit_policy.clone(),
                output_interfaces: RuntimeOutputInterfaceConfiguration::new(
                    self.configuration.output_interfaces.limits.clone(),
                    legacy_recovery_roots,
                ),
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

    pub fn validate_output_interface_limits(&mut self, limits: &OutputInterfaceLimitPolicy) {
        for (name, accepted) in [
            (
                "maximum_page_items",
                limits.maximum_page_items.into_u64() > 0,
            ),
            (
                "maximum_preview_bytes",
                limits.maximum_preview_bytes.into_u64() > 0,
            ),
            (
                "maximum_read_bytes",
                limits.maximum_read_bytes.into_u64() > 0,
            ),
            (
                "maximum_recovery_files_per_root",
                limits.maximum_recovery_files_per_root.into_u64() > 0,
            ),
        ] {
            if !accepted {
                self.issues
                    .push(ConfigurationIssue::invalid_output_interface_limit(name));
            }
        }
    }

    pub fn validate_fragile_index_storage_parent(&mut self) {
        let index_path =
            RuntimeStorePath::new(PathBuf::from(self.configuration.store_path.as_str()))
                .fragile_index_path();
        if let Some(parent) = index_path.parent()
            && parent.exists()
            && !parent.is_dir()
        {
            self.issues
                .push(ConfigurationIssue::unwritable_fragile_index_storage(
                    self.configuration.store_path.clone(),
                    "fragile index parent exists and is not a directory",
                ));
        }
    }

    pub fn validate_fragile_index_location(
        &mut self,
        repositories: &[RepositoryAdapterConfiguration],
        legacy_recovery_roots: &[RuntimeLegacyRecoveryRoot],
    ) {
        let index_path =
            RuntimeStorePath::new(PathBuf::from(self.configuration.store_path.as_str()))
                .fragile_index_path();
        for repository in repositories {
            if RuntimePathBoundary::new(repository.path().to_path_buf()).contains(&index_path) {
                self.issues
                    .push(ConfigurationIssue::invalid_fragile_index_configuration(
                        self.configuration.store_path.clone(),
                        "fragile index storage must not live under an active repository",
                    ));
            }
        }
        for root in legacy_recovery_roots {
            if RuntimePathBoundary::new(root.path().to_path_buf()).contains(&index_path) {
                self.issues
                    .push(ConfigurationIssue::invalid_fragile_index_configuration(
                        self.configuration.store_path.clone(),
                        "fragile index storage must not live under a legacy recovery root",
                    ));
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
            TranscriptSource::ClaudeSubagentOutput(root) => self
                .transcript_root(root)
                .map(TranscriptAdapterConfiguration::ClaudeSubagentOutput),
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

    pub fn legacy_recovery_roots(&mut self) -> Vec<RuntimeLegacyRecoveryRoot> {
        self.configuration
            .output_interfaces
            .legacy_recovery_sources
            .iter()
            .filter_map(|source| self.legacy_recovery_source(source))
            .collect()
    }

    pub fn legacy_recovery_source(
        &mut self,
        source: &LegacyRecoverySource,
    ) -> Option<RuntimeLegacyRecoveryRoot> {
        match source {
            LegacyRecoverySource::LegacyReports(root) => {
                self.legacy_recovery_root(root, LegacyRecoveryKind::LegacyReports)
            }
            LegacyRecoverySource::LegacyAgentOutputs(root) => {
                self.legacy_recovery_root(root, LegacyRecoveryKind::LegacyAgentOutputs)
            }
        }
    }

    pub fn legacy_recovery_root(
        &mut self,
        root: &LegacyRecoveryRoot,
        kind: LegacyRecoveryKind,
    ) -> Option<RuntimeLegacyRecoveryRoot> {
        let path = PathBuf::from(root.path.as_str());
        if path.is_dir() {
            Some(RuntimeLegacyRecoveryRoot::new(kind, path))
        } else {
            self.issues
                .push(ConfigurationIssue::invalid_legacy_recovery_root(
                    root.path.clone(),
                    "legacy recovery root must exist and be a directory",
                ));
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeStorePath {
    path: PathBuf,
}

impl RuntimeStorePath {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn fragile_index_path(&self) -> PathBuf {
        PathBuf::from(format!("{}.output-index.json", self.path.display()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePathBoundary {
    root: PathBuf,
}

impl RuntimePathBoundary {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn contains(&self, candidate: &Path) -> bool {
        let root = self
            .root
            .canonicalize()
            .unwrap_or_else(|_| self.root.clone());
        let candidate = RuntimeCandidatePath::new(candidate.to_path_buf()).canonical_or_original();
        candidate.starts_with(root)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeCandidatePath {
    path: PathBuf,
}

impl RuntimeCandidatePath {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn canonical_or_original(&self) -> PathBuf {
        if let Ok(path) = self.path.canonicalize() {
            return path;
        }
        let Some(parent) = self.path.parent() else {
            return self.path.clone();
        };
        match parent.canonicalize() {
            Ok(parent) => self
                .path
                .file_name()
                .map(|name| parent.join(name))
                .unwrap_or(parent),
            Err(_) => self.path.clone(),
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

    pub fn invalid_output_interface_limit(name: &'static str) -> ConfigurationValidationIssue {
        ConfigurationValidationIssue {
            path: None,
            kind: ConfigurationValidationIssueKind::InvalidOutputInterfaceLimit,
            detail: Some(ValidationIssueDetail::new(format!(
                "{name} must be greater than zero"
            ))),
        }
    }

    pub fn invalid_legacy_recovery_root(
        path: FilesystemPath,
        detail: impl Into<String>,
    ) -> ConfigurationValidationIssue {
        ConfigurationValidationIssue {
            path: Some(path),
            kind: ConfigurationValidationIssueKind::InvalidLegacyRecoveryRoot,
            detail: Some(ValidationIssueDetail::new(detail.into())),
        }
    }

    pub fn invalid_fragile_index_configuration(
        path: FilesystemPath,
        detail: impl Into<String>,
    ) -> ConfigurationValidationIssue {
        ConfigurationValidationIssue {
            path: Some(path),
            kind: ConfigurationValidationIssueKind::InvalidFragileIndexConfiguration,
            detail: Some(ValidationIssueDetail::new(detail.into())),
        }
    }

    pub fn unwritable_fragile_index_storage(
        path: FilesystemPath,
        detail: impl Into<String>,
    ) -> ConfigurationValidationIssue {
        ConfigurationValidationIssue {
            path: Some(path),
            kind: ConfigurationValidationIssueKind::UnwritableFragileIndexStorage,
            detail: Some(ValidationIssueDetail::new(detail.into())),
        }
    }
}
