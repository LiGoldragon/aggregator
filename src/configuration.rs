use std::path::{Path, PathBuf};

use meta_signal_aggregator::{
    ActiveRepository, AggregatorConfiguration, ConfigurationValidationIssue,
    ConfigurationValidationIssueKind, ConfigurationValidationOutcome,
    ConfigurationValidationReport, DefaultingPolicy, FilesystemPath, LegacyRecoveryRoot,
    LegacyRecoverySource, OutputInterfaceConfiguration, OutputInterfaceLimitPolicy, RepositoryName,
    SocketMode, TranscriptRoot, TranscriptSource,
};
use signal_aggregator::{
    LimitPolicy, Projection, RepositoryIdentifier, SelectedSources, SourceKind, SourceSelection,
};

use crate::{Error, Result, adapter::TranscriptScanLimits, wire::DatomText};

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
        DatomText::read("configuration", &text)
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
        std::fs::write(&temporary_path, DatomText::print(configuration))
            .map_err(|error| Error::io("writing temporary configuration", error))?;
        std::fs::rename(&temporary_path, path)
            .map_err(|error| Error::io("committing configuration", error))
    }

    pub fn temporary_path(&self, path: &Path) -> PathBuf {
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("configuration.datom");
        path.with_file_name(format!(".{file_name}.{}.tmp", std::process::id()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigurationFixture;

impl ConfigurationFixture {
    pub fn minimal() -> AggregatorConfiguration {
        AggregatorConfiguration {
            ordinary_socket_path: String::from("/run/aggregator/aggregator.sock"),
            ordinary_socket_mode: 0o660,
            meta_socket_path: String::from("/run/aggregator/aggregator-meta.sock"),
            meta_socket_mode: 0o600,
            store_path: String::from("/var/lib/aggregator/aggregator.sema"),
            active_repositories: vec![ActiveRepository {
                repository_name: String::from("example-repository"),
                filesystem_path: String::from("/srv/aggregator/repositories/example"),
            }],
            transcript_sources: vec![TranscriptSource::Claude(TranscriptRoot {
                filesystem_path: String::from("/srv/aggregator/transcripts/claude"),
            })],
            default_projection: Projection::MetadataOnly,
            default_limit_policy: LimitPolicy {
                maximum_segments: 32,
                maximum_bytes: 4096,
            },
            output_interface_configuration: OutputInterfaceConfiguration::default_policy(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
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

    pub fn archive_root_path(&self) -> PathBuf {
        self.store_path
            .parent()
            .map(|parent| parent.join("session-archive"))
            .unwrap_or_else(|| PathBuf::from("session-archive"))
    }

    pub fn select_sources(&self, selection: &SourceSelection) -> RuntimeSourceSelection {
        RuntimeSourceSelector::new(self).select(selection)
    }
}

#[derive(Debug, Clone, PartialEq)]
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
    PiSubagentOutput(TranscriptRootConfiguration),
}

impl TranscriptAdapterConfiguration {
    pub fn kind(&self) -> SourceKind {
        match self {
            Self::Claude(_) => SourceKind::Claude,
            Self::ClaudeSubagentOutput(_) => SourceKind::ClaudeSubagentOutput,
            Self::Codex(_) => SourceKind::Codex,
            Self::Pi(_) => SourceKind::Pi,
            Self::PiSubagentOutput(_) => SourceKind::PiSubagentOutput,
        }
    }

    pub fn root(&self) -> &TranscriptRootConfiguration {
        match self {
            Self::Claude(root)
            | Self::ClaudeSubagentOutput(root)
            | Self::Codex(root)
            | Self::Pi(root)
            | Self::PiSubagentOutput(root) => root,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptRootConfiguration {
    path: PathBuf,
    scan_limits: TranscriptScanLimits,
}

impl TranscriptRootConfiguration {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            scan_limits: TranscriptScanLimits::default_runtime(),
        }
    }

    pub fn with_scan_limits(mut self, scan_limits: TranscriptScanLimits) -> Self {
        self.scan_limits = scan_limits;
        self
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn scan_limits(&self) -> &TranscriptScanLimits {
        &self.scan_limits
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
        self.name.as_str().to_string()
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

#[derive(Debug, Clone, PartialEq)]
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
            SourceSelection::Only(SelectedSources { source_kinds }) => {
                self.select_only(source_kinds)
            }
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

#[derive(Debug, Clone, PartialEq)]
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
        self.validate_output_interface_limits(
            &self
                .configuration
                .output_interface_configuration
                .output_interface_limit_policy,
        );
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
                    self.configuration
                        .output_interface_configuration
                        .output_interface_limit_policy
                        .clone(),
                    legacy_recovery_roots,
                ),
            })
        } else {
            RuntimeConfigurationValidation::Rejected(ConfigurationValidationReport {
                configuration_validation_issues: self.issues,
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
            if mode > 0o777 {
                self.issues
                    .push(ConfigurationIssue::invalid_socket_mode(path.clone(), mode));
            }
        }
    }

    pub fn validate_output_interface_limits(&mut self, limits: &OutputInterfaceLimitPolicy) {
        // The contract defaults are the runtime's absolute practical ceilings. Configuration may
        // tighten a workload, but it cannot turn an input-controlled limit into an unbounded
        // allocation, scan, or reply.
        let ceiling = OutputInterfaceLimitPolicy::default_policy();
        for (name, value, maximum) in [
            (
                "maximum_page_items",
                limits.maximum_page_items,
                ceiling.maximum_page_items,
            ),
            (
                "maximum_preview_bytes",
                limits.maximum_preview_bytes,
                ceiling.maximum_preview_bytes,
            ),
            (
                "maximum_read_bytes",
                limits.maximum_read_bytes,
                ceiling.maximum_read_bytes,
            ),
            (
                "maximum_recovery_files_per_root",
                limits.maximum_recovery_files_per_root,
                ceiling.maximum_recovery_files_per_root,
            ),
            (
                "maximum_transcript_scan_entries",
                limits.maximum_transcript_scan_entries,
                ceiling.maximum_transcript_scan_entries,
            ),
            (
                "maximum_transcript_discovered_files",
                limits.maximum_transcript_discovered_files,
                ceiling.maximum_transcript_discovered_files,
            ),
            (
                "maximum_transcript_file_bytes",
                limits.maximum_transcript_file_bytes,
                ceiling.maximum_transcript_file_bytes,
            ),
            (
                "maximum_transcript_line_bytes",
                limits.maximum_transcript_line_bytes,
                ceiling.maximum_transcript_line_bytes,
            ),
            (
                "maximum_transcript_read_failures",
                limits.maximum_transcript_read_failures,
                ceiling.maximum_transcript_read_failures,
            ),
        ] {
            if value == 0 || value > maximum {
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
        let typed_data_path = PathBuf::from(format!("{}.d", index_path.display()));
        for candidate in [&index_path, &typed_data_path] {
            for repository in repositories {
                if RuntimePathBoundary::new(repository.path().to_path_buf()).contains(candidate) {
                    self.issues
                        .push(ConfigurationIssue::invalid_fragile_index_configuration(
                            self.configuration.store_path.clone(),
                            "fragile index storage must not live under an active repository",
                        ));
                }
            }
            for root in legacy_recovery_roots {
                if RuntimePathBoundary::new(root.path().to_path_buf()).contains(candidate) {
                    self.issues
                        .push(ConfigurationIssue::invalid_fragile_index_configuration(
                            self.configuration.store_path.clone(),
                            "fragile index storage must not live under a legacy recovery root",
                        ));
                }
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
            TranscriptSource::PiSubagentOutput(root) => self
                .transcript_root(root)
                .map(TranscriptAdapterConfiguration::PiSubagentOutput),
        }
    }

    pub fn transcript_root(
        &mut self,
        root: &TranscriptRoot,
    ) -> Option<TranscriptRootConfiguration> {
        let path = PathBuf::from(root.filesystem_path.as_str());
        if path.is_dir() {
            Some(
                TranscriptRootConfiguration::new(path).with_scan_limits(
                    TranscriptScanLimits::from_output_interface_limits(
                        &self
                            .configuration
                            .output_interface_configuration
                            .output_interface_limit_policy,
                    ),
                ),
            )
        } else {
            self.issues.push(ConfigurationIssue::unreadable_path(
                root.filesystem_path.clone(),
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
        let path = PathBuf::from(repository.filesystem_path.as_str());
        if path.is_dir() {
            Some(RepositoryAdapterConfiguration::new(
                repository.repository_name.clone(),
                path,
            ))
        } else {
            self.issues.push(ConfigurationIssue::unreadable_path(
                repository.filesystem_path.clone(),
                "repository root must exist and be a directory",
            ));
            None
        }
    }

    pub fn legacy_recovery_roots(&mut self) -> Vec<RuntimeLegacyRecoveryRoot> {
        self.configuration
            .output_interface_configuration
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
        let path = PathBuf::from(root.filesystem_path.as_str());
        if path.is_dir() {
            Some(RuntimeLegacyRecoveryRoot::new(kind, path))
        } else {
            self.issues
                .push(ConfigurationIssue::invalid_legacy_recovery_root(
                    root.filesystem_path.clone(),
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
            filesystem_path_option: None,
            configuration_validation_issue_kind:
                ConfigurationValidationIssueKind::MissingTranscriptSource,
            validation_issue_detail_option: Some(String::from(
                "no readable transcript source configured",
            )),
        }
    }

    pub fn missing_repository() -> ConfigurationValidationIssue {
        ConfigurationValidationIssue {
            filesystem_path_option: None,
            configuration_validation_issue_kind:
                ConfigurationValidationIssueKind::MissingRepository,
            validation_issue_detail_option: Some(String::from(
                "no readable active repository configured",
            )),
        }
    }

    pub fn unreadable_path(
        path: FilesystemPath,
        detail: impl Into<String>,
    ) -> ConfigurationValidationIssue {
        ConfigurationValidationIssue {
            filesystem_path_option: Some(path),
            configuration_validation_issue_kind: ConfigurationValidationIssueKind::UnreadablePath,
            validation_issue_detail_option: Some(detail.into()),
        }
    }

    pub fn invalid_socket_mode(
        path: FilesystemPath,
        mode: SocketMode,
    ) -> ConfigurationValidationIssue {
        ConfigurationValidationIssue {
            filesystem_path_option: Some(path),
            configuration_validation_issue_kind:
                ConfigurationValidationIssueKind::InvalidSocketMode,
            validation_issue_detail_option: Some(format!(
                "socket mode {:#o} is outside permission bits",
                mode
            )),
        }
    }

    pub fn invalid_output_interface_limit(name: &'static str) -> ConfigurationValidationIssue {
        ConfigurationValidationIssue {
            filesystem_path_option: None,
            configuration_validation_issue_kind:
                ConfigurationValidationIssueKind::InvalidOutputInterfaceLimit,
            validation_issue_detail_option: Some(format!("{name} must be greater than zero")),
        }
    }

    pub fn invalid_legacy_recovery_root(
        path: FilesystemPath,
        detail: impl Into<String>,
    ) -> ConfigurationValidationIssue {
        ConfigurationValidationIssue {
            filesystem_path_option: Some(path),
            configuration_validation_issue_kind:
                ConfigurationValidationIssueKind::InvalidLegacyRecoveryRoot,
            validation_issue_detail_option: Some(detail.into()),
        }
    }

    pub fn invalid_fragile_index_configuration(
        path: FilesystemPath,
        detail: impl Into<String>,
    ) -> ConfigurationValidationIssue {
        ConfigurationValidationIssue {
            filesystem_path_option: Some(path),
            configuration_validation_issue_kind:
                ConfigurationValidationIssueKind::InvalidFragileIndexConfiguration,
            validation_issue_detail_option: Some(detail.into()),
        }
    }

    pub fn unwritable_fragile_index_storage(
        path: FilesystemPath,
        detail: impl Into<String>,
    ) -> ConfigurationValidationIssue {
        ConfigurationValidationIssue {
            filesystem_path_option: Some(path),
            configuration_validation_issue_kind:
                ConfigurationValidationIssueKind::UnwritableFragileIndexStorage,
            validation_issue_detail_option: Some(detail.into()),
        }
    }
}
