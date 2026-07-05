use signal_aggregator::{
    EvidencePackage, EvidenceRequest, PackageIdentifier, SourceKind, TimeWindow,
};

use crate::{
    AdapterKind, CollectionClock, Error, Result, RuntimeConfiguration,
    adapter::{
        TranscriptReadOutcome, TranscriptReadRequest,
        claude::ClaudeTranscriptAdapter,
        codex::CodexTranscriptAdapter,
        pi::PiTranscriptAdapter,
        repository::{
            RepositoryAdapter, RepositoryCommandPolicy, RepositoryObservationMode,
            RepositoryReadOutcome,
        },
    },
    configuration::TranscriptAdapterConfiguration,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NexusPlane {
    adapters: Vec<AdapterKind>,
    runtime_configuration: Option<RuntimeConfiguration>,
    clock: CollectionClock,
    repository_observation_mode: RepositoryObservationMode,
}

impl NexusPlane {
    pub fn with_adapters(adapters: Vec<AdapterKind>) -> Self {
        Self {
            adapters,
            runtime_configuration: None,
            clock: CollectionClock::system(),
            repository_observation_mode: RepositoryObservationMode::CommandPolicy(
                RepositoryCommandPolicy::read_only_unimplemented(),
            ),
        }
    }

    pub fn with_runtime_configuration(
        runtime_configuration: RuntimeConfiguration,
        clock: CollectionClock,
    ) -> Self {
        Self {
            adapters: Vec::new(),
            runtime_configuration: Some(runtime_configuration),
            clock,
            repository_observation_mode: RepositoryObservationMode::CommandPolicy(
                RepositoryCommandPolicy::read_only_unimplemented(),
            ),
        }
    }

    pub fn with_repository_observation_mode(mut self, mode: RepositoryObservationMode) -> Self {
        self.repository_observation_mode = mode;
        self
    }

    pub fn adapter_count(&self) -> usize {
        self.runtime_configuration
            .as_ref()
            .map_or(self.adapters.len(), |configuration| {
                configuration.transcript_sources().len()
                    + usize::from(!configuration.repositories().is_empty())
            })
    }

    pub fn collect(&self, request: EvidenceRequest) -> Result<EvidencePackage> {
        let Some(configuration) = &self.runtime_configuration else {
            let adapter = self
                .adapters
                .first()
                .copied()
                .unwrap_or(AdapterKind::Repository);
            return Err(Error::CollectionNotImplemented { adapter });
        };
        let lowered_time_window = self.clock.lower_time_window(&request.time_window)?;
        let selection = configuration.select_sources(&request.source_selection);
        let transcript_request = TranscriptReadRequest::new(
            lowered_time_window,
            request.projection.clone(),
            request.limit_policy.clone(),
        );
        let mut package_builder = EvidencePackageBuilder::new(&request, &self.clock);
        for source in selection.transcript_sources {
            package_builder
                .merge_transcript(self.collect_transcript_source(source, &transcript_request));
        }
        if !selection.repositories.is_empty() {
            package_builder
                .merge_repository(self.collect_repository_sources(selection.repositories));
        }
        Ok(package_builder.finish())
    }

    pub fn repository_command_policy(&self) -> RepositoryCommandPolicy {
        match &self.repository_observation_mode {
            RepositoryObservationMode::CommandPolicy(policy) => policy.clone(),
            RepositoryObservationMode::Fixture(_) => {
                RepositoryCommandPolicy::read_only_unimplemented()
            }
        }
    }

    pub fn collect_repository_sources(
        &self,
        repositories: Vec<crate::RepositoryAdapterConfiguration>,
    ) -> RepositoryReadOutcome {
        match &self.repository_observation_mode {
            RepositoryObservationMode::CommandPolicy(policy) => {
                RepositoryAdapter::command_policy(repositories, policy.clone()).collect()
            }
            RepositoryObservationMode::Fixture(fixture) => {
                RepositoryAdapter::fixture(repositories, fixture.clone()).collect()
            }
        }
    }

    pub fn collect_transcript_source(
        &self,
        source: TranscriptAdapterConfiguration,
        request: &TranscriptReadRequest,
    ) -> TranscriptReadOutcome {
        match source {
            TranscriptAdapterConfiguration::Claude(root) => {
                ClaudeTranscriptAdapter::new(root).collect(request)
            }
            TranscriptAdapterConfiguration::Codex(root) => {
                CodexTranscriptAdapter::new(root).collect(request)
            }
            TranscriptAdapterConfiguration::Pi(root) => {
                PiTranscriptAdapter::new(root).collect(request)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidencePackageBuilder<'a> {
    request: &'a EvidenceRequest,
    clock: &'a CollectionClock,
    source_volumes: Vec<signal_aggregator::SourceVolume>,
    transcript_segments: Vec<signal_aggregator::TranscriptSegment>,
    repository_changes: Vec<signal_aggregator::RepositoryChange>,
    truncations: Vec<signal_aggregator::Truncation>,
    read_failures: Vec<signal_aggregator::ReadFailure>,
}

impl<'a> EvidencePackageBuilder<'a> {
    pub fn new(request: &'a EvidenceRequest, clock: &'a CollectionClock) -> Self {
        Self {
            request,
            clock,
            source_volumes: Vec::new(),
            transcript_segments: Vec::new(),
            repository_changes: Vec::new(),
            truncations: Vec::new(),
            read_failures: Vec::new(),
        }
    }

    pub fn merge_transcript(&mut self, outcome: TranscriptReadOutcome) {
        self.source_volumes.extend(outcome.source_volumes);
        self.transcript_segments.extend(outcome.transcript_segments);
        self.truncations.extend(outcome.truncations);
        self.read_failures.extend(outcome.read_failures);
    }

    pub fn merge_repository(&mut self, outcome: RepositoryReadOutcome) {
        self.repository_changes.extend(outcome.repository_changes);
        self.read_failures.extend(outcome.read_failures);
    }

    pub fn finish(self) -> EvidencePackage {
        EvidencePackage {
            package_identifier: PackageIdentifier::new(format!(
                "package-{}",
                self.request.request_identifier.as_str()
            )),
            request_identifier: self.request.request_identifier.clone(),
            time_window: self.request.time_window.clone(),
            collected_at: self.clock.reference_timestamp(),
            source_volumes: self.source_volumes,
            transcript_segments: self.transcript_segments,
            repository_changes: self.repository_changes,
            truncations: self.truncations,
            read_failures: self.read_failures,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoweredTimeWindow {
    original: TimeWindow,
    adapter_window: TimeWindow,
}

impl LoweredTimeWindow {
    pub fn new(original: TimeWindow, adapter_window: TimeWindow) -> Self {
        Self {
            original,
            adapter_window,
        }
    }

    pub fn original(&self) -> &TimeWindow {
        &self.original
    }

    pub fn adapter_window(&self) -> &TimeWindow {
        &self.adapter_window
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceKindContactPoint;

impl SourceKindContactPoint {
    pub fn adapter_kind(source: SourceKind) -> Option<AdapterKind> {
        match source {
            SourceKind::Claude => Some(AdapterKind::ClaudeTranscript),
            SourceKind::Codex => Some(AdapterKind::CodexTranscript),
            SourceKind::Pi => Some(AdapterKind::PiTranscript),
            SourceKind::Repository => Some(AdapterKind::Repository),
        }
    }
}
