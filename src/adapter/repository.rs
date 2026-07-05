use signal_aggregator::{
    CommitIdentifier, FilesystemPath, ReadFailure, ReadFailureReason, RepositoryChange,
    RepositoryIdentifier, RepositoryPath, RepositoryWorktreeState, SourceIdentifier, SourceKind,
    Timestamp,
};

use crate::{AdapterKind, configuration::RepositoryAdapterConfiguration};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryAdapter {
    repositories: Vec<RepositoryAdapterConfiguration>,
    observation_mode: RepositoryObservationMode,
}

impl RepositoryAdapter {
    pub fn fixture(
        repositories: Vec<RepositoryAdapterConfiguration>,
        fixture: RepositoryEvidenceFixture,
    ) -> Self {
        Self {
            repositories,
            observation_mode: RepositoryObservationMode::Fixture(fixture),
        }
    }

    pub fn command_policy(
        repositories: Vec<RepositoryAdapterConfiguration>,
        policy: RepositoryCommandPolicy,
    ) -> Self {
        Self {
            repositories,
            observation_mode: RepositoryObservationMode::CommandPolicy(policy),
        }
    }

    pub fn kind(&self) -> AdapterKind {
        AdapterKind::Repository
    }

    pub fn collect(&self) -> RepositoryReadOutcome {
        RepositoryCollector::new(self.repositories.clone(), self.observation_mode.clone()).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepositoryObservationMode {
    Fixture(RepositoryEvidenceFixture),
    CommandPolicy(RepositoryCommandPolicy),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryCommandPolicy {
    authorization: RepositoryCommandAuthorization,
}

impl RepositoryCommandPolicy {
    pub fn unavailable() -> Self {
        Self {
            authorization: RepositoryCommandAuthorization::Unavailable,
        }
    }

    pub fn read_only_unimplemented() -> Self {
        Self {
            authorization: RepositoryCommandAuthorization::ReadOnlyUnimplemented,
        }
    }

    pub fn authorization(&self) -> RepositoryCommandAuthorization {
        self.authorization
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepositoryCommandAuthorization {
    Unavailable,
    ReadOnlyUnimplemented,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryEvidenceFixture {
    changes: Vec<RepositoryChangeFixture>,
}

impl RepositoryEvidenceFixture {
    pub fn new(changes: Vec<RepositoryChangeFixture>) -> Self {
        Self { changes }
    }

    pub fn changes(&self) -> &[RepositoryChangeFixture] {
        &self.changes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryChangeFixture {
    repository: RepositoryIdentifier,
    path: FilesystemPath,
    commit_identifier: Option<CommitIdentifier>,
    commit_timestamp: Option<Timestamp>,
    changed_paths: Vec<RepositoryPath>,
    worktree_state: RepositoryWorktreeState,
}

impl RepositoryChangeFixture {
    pub fn new(
        repository: RepositoryIdentifier,
        path: FilesystemPath,
        changed_paths: Vec<RepositoryPath>,
        worktree_state: RepositoryWorktreeState,
    ) -> Self {
        Self {
            repository,
            path,
            commit_identifier: None,
            commit_timestamp: None,
            changed_paths,
            worktree_state,
        }
    }

    pub fn with_commit(
        mut self,
        commit_identifier: CommitIdentifier,
        commit_timestamp: Timestamp,
    ) -> Self {
        self.commit_identifier = Some(commit_identifier);
        self.commit_timestamp = Some(commit_timestamp);
        self
    }

    pub fn into_change(self) -> RepositoryChange {
        RepositoryChange {
            repository: self.repository,
            path: self.path,
            commit_identifier: self.commit_identifier,
            commit_timestamp: self.commit_timestamp,
            changed_paths: self.changed_paths,
            worktree_state: self.worktree_state,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryReadOutcome {
    pub repository_changes: Vec<RepositoryChange>,
    pub read_failures: Vec<ReadFailure>,
}

impl RepositoryReadOutcome {
    pub fn empty() -> Self {
        Self {
            repository_changes: Vec::new(),
            read_failures: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryCollector {
    repositories: Vec<RepositoryAdapterConfiguration>,
    observation_mode: RepositoryObservationMode,
}

impl RepositoryCollector {
    pub fn new(
        repositories: Vec<RepositoryAdapterConfiguration>,
        observation_mode: RepositoryObservationMode,
    ) -> Self {
        Self {
            repositories,
            observation_mode,
        }
    }

    pub fn collect(&self) -> RepositoryReadOutcome {
        match &self.observation_mode {
            RepositoryObservationMode::Fixture(fixture) => self.collect_fixture(fixture),
            RepositoryObservationMode::CommandPolicy(policy) => self.collect_command_policy(policy),
        }
    }

    pub fn collect_fixture(&self, fixture: &RepositoryEvidenceFixture) -> RepositoryReadOutcome {
        let configured_names = self
            .repositories
            .iter()
            .map(RepositoryAdapterConfiguration::identifier)
            .collect::<Vec<_>>();
        let repository_changes = fixture
            .changes()
            .iter()
            .filter(|change| configured_names.contains(&change.repository))
            .cloned()
            .map(RepositoryChangeFixture::into_change)
            .collect();
        RepositoryReadOutcome {
            repository_changes,
            read_failures: Vec::new(),
        }
    }

    pub fn collect_command_policy(
        &self,
        policy: &RepositoryCommandPolicy,
    ) -> RepositoryReadOutcome {
        let read_failures = self
            .repositories
            .iter()
            .map(|repository| self.command_policy_failure(repository, policy.authorization()))
            .collect();
        RepositoryReadOutcome {
            repository_changes: Vec::new(),
            read_failures,
        }
    }

    pub fn command_policy_failure(
        &self,
        repository: &RepositoryAdapterConfiguration,
        authorization: RepositoryCommandAuthorization,
    ) -> ReadFailure {
        let reason = match authorization {
            RepositoryCommandAuthorization::Unavailable => ReadFailureReason::UnsupportedFormat,
            RepositoryCommandAuthorization::ReadOnlyUnimplemented => ReadFailureReason::IoFailure,
        };
        ReadFailure {
            source: SourceKind::Repository,
            path: Some(FilesystemPath::new(repository.path().display().to_string())),
            source_identifier: Some(SourceIdentifier::new(
                repository.identifier().as_str().to_string(),
            )),
            reason,
        }
    }
}
