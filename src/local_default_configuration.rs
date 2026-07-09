use std::path::{Path, PathBuf};

use meta_signal_aggregator::{
    ActiveRepository, AggregatorConfiguration, FilesystemPath, OutputInterfaceConfiguration,
    SocketMode, TranscriptRoot, TranscriptSource,
};
use signal_aggregator::{ByteLimit, LimitPolicy, Projection, SegmentLimit};

use crate::{
    ClaudeNativeSubagentOutputRoot, ClaudeProjectTranscriptRoot, HomeDirectory,
    PiTintinwebSubagentOutputRoot, TemporaryDirectory, UserIdentifier, WorkspacePath,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalDefaultConfigurationRequest {
    home_directory: HomeDirectory,
    workspaces: Vec<WorkspacePath>,
    temporary_directory: TemporaryDirectory,
    user_identifier: UserIdentifier,
    runtime_directory: PathBuf,
    store_path: PathBuf,
}

impl LocalDefaultConfigurationRequest {
    pub fn new(
        home_directory: HomeDirectory,
        workspaces: Vec<WorkspacePath>,
        temporary_directory: TemporaryDirectory,
        user_identifier: UserIdentifier,
        runtime_directory: PathBuf,
        store_path: PathBuf,
    ) -> Self {
        Self {
            home_directory,
            workspaces,
            temporary_directory,
            user_identifier,
            runtime_directory,
            store_path,
        }
    }

    pub fn configuration(&self) -> AggregatorConfiguration {
        AggregatorConfiguration {
            ordinary_socket_path: FilesystemPath::new(
                self.runtime_directory
                    .join("aggregator.sock")
                    .display()
                    .to_string(),
            ),
            ordinary_socket_mode: SocketMode::new(0o600),
            meta_socket_path: FilesystemPath::new(
                self.runtime_directory
                    .join("aggregator-meta.sock")
                    .display()
                    .to_string(),
            ),
            meta_socket_mode: SocketMode::new(0o600),
            store_path: FilesystemPath::new(self.store_path.display().to_string()),
            active_repositories: Vec::<ActiveRepository>::new(),
            transcript_sources: self.transcript_sources(),
            default_projection: Projection::MetadataOnly,
            default_limit_policy: LimitPolicy {
                maximum_segments: SegmentLimit::new(32),
                maximum_bytes: ByteLimit::new(4096),
            },
            output_interfaces: OutputInterfaceConfiguration::default(),
        }
    }

    pub fn transcript_root_paths(&self) -> Vec<PathBuf> {
        self.claude_project_roots()
            .into_iter()
            .map(|root| root.path().to_path_buf())
            .chain(std::iter::once(
                ClaudeNativeSubagentOutputRoot::from_temporary_directory_and_user(
                    &self.temporary_directory,
                    self.user_identifier,
                )
                .path()
                .to_path_buf(),
            ))
            .chain(std::iter::once(
                PiTintinwebSubagentOutputRoot::from_temporary_directory_and_user(
                    &self.temporary_directory,
                    self.user_identifier,
                )
                .path()
                .to_path_buf(),
            ))
            .collect()
    }

    pub fn runtime_directory(&self) -> &Path {
        &self.runtime_directory
    }

    pub fn store_parent_directory(&self) -> Option<&Path> {
        self.store_path.parent()
    }

    fn transcript_sources(&self) -> Vec<TranscriptSource> {
        self.claude_project_roots()
            .into_iter()
            .map(|root| {
                TranscriptSource::Claude(TranscriptRoot {
                    path: FilesystemPath::new(root.path().display().to_string()),
                })
            })
            .chain(std::iter::once(TranscriptSource::ClaudeSubagentOutput(
                TranscriptRoot {
                    path: FilesystemPath::new(
                        ClaudeNativeSubagentOutputRoot::from_temporary_directory_and_user(
                            &self.temporary_directory,
                            self.user_identifier,
                        )
                        .path()
                        .display()
                        .to_string(),
                    ),
                },
            )))
            .chain(std::iter::once(TranscriptSource::PiSubagentOutput(
                TranscriptRoot {
                    path: FilesystemPath::new(
                        PiTintinwebSubagentOutputRoot::from_temporary_directory_and_user(
                            &self.temporary_directory,
                            self.user_identifier,
                        )
                        .path()
                        .display()
                        .to_string(),
                    ),
                },
            )))
            .collect()
    }

    fn claude_project_roots(&self) -> Vec<ClaudeProjectTranscriptRoot> {
        self.workspaces
            .iter()
            .map(|workspace| {
                ClaudeProjectTranscriptRoot::from_home_and_workspace(
                    &self.home_directory,
                    workspace,
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nota::NotaEncode;

    #[test]
    fn local_default_configuration_derives_roots_from_fake_context() {
        let request = LocalDefaultConfigurationRequest::new(
            HomeDirectory::new("/fake/home").expect("home"),
            vec![WorkspacePath::new("/fake/workspace").expect("workspace")],
            TemporaryDirectory::new("/fake/tmp").expect("temporary directory"),
            UserIdentifier::new(123),
            PathBuf::from("/fake/run/aggregator"),
            PathBuf::from("/fake/state/aggregator.sema"),
        );

        let text = request.configuration().to_nota();
        assert!(text.contains("/.claude/projects/-fake-workspace"));
        assert!(text.contains("/fake/tmp/claude-123"));
        assert!(text.contains("/fake/tmp/pi-subagents-123"));
        assert!(!text.contains("-home-li-primary"));
        assert!(!text.contains("/tmp/claude-1001"));
    }
}
