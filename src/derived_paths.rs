use std::path::{Path, PathBuf};

use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HomeDirectory {
    path: PathBuf,
}

impl HomeDirectory {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if !path.is_absolute() {
            return Err(Error::argument(format!(
                "home directory must be absolute: {}",
                path.display()
            )));
        }
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspacePath {
    path: PathBuf,
}

impl WorkspacePath {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if !path.is_absolute() {
            return Err(Error::argument(format!(
                "workspace path must be absolute: {}",
                path.display()
            )));
        }
        Ok(Self { path })
    }

    pub fn normalized_text(&self) -> String {
        PathText::new(self.path.to_string_lossy().into_owned()).without_trailing_separators()
    }

    pub fn claude_project_component(&self) -> ClaudeProjectPathComponent {
        ClaudeProjectPathComponent::from_workspace(self)
    }

    pub fn pi_tintinweb_component(&self) -> PiTintinwebCwdComponent {
        PiTintinwebCwdComponent::from_workspace(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporaryDirectory {
    path: PathBuf,
}

impl TemporaryDirectory {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if !path.is_absolute() {
            return Err(Error::argument(format!(
                "temporary directory must be absolute: {}",
                path.display()
            )));
        }
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserIdentifier {
    value: u32,
}

impl UserIdentifier {
    pub fn new(value: u32) -> Self {
        Self { value }
    }

    pub fn value(&self) -> u32 {
        self.value
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeProjectPathComponent {
    text: String,
}

impl ClaudeProjectPathComponent {
    pub fn from_workspace(workspace: &WorkspacePath) -> Self {
        Self {
            text: workspace.normalized_text().replace('/', "-"),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeProjectTranscriptRoot {
    path: PathBuf,
}

impl ClaudeProjectTranscriptRoot {
    pub fn from_home_and_workspace(home: &HomeDirectory, workspace: &WorkspacePath) -> Self {
        Self {
            path: home
                .path()
                .join(".claude")
                .join("projects")
                .join(workspace.claude_project_component().as_str()),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeNativeSubagentOutputRoot {
    path: PathBuf,
}

impl ClaudeNativeSubagentOutputRoot {
    pub fn from_temporary_directory_and_user(
        temporary_directory: &TemporaryDirectory,
        user_identifier: UserIdentifier,
    ) -> Self {
        Self {
            path: temporary_directory
                .path()
                .join(format!("claude-{}", user_identifier.value())),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PiTintinwebCwdComponent {
    text: String,
}

impl PiTintinwebCwdComponent {
    pub fn from_workspace(workspace: &WorkspacePath) -> Self {
        let without_drive =
            DrivePrefixText::new(workspace.normalized_text()).without_drive_prefix();
        Self {
            text: without_drive
                .replace(['/', '\\'], "-")
                .trim_start_matches('-')
                .to_string(),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PiTintinwebSubagentOutputRoot {
    path: PathBuf,
}

impl PiTintinwebSubagentOutputRoot {
    pub fn from_temporary_directory_and_user(
        temporary_directory: &TemporaryDirectory,
        user_identifier: UserIdentifier,
    ) -> Self {
        Self {
            path: temporary_directory
                .path()
                .join(format!("pi-subagents-{}", user_identifier.value())),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PathText {
    text: String,
}

impl PathText {
    fn new(text: String) -> Self {
        Self { text }
    }

    fn without_trailing_separators(&self) -> String {
        let mut text = self.text.as_str();
        while text.len() > 1 && (text.ends_with('/') || text.ends_with('\\')) {
            text = &text[..text.len() - 1];
        }
        text.to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DrivePrefixText {
    text: String,
}

impl DrivePrefixText {
    fn new(text: String) -> Self {
        Self { text }
    }

    fn without_drive_prefix(&self) -> String {
        let bytes = self.text.as_bytes();
        if bytes.len() >= 2 && bytes[1] == b':' && bytes[0].is_ascii_alphabetic() {
            self.text[2..].to_string()
        } else {
            self.text.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claude_project_component_recreates_workspace_path_encoding() {
        assert_eq!(
            WorkspacePath::new("/home/li/primary")
                .expect("workspace")
                .claude_project_component()
                .as_str(),
            "-home-li-primary"
        );
        assert_eq!(
            WorkspacePath::new("/tmp")
                .expect("workspace")
                .claude_project_component()
                .as_str(),
            "-tmp"
        );
        assert_eq!(
            WorkspacePath::new("/home/li/Criopolis")
                .expect("workspace")
                .claude_project_component()
                .as_str(),
            "-home-li-Criopolis"
        );
        assert_eq!(
            WorkspacePath::new("/home/li/primary/")
                .expect("workspace")
                .claude_project_component()
                .as_str(),
            "-home-li-primary"
        );
    }

    #[test]
    fn claude_project_root_uses_home_directory_and_encoded_workspace() {
        let home = HomeDirectory::new("/fake/account").expect("home");
        let workspace = WorkspacePath::new("/workspace/main").expect("workspace");
        assert_eq!(
            ClaudeProjectTranscriptRoot::from_home_and_workspace(&home, &workspace).path(),
            Path::new("/fake/account/.claude/projects/-workspace-main")
        );
    }

    #[test]
    fn claude_subagent_root_uses_temporary_directory_and_user_identifier() {
        let temporary_directory = TemporaryDirectory::new("/scratch").expect("temporary directory");
        assert_eq!(
            ClaudeNativeSubagentOutputRoot::from_temporary_directory_and_user(
                &temporary_directory,
                UserIdentifier::new(42),
            )
            .path(),
            Path::new("/scratch/claude-42")
        );
    }

    #[test]
    fn pi_tintinweb_component_strips_leading_dash_after_separator_encoding() {
        assert_eq!(
            WorkspacePath::new("/home/li/primary")
                .expect("workspace")
                .pi_tintinweb_component()
                .as_str(),
            "home-li-primary"
        );
    }
}
