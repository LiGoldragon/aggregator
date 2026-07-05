use crate::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggregatorDaemonCommand;

impl AggregatorDaemonCommand {
    pub fn from_environment() -> Self {
        Self
    }

    pub fn run(&self) -> Result<()> {
        Err(Error::DaemonRuntimeNotImplemented)
    }
}
