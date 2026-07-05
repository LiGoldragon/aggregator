use crate::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggregatorClientCommand;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaAggregatorClientCommand;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigurationWriterCommand;

impl AggregatorClientCommand {
    pub fn from_environment() -> Self {
        Self
    }

    pub fn run(&self) -> Result<()> {
        Err(Error::TransportNotImplemented {
            binary: "aggregator",
        })
    }
}

impl MetaAggregatorClientCommand {
    pub fn from_environment() -> Self {
        Self
    }

    pub fn run(&self) -> Result<()> {
        Err(Error::TransportNotImplemented {
            binary: "meta-aggregator",
        })
    }
}

impl ConfigurationWriterCommand {
    pub fn from_environment() -> Self {
        Self
    }

    pub fn run(&self) -> Result<()> {
        Err(Error::ConfigurationStorageNotImplemented)
    }
}
