//! Aggregator runtime scaffold.
//!
//! The component collects and normalizes evidence. It does not synthesize,
//! review, recommend, score, or judge collected work.

pub mod adapter;
pub mod client;
pub mod configuration;
pub mod daemon;
pub mod error;
pub mod nexus;
pub mod sema;
pub mod signal;

pub use adapter::AdapterKind;
pub use client::{
    AggregatorClientCommand, ConfigurationWriterCommand, MetaAggregatorClientCommand,
};
pub use configuration::{
    ConfigurationFixture, ConfigurationStore, RepositoryAdapterConfiguration, RuntimeConfiguration,
    RuntimeConfigurationValidation, RuntimeSourceSelection, TranscriptAdapterConfiguration,
    TranscriptRootConfiguration,
};
pub use daemon::AggregatorDaemonCommand;
pub use error::{Error, Result};
pub use nexus::NexusPlane;
pub use sema::SemaPlane;
pub use signal::SignalPlane;
