//! Aggregator runtime scaffold.
//!
//! The component collects and normalizes evidence. It does not synthesize,
//! review, recommend, score, or judge collected work.

pub mod adapter;
pub mod client;
pub mod clock;
pub mod configuration;
pub mod daemon;
pub mod derived_paths;
pub mod error;
pub mod local_default_configuration;
pub mod nexus;
pub mod output_index;
pub mod sema;
pub mod signal;
pub mod time_model;

pub use adapter::AdapterKind;
pub use client::{
    AggregatorClientCommand, ConfigurationWriterCommand, MetaAggregatorClientCommand,
};
pub use clock::{CollectionClock, ReferenceTime};
pub use configuration::{
    ConfigurationFixture, ConfigurationStore, LegacyRecoveryKind, RepositoryAdapterConfiguration,
    RuntimeConfiguration, RuntimeConfigurationValidation, RuntimeLegacyRecoveryRoot,
    RuntimeOutputInterfaceConfiguration, RuntimeSourceSelection, RuntimeStorePath,
    TranscriptAdapterConfiguration, TranscriptRootConfiguration,
};
pub use daemon::AggregatorDaemonCommand;
pub use derived_paths::{
    ClaudeNativeSubagentOutputRoot, ClaudeProjectPathComponent, ClaudeProjectTranscriptRoot,
    HomeDirectory, PiTintinwebCwdComponent, PiTintinwebSubagentOutputRoot, TemporaryDirectory,
    UserIdentifier, WorkspacePath,
};
pub use error::{Error, Result};
pub use local_default_configuration::LocalDefaultConfigurationRequest;
pub use nexus::NexusPlane;
pub use output_index::{FragileIndexStore, OutputInterfaceRuntime};
pub use sema::SemaPlane;
pub use signal::SignalPlane;
