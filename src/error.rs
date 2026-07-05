use crate::adapter::AdapterKind;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{binary} transport is not implemented in the scaffold")]
    TransportNotImplemented { binary: &'static str },

    #[error("aggregator daemon runtime is not implemented in the scaffold")]
    DaemonRuntimeNotImplemented,

    #[error("configuration storage is not implemented in the scaffold")]
    ConfigurationStorageNotImplemented,

    #[error("{adapter:?} collection is not implemented in the scaffold")]
    CollectionNotImplemented { adapter: AdapterKind },

    #[error("argument error: {detail}")]
    Argument { detail: String },
}
