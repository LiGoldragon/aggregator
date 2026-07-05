use signal_aggregator::{EvidencePackage, EvidenceRequest};

use crate::{AdapterKind, Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NexusPlane {
    adapters: Vec<AdapterKind>,
}

impl NexusPlane {
    pub fn with_adapters(adapters: Vec<AdapterKind>) -> Self {
        Self { adapters }
    }

    pub fn adapter_count(&self) -> usize {
        self.adapters.len()
    }

    pub fn collect(&self, request: EvidenceRequest) -> Result<EvidencePackage> {
        let adapter = self
            .adapters
            .first()
            .copied()
            .unwrap_or(AdapterKind::Repository);
        let _request_identifier = request.request_identifier;
        Err(Error::CollectionNotImplemented { adapter })
    }
}
