use signal_aggregator::{
    AggregatorOperationKind, AggregatorReply, ContractName, ContractVersion, EvidenceRejected,
    RejectionReason, RequestIdentifier, VersionReport,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignalPlane;

impl SignalPlane {
    pub fn version_report(&self) -> AggregatorReply {
        AggregatorReply::VersionReported(VersionReport {
            contract_name: ContractName::new("signal-aggregator"),
            contract_version: ContractVersion::new("0.1.0"),
        })
    }

    pub fn reject_collect(
        &self,
        request_identifier: RequestIdentifier,
        reason: RejectionReason,
    ) -> AggregatorReply {
        AggregatorReply::EvidenceRejected(EvidenceRejected {
            request_identifier,
            operation: AggregatorOperationKind::Collect,
            reason,
        })
    }
}
