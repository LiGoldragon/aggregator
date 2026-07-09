use signal_aggregator::{
    AggregatorOperationKind, AggregatorReply, ContractName, ContractVersion, EvidenceRejected,
    EvidenceRequest, OperationKind, OperationRejected, OperationRejectionReason,
    RejectedFragileReference, RejectionReason, RequestIdentifier, TimeWindow, VersionReport,
};

use crate::time_model::CanonicalTimestamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignalPlane;

impl SignalPlane {
    pub fn version_report(&self) -> AggregatorReply {
        AggregatorReply::VersionReported(VersionReport {
            contract_name: ContractName::new("signal-aggregator"),
            contract_version: ContractVersion::new("0.5.0"),
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

    pub fn reject_operation(
        &self,
        request_identifier: RequestIdentifier,
        operation: OperationKind,
        reason: OperationRejectionReason,
        reference: Option<RejectedFragileReference>,
    ) -> AggregatorReply {
        AggregatorReply::OperationRejected(OperationRejected {
            request_identifier,
            operation,
            reason,
            reference,
        })
    }

    pub fn collect_rejection(&self, request: &EvidenceRequest) -> Option<AggregatorReply> {
        self.validate_time_window(request)
            .or_else(|| self.validate_limits(request))
            .map(|reason| self.reject_collect(request.request_identifier.clone(), reason))
    }

    pub fn validate_time_window(&self, request: &EvidenceRequest) -> Option<RejectionReason> {
        match &request.time_window {
            TimeWindow::Recent(duration) => {
                if duration.amount.into_u64() == 0 {
                    Some(RejectionReason::InvalidTimeWindow)
                } else {
                    None
                }
            }
            TimeWindow::Range(range) => {
                let Ok(start) = CanonicalTimestamp::parse(&range.start) else {
                    return Some(RejectionReason::InvalidTimeWindow);
                };
                let Ok(end) = CanonicalTimestamp::parse(&range.end) else {
                    return Some(RejectionReason::InvalidTimeWindow);
                };
                if start.is_after(&end) {
                    Some(RejectionReason::InvalidTimeWindow)
                } else {
                    None
                }
            }
            TimeWindow::Since(timestamp) => {
                if CanonicalTimestamp::parse(timestamp).is_err() {
                    Some(RejectionReason::InvalidTimeWindow)
                } else {
                    None
                }
            }
        }
    }

    pub fn validate_limits(&self, request: &EvidenceRequest) -> Option<RejectionReason> {
        if request.limit_policy.maximum_segments.into_u64() == 0
            || request.limit_policy.maximum_bytes.into_u64() == 0
        {
            Some(RejectionReason::LimitExceeded)
        } else {
            None
        }
    }
}
