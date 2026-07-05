use signal_aggregator::{
    AggregatorOperationKind, AggregatorReply, ContractName, ContractVersion, EvidenceRejected,
    EvidenceRequest, RejectionReason, RequestIdentifier, TimeWindow, VersionReport,
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
                if range.start.as_str() > range.end.as_str() {
                    Some(RejectionReason::InvalidTimeWindow)
                } else {
                    None
                }
            }
            TimeWindow::Since(timestamp) => {
                if timestamp.as_str().is_empty() {
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
