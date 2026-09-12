use signal_aggregator::{
    EvidenceRejected, EvidenceRequest, OperationKind, OperationRejected, OperationRejectionReason,
    RejectedFragileReference, RejectionReason, RequestIdentifier, Response, TimeWindow,
    VersionReport,
};

use crate::time_model::CanonicalTimestamp;

/// The name and version this runtime reports for the contract it speaks.
pub const CONTRACT_NAME: &str = "signal-aggregator";
pub const CONTRACT_VERSION: &str = "0.7.0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignalPlane;

impl SignalPlane {
    pub fn version_report(&self) -> Response {
        Response::VersionReported(VersionReport {
            contract_name: String::from(CONTRACT_NAME),
            contract_version: String::from(CONTRACT_VERSION),
        })
    }

    pub fn reject_collect(
        &self,
        request_identifier: RequestIdentifier,
        rejection_reason: RejectionReason,
    ) -> Response {
        Response::EvidenceRejected(EvidenceRejected {
            request_identifier,
            operation_kind: OperationKind::Collect,
            rejection_reason,
        })
    }

    pub fn reject_operation(
        &self,
        request_identifier: RequestIdentifier,
        operation_kind: OperationKind,
        operation_rejection_reason: OperationRejectionReason,
        rejected_fragile_reference_option: Option<RejectedFragileReference>,
    ) -> Response {
        Response::OperationRejected(OperationRejected {
            request_identifier,
            operation_kind,
            operation_rejection_reason,
            rejected_fragile_reference_option,
        })
    }

    pub fn collect_rejection(&self, request: &EvidenceRequest) -> Option<Response> {
        self.validate_time_window(request)
            .or_else(|| self.validate_limits(request))
            .map(|reason| self.reject_collect(request.request_identifier.clone(), reason))
    }

    pub fn validate_time_window(&self, request: &EvidenceRequest) -> Option<RejectionReason> {
        match &request.time_window {
            TimeWindow::Recent(duration) => {
                if duration.duration_amount <= 0 {
                    Some(RejectionReason::InvalidTimeWindow)
                } else {
                    None
                }
            }
            TimeWindow::Range(range) => {
                let Ok(start) = CanonicalTimestamp::parse(&range.start_timestamp) else {
                    return Some(RejectionReason::InvalidTimeWindow);
                };
                let Ok(end) = CanonicalTimestamp::parse(&range.end_timestamp) else {
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
        if request.limit_policy.maximum_segments <= 0 || request.limit_policy.maximum_bytes <= 0 {
            Some(RejectionReason::LimitExceeded)
        } else {
            None
        }
    }
}
