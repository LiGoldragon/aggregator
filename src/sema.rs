use meta_signal_aggregator::{
    AggregatorConfiguration, ConfigurationCandidate, ConfigurationChange, ConfigurationConfigured,
    ConfigurationObservation, ConfigurationObserved, ConfigurationRejected,
    ConfigurationRejectionReason, ConfigurationValidated, ConfigurationValidationIssue,
    ConfigurationValidationIssueKind, ConfigurationValidationOutcome,
    ConfigurationValidationReport, OperationKind as MetaOperationKind, Response as MetaResponse,
};

use crate::{ConfigurationStore, RuntimeConfiguration};

#[derive(Debug, Clone, PartialEq)]
pub struct SemaPlane {
    configuration: Option<AggregatorConfiguration>,
    store: Option<ConfigurationStore>,
}

impl SemaPlane {
    pub fn empty() -> Self {
        Self {
            configuration: None,
            store: None,
        }
    }

    pub fn with_configuration(configuration: AggregatorConfiguration) -> Self {
        Self {
            configuration: Some(configuration),
            store: None,
        }
    }

    pub fn with_configuration_store(
        configuration: AggregatorConfiguration,
        store: ConfigurationStore,
    ) -> Self {
        Self {
            configuration: Some(configuration),
            store: Some(store),
        }
    }

    pub fn active_configuration(&self) -> Option<AggregatorConfiguration> {
        self.configuration.clone()
    }

    pub fn configure(&mut self, change: ConfigurationChange) -> MetaResponse {
        if !matches!(
            RuntimeConfiguration::validate_from_meta(&change.aggregator_configuration),
            crate::RuntimeConfigurationValidation::Accepted(_)
        ) {
            return Self::rejected(ConfigurationRejectionReason::InvalidConfiguration);
        }
        let Some(store) = &self.store else {
            return Self::rejected(ConfigurationRejectionReason::StoreUnavailable);
        };
        if store
            .write_configuration(&change.aggregator_configuration)
            .is_err()
        {
            return Self::rejected(ConfigurationRejectionReason::StoreUnavailable);
        }
        self.configuration = Some(change.aggregator_configuration.clone());
        MetaResponse::ConfigurationConfigured(ConfigurationConfigured {
            aggregator_configuration: change.aggregator_configuration,
        })
    }

    pub fn rejected(configuration_rejection_reason: ConfigurationRejectionReason) -> MetaResponse {
        MetaResponse::ConfigurationRejected(ConfigurationRejected {
            operation_kind: MetaOperationKind::Configure,
            configuration_rejection_reason,
        })
    }

    pub fn observe_configuration(&self) -> MetaResponse {
        let configuration_observation = match &self.configuration {
            Some(configuration) => ConfigurationObservation::Configured(configuration.clone()),
            None => ConfigurationObservation::NotConfigured,
        };
        MetaResponse::ConfigurationObserved(ConfigurationObserved {
            configuration_observation,
        })
    }

    pub fn validate_candidate(&self, candidate: ConfigurationCandidate) -> MetaResponse {
        MetaResponse::ConfigurationValidated(ConfigurationValidated {
            configuration_validation_outcome: RuntimeConfiguration::validate_from_meta(
                &candidate.aggregator_configuration,
            )
            .outcome(),
        })
    }

    pub fn validate_current_shape(&self) -> MetaResponse {
        let configuration_validation_outcome = match &self.configuration {
            Some(configuration) => {
                RuntimeConfiguration::validate_from_meta(configuration).outcome()
            }
            None => ConfigurationValidationOutcome::Rejected(ConfigurationValidationReport {
                configuration_validation_issues: vec![ConfigurationValidationIssue {
                    filesystem_path_option: None,
                    configuration_validation_issue_kind:
                        ConfigurationValidationIssueKind::MissingTranscriptSource,
                    validation_issue_detail_option: Some(String::from(
                        "configuration has not been provided",
                    )),
                }],
            }),
        };
        MetaResponse::ConfigurationValidated(ConfigurationValidated {
            configuration_validation_outcome,
        })
    }
}
