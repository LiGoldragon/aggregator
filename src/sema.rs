use meta_signal_aggregator::{
    AggregatorConfiguration, ConfigurationCandidate, ConfigurationChange, ConfigurationConfigured,
    ConfigurationObservation, ConfigurationObserved, ConfigurationRejected,
    ConfigurationRejectionReason, ConfigurationValidated, ConfigurationValidationIssue,
    ConfigurationValidationIssueKind, ConfigurationValidationOutcome,
    ConfigurationValidationReport, MetaAggregatorOperationKind, MetaAggregatorReply,
    ValidationIssueDetail,
};

use crate::{ConfigurationStore, RuntimeConfiguration};

#[derive(Debug, Clone, PartialEq, Eq)]
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

    pub fn configure(&mut self, change: ConfigurationChange) -> MetaAggregatorReply {
        if !matches!(
            RuntimeConfiguration::validate_from_meta(&change.configuration),
            crate::RuntimeConfigurationValidation::Accepted(_)
        ) {
            return MetaAggregatorReply::ConfigurationRejected(ConfigurationRejected {
                operation: MetaAggregatorOperationKind::Configure,
                reason: ConfigurationRejectionReason::InvalidConfiguration,
            });
        }
        let Some(store) = &self.store else {
            return MetaAggregatorReply::ConfigurationRejected(ConfigurationRejected {
                operation: MetaAggregatorOperationKind::Configure,
                reason: ConfigurationRejectionReason::StoreUnavailable,
            });
        };
        if store.write_configuration(&change.configuration).is_err() {
            return MetaAggregatorReply::ConfigurationRejected(ConfigurationRejected {
                operation: MetaAggregatorOperationKind::Configure,
                reason: ConfigurationRejectionReason::StoreUnavailable,
            });
        }
        self.configuration = Some(change.configuration.clone());
        MetaAggregatorReply::ConfigurationConfigured(ConfigurationConfigured {
            configuration: change.configuration,
        })
    }

    pub fn observe_configuration(&self) -> MetaAggregatorReply {
        let observation = match &self.configuration {
            Some(configuration) => ConfigurationObservation::Configured(configuration.clone()),
            None => ConfigurationObservation::NotConfigured,
        };
        MetaAggregatorReply::ConfigurationObserved(ConfigurationObserved { observation })
    }

    pub fn validate_candidate(&self, candidate: ConfigurationCandidate) -> MetaAggregatorReply {
        MetaAggregatorReply::ConfigurationValidated(ConfigurationValidated {
            outcome: RuntimeConfiguration::validate_from_meta(&candidate.configuration).outcome(),
        })
    }

    pub fn validate_current_shape(&self) -> MetaAggregatorReply {
        let outcome = match &self.configuration {
            Some(configuration) => {
                RuntimeConfiguration::validate_from_meta(configuration).outcome()
            }
            None => ConfigurationValidationOutcome::Rejected(ConfigurationValidationReport {
                issues: vec![ConfigurationValidationIssue {
                    path: None,
                    kind: ConfigurationValidationIssueKind::MissingTranscriptSource,
                    detail: Some(ValidationIssueDetail::new(
                        "configuration has not been provided",
                    )),
                }],
            }),
        };
        MetaAggregatorReply::ConfigurationValidated(ConfigurationValidated { outcome })
    }
}
