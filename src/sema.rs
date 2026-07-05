use meta_signal_aggregator::{
    AggregatorConfiguration, ConfigurationChange, ConfigurationConfigured,
    ConfigurationObservation, ConfigurationObserved, ConfigurationValidated,
    ConfigurationValidationOutcome, MetaAggregatorReply,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemaPlane {
    configuration: Option<AggregatorConfiguration>,
}

impl SemaPlane {
    pub fn empty() -> Self {
        Self {
            configuration: None,
        }
    }

    pub fn with_configuration(configuration: AggregatorConfiguration) -> Self {
        Self {
            configuration: Some(configuration),
        }
    }

    pub fn configure(&mut self, change: ConfigurationChange) -> MetaAggregatorReply {
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

    pub fn validate_current_shape(&self) -> MetaAggregatorReply {
        MetaAggregatorReply::ConfigurationValidated(ConfigurationValidated {
            outcome: ConfigurationValidationOutcome::Accepted,
        })
    }
}
