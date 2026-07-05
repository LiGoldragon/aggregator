use signal_aggregator::{RelativeDuration, TimeRange, TimeWindow, Timestamp};
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{Error, Result, time_model::CanonicalTimestamp};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionClock {
    reference_time: ReferenceTime,
}

impl CollectionClock {
    pub fn system() -> Self {
        Self {
            reference_time: ReferenceTime::now(),
        }
    }

    pub fn fixed(reference_time: ReferenceTime) -> Self {
        Self { reference_time }
    }

    pub fn from_environment() -> Result<Self> {
        match std::env::var("AGGREGATOR_REFERENCE_TIMESTAMP") {
            Ok(value) => Ok(Self::fixed(ReferenceTime::from_timestamp(Timestamp::new(
                value,
            ))?)),
            Err(std::env::VarError::NotPresent) => Ok(Self::system()),
            Err(error) => Err(Error::argument(format!(
                "AGGREGATOR_REFERENCE_TIMESTAMP is not readable: {error}"
            ))),
        }
    }

    pub fn reference_timestamp(&self) -> Timestamp {
        self.reference_time.timestamp()
    }

    pub fn lower_time_window(&self, time_window: &TimeWindow) -> Result<TimeWindow> {
        self.reference_time.lower(time_window)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceTime {
    instant: OffsetDateTime,
}

impl ReferenceTime {
    pub fn now() -> Self {
        Self {
            instant: OffsetDateTime::now_utc(),
        }
    }

    pub fn from_timestamp(timestamp: Timestamp) -> Result<Self> {
        let instant = CanonicalTimestamp::parse(&timestamp)
            .map_err(|error| Error::Clock {
                detail: format!(
                    "invalid reference timestamp {}: {error:?}",
                    timestamp.as_str()
                ),
            })?
            .instant();
        Ok(Self { instant })
    }

    pub fn timestamp(&self) -> Timestamp {
        Timestamp::new(
            self.instant
                .format(&Rfc3339)
                .expect("RFC3339 formatting for OffsetDateTime should be infallible"),
        )
    }

    pub fn lower(&self, time_window: &TimeWindow) -> Result<TimeWindow> {
        match time_window {
            TimeWindow::Recent(duration) => RelativeTimeWindow::new(duration.clone()).lower(self),
            TimeWindow::Range(range) => Ok(TimeWindow::Range(range.clone())),
            TimeWindow::Since(timestamp) => Ok(TimeWindow::Since(timestamp.clone())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelativeTimeWindow {
    duration: RelativeDuration,
}

impl RelativeTimeWindow {
    pub fn new(duration: RelativeDuration) -> Self {
        Self { duration }
    }

    pub fn lower(&self, reference_time: &ReferenceTime) -> Result<TimeWindow> {
        let absolute_duration = AbsoluteDuration::from_relative(&self.duration)?;
        let start = reference_time.instant - absolute_duration.duration;
        Ok(TimeWindow::Range(TimeRange {
            start: Timestamp::new(start.format(&Rfc3339).map_err(|error| Error::Clock {
                detail: format!("failed to format lowered start timestamp: {error}"),
            })?),
            end: reference_time.timestamp(),
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbsoluteDuration {
    duration: Duration,
}

impl AbsoluteDuration {
    pub fn from_relative(duration: &RelativeDuration) -> Result<Self> {
        let amount = duration.amount.into_u64();
        let amount = i64::try_from(amount).map_err(|_| Error::Clock {
            detail: "relative duration amount is too large".to_string(),
        })?;
        let duration = match duration.unit {
            signal_aggregator::DurationUnit::Minutes => Duration::minutes(amount),
            signal_aggregator::DurationUnit::Hours => Duration::hours(amount),
            signal_aggregator::DurationUnit::Days => Duration::days(amount),
        };
        Ok(Self { duration })
    }
}
