use signal_aggregator::Timestamp;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalTimestamp {
    timestamp: Timestamp,
    instant: OffsetDateTime,
}

impl CanonicalTimestamp {
    pub fn parse(timestamp: &Timestamp) -> Result<Self, TimestampParseError> {
        let instant = OffsetDateTime::parse(timestamp.as_str(), &Rfc3339)
            .map_err(|_| TimestampParseError::Malformed)?;
        let formatted = instant
            .format(&Rfc3339)
            .map_err(|_| TimestampParseError::Malformed)?;
        if formatted != timestamp.as_str() || !timestamp.as_str().ends_with('Z') {
            return Err(TimestampParseError::NonCanonical);
        }
        Ok(Self {
            timestamp: timestamp.clone(),
            instant,
        })
    }

    pub fn timestamp(&self) -> &Timestamp {
        &self.timestamp
    }

    pub fn instant(&self) -> OffsetDateTime {
        self.instant
    }

    pub fn is_before(&self, other: &Self) -> bool {
        self.instant < other.instant
    }

    pub fn is_after(&self, other: &Self) -> bool {
        self.instant > other.instant
    }

    pub fn is_at_or_after(&self, other: &Self) -> bool {
        self.instant >= other.instant
    }

    pub fn is_at_or_before(&self, other: &Self) -> bool {
        self.instant <= other.instant
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampParseError {
    Malformed,
    NonCanonical,
}
