use std::fmt;

use chrono::{DateTime, ParseError};
use s3s::dto::Timestamp;
use time::error::ComponentRange;

#[derive(Debug)]
pub enum Rfc2822Error {
    ParseError(ParseError),
    ComponentRange(ComponentRange),
}

impl fmt::Display for Rfc2822Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ParseError(e) => write!(f, "RFC2822 parse error: {}", e),
            Self::ComponentRange(e) => write!(f, "RFC2822 component range error: {}", e),
        }
    }
}

impl From<ParseError> for Rfc2822Error {
    fn from(e: ParseError) -> Self {
        Self::ParseError(e)
    }
}

impl From<ComponentRange> for Rfc2822Error {
    fn from(e: ComponentRange) -> Self {
        Self::ComponentRange(e)
    }
}

impl std::error::Error for Rfc2822Error {}

pub trait FromRfc2822: Sized {
    fn from_rfc2822(s: &str) -> Result<Self, Rfc2822Error>;
}

impl FromRfc2822 for Timestamp {
    fn from_rfc2822(s: &str) -> Result<Self, Rfc2822Error> {
        Ok(Timestamp::from(time::OffsetDateTime::from_unix_timestamp(
            DateTime::parse_from_rfc2822(s)?.timestamp(),
        )?))
    }
}
