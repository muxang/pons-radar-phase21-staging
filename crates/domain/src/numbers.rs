use std::{fmt, str::FromStr};

use alloy_primitives::U256;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ChainId(u64);

impl ChainId {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BlockNumber(u64);

impl BlockNumber {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LogIndex(u64);

impl LogIndex {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
#[error("invalid unsigned 256-bit amount: {0}")]
pub struct RawAmountParseError(String);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RawAmount(U256);

impl RawAmount {
    #[must_use]
    pub const fn new(value: U256) -> Self {
        Self(value)
    }
    #[must_use]
    pub const fn value(self) -> U256 {
        self.0
    }
    #[must_use]
    pub fn to_storage_string(self) -> String {
        self.0.to_string()
    }
}

impl FromStr for RawAmount {
    type Err = RawAmountParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty()
            || (value.len() > 1 && value.starts_with('0'))
            || !value.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(RawAmountParseError(value.to_owned()));
        }
        U256::from_str_radix(value, 10)
            .map(Self)
            .map_err(|_| RawAmountParseError(value.to_owned()))
    }
}

impl fmt::Display for RawAmount {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Serialize for RawAmount {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for RawAmount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NormalizedAmount(Decimal);

impl NormalizedAmount {
    #[must_use]
    pub const fn new(value: Decimal) -> Self {
        Self(value)
    }
    #[must_use]
    pub const fn value(self) -> Decimal {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_amount_round_trips_full_u256_range() {
        let amount = RawAmount::new(U256::MAX);
        let encoded = amount.to_storage_string();
        assert_eq!(encoded.parse::<RawAmount>().unwrap(), amount);
        assert_eq!(
            encoded,
            "115792089237316195423570985008687907853269984665640564039457584007913129639935"
        );
    }

    #[test]
    fn raw_amount_rejects_non_canonical_decimal() {
        for invalid in ["", "-1", "+1", "01", "1.0", "0x10"] {
            assert!(invalid.parse::<RawAmount>().is_err(), "accepted {invalid}");
        }
        assert_eq!("0".parse::<RawAmount>().unwrap().to_string(), "0");
    }
}
