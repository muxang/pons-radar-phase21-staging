use std::{fmt, str::FromStr};

use alloy_primitives::{Address, B256};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum IdentifierParseError {
    #[error("invalid EVM address: {0}")]
    Address(String),
    #[error("invalid EVM hash: {0}")]
    Hash(String),
}

macro_rules! address_type {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(Address);

        impl $name {
            pub const BYTE_LENGTH: usize = 20;

            #[must_use]
            pub const fn new(value: Address) -> Self {
                Self(value)
            }

            #[must_use]
            pub const fn as_address(&self) -> &Address {
                &self.0
            }

            #[must_use]
            pub fn as_bytes(&self) -> &[u8; Self::BYTE_LENGTH] {
                self.0.as_ref()
            }

            /// Creates the identifier from its exact binary representation.
            ///
            /// # Errors
            ///
            /// Returns an error unless the slice contains exactly 20 bytes.
            pub fn from_slice(value: &[u8]) -> Result<Self, IdentifierParseError> {
                if value.len() != Self::BYTE_LENGTH {
                    return Err(IdentifierParseError::Address(format!(
                        "expected 20 bytes, got {}",
                        value.len()
                    )));
                }
                Ok(Self(Address::from_slice(value)))
            }
        }

        impl FromStr for $name {
            type Err = IdentifierParseError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Address::from_str(value)
                    .map(Self)
                    .map_err(|error| IdentifierParseError::Address(error.to_string()))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{:#x}", self.0)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.to_string())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                String::deserialize(deserializer)?
                    .parse()
                    .map_err(D::Error::custom)
            }
        }
    };
}

address_type!(TokenAddress);
address_type!(CurveAddress);
address_type!(WalletAddress);
address_type!(ContractAddress);

macro_rules! hash_type {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(B256);

        impl $name {
            pub const BYTE_LENGTH: usize = 32;

            #[must_use]
            pub const fn new(value: B256) -> Self {
                Self(value)
            }

            #[must_use]
            pub const fn as_hash(&self) -> &B256 {
                &self.0
            }

            #[must_use]
            pub fn as_bytes(&self) -> &[u8; Self::BYTE_LENGTH] {
                self.0.as_ref()
            }

            /// Creates the hash from its exact binary representation.
            ///
            /// # Errors
            ///
            /// Returns an error unless the slice contains exactly 32 bytes.
            pub fn from_slice(value: &[u8]) -> Result<Self, IdentifierParseError> {
                if value.len() != Self::BYTE_LENGTH {
                    return Err(IdentifierParseError::Hash(format!(
                        "expected 32 bytes, got {}",
                        value.len()
                    )));
                }
                Ok(Self(B256::from_slice(value)))
            }
        }

        impl FromStr for $name {
            type Err = IdentifierParseError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                B256::from_str(value)
                    .map(Self)
                    .map_err(|error| IdentifierParseError::Hash(error.to_string()))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{:#x}", self.0)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.to_string())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                String::deserialize(deserializer)?
                    .parse()
                    .map_err(D::Error::custom)
            }
        }
    };
}

hash_type!(TxHash);
hash_type!(BlockHash);
hash_type!(LogTopic);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_normalizes_to_lowercase_fixed_width_hex() {
        let address: WalletAddress = "0x7eD598BcEf8bd9Edd8C97A195C6d13f40801EC7e"
            .parse()
            .unwrap();
        assert_eq!(
            address.to_string(),
            "0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e"
        );
        assert_eq!(
            WalletAddress::from_slice(address.as_bytes()).unwrap(),
            address
        );
    }

    #[test]
    fn invalid_addresses_and_hashes_are_rejected() {
        assert!("0x1234".parse::<TokenAddress>().is_err());
        assert!(TxHash::from_slice(&[0_u8; 31]).is_err());
        assert!("not-a-hash".parse::<TxHash>().is_err());
    }
}
