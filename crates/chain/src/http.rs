use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use pons_domain::{BlockNumber, ChainId, ContractAddress, LogIndex, TxHash};
use reqwest::Client;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};

use crate::{BlockHeader, ChainLog, ChainRpc, LogFilter, Receipt, RunError};

#[derive(Debug)]
pub struct HttpRpcProvider {
    client: Client,
    url: String,
    next_id: AtomicU64,
}

impl HttpRpcProvider {
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            url: url.into(),
            next_id: AtomicU64::new(1),
        }
    }

    async fn request<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Value,
    ) -> Result<T, RunError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let response = self
            .client
            .post(&self.url)
            .json(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
            .send()
            .await
            .map_err(|error| RunError::Rpc(error.to_string()))?;
        if !response.status().is_success() {
            return Err(RunError::Rpc(format!("HTTP status {}", response.status())));
        }
        let envelope: RpcEnvelope<T> = response
            .json()
            .await
            .map_err(|error| RunError::Rpc(error.to_string()))?;
        match (envelope.result, envelope.error) {
            (Some(result), None) => Ok(result),
            (_, Some(error)) => Err(RunError::Rpc(format!("{} ({})", error.message, error.code))),
            _ => Err(RunError::Rpc("missing JSON-RPC result".to_owned())),
        }
    }
}

#[derive(Deserialize)]
struct RpcEnvelope<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireBlock {
    number: String,
    hash: String,
    parent_hash: String,
    timestamp: String,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WireLog {
    block_number: String,
    block_hash: String,
    transaction_hash: String,
    #[serde(default)]
    transaction_index: Option<String>,
    log_index: String,
    address: String,
    topics: Vec<String>,
    data: String,
    #[serde(default)]
    removed: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireReceipt {
    transaction_hash: String,
    block_number: String,
    block_hash: String,
    status: String,
    logs: Vec<WireLog>,
}

pub(crate) fn parse_quantity(value: &str) -> Result<u64, RunError> {
    let digits = value
        .strip_prefix("0x")
        .ok_or_else(|| RunError::InvalidResponse("quantity lacks 0x prefix".to_owned()))?;
    u64::from_str_radix(digits, 16).map_err(|error| RunError::InvalidResponse(error.to_string()))
}

pub(crate) fn parse_log(value: WireLog) -> Result<ChainLog, RunError> {
    Ok(ChainLog {
        block_number: BlockNumber::new(parse_quantity(&value.block_number)?),
        block_hash: value.block_hash.parse().map_err(
            |error: pons_domain::IdentifierParseError| RunError::InvalidResponse(error.to_string()),
        )?,
        tx_hash: value.transaction_hash.parse().map_err(
            |error: pons_domain::IdentifierParseError| RunError::InvalidResponse(error.to_string()),
        )?,
        transaction_index: value
            .transaction_index
            .as_deref()
            .map(parse_quantity)
            .transpose()?,
        log_index: LogIndex::new(parse_quantity(&value.log_index)?),
        address: value
            .address
            .parse()
            .map_err(|error: pons_domain::IdentifierParseError| {
                RunError::InvalidResponse(error.to_string())
            })?,
        topics: value
            .topics
            .into_iter()
            .map(|topic| {
                topic
                    .parse()
                    .map_err(|error: pons_domain::IdentifierParseError| {
                        RunError::InvalidResponse(error.to_string())
                    })
            })
            .collect::<Result<_, _>>()?,
        data: decode_hex(&value.data)?,
        removed: value.removed,
    })
}

fn decode_hex(value: &str) -> Result<Vec<u8>, RunError> {
    let digits = value
        .strip_prefix("0x")
        .ok_or_else(|| RunError::InvalidResponse("bytes lack 0x prefix".to_owned()))?;
    if digits.len() % 2 != 0 {
        return Err(RunError::InvalidResponse("odd-length hex bytes".to_owned()));
    }
    (0..digits.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&digits[index..index + 2], 16)
                .map_err(|error| RunError::InvalidResponse(error.to_string()))
        })
        .collect()
}

fn filter_json(from: BlockNumber, to: BlockNumber, filter: &LogFilter) -> Value {
    let mut value =
        json!({"fromBlock":format!("0x{:x}", from.get()),"toBlock":format!("0x{:x}", to.get())});
    if !filter.addresses.is_empty() {
        value["address"] = Value::Array(
            filter
                .addresses
                .iter()
                .map(|address| Value::String(address.to_string()))
                .collect(),
        );
    }
    if !filter.topics.is_empty() {
        value["topics"] = Value::Array(
            filter
                .topics
                .iter()
                .map(|position| match position {
                    None => Value::Null,
                    Some(topics) if topics.len() == 1 => Value::String(topics[0].to_string()),
                    Some(topics) => Value::Array(
                        topics
                            .iter()
                            .map(|topic| Value::String(topic.to_string()))
                            .collect(),
                    ),
                })
                .collect(),
        );
    }
    value
}

#[async_trait]
impl ChainRpc for HttpRpcProvider {
    async fn chain_id(&self) -> Result<ChainId, RunError> {
        let value: String = self.request("eth_chainId", json!([])).await?;
        Ok(ChainId::new(parse_quantity(&value)?))
    }

    async fn block_number(&self) -> Result<BlockNumber, RunError> {
        let value: String = self.request("eth_blockNumber", json!([])).await?;
        Ok(BlockNumber::new(parse_quantity(&value)?))
    }

    async fn code(&self, address: ContractAddress) -> Result<Vec<u8>, RunError> {
        let value: String = self
            .request("eth_getCode", json!([address.to_string(), "latest"]))
            .await?;
        decode_hex(&value)
    }

    async fn call(
        &self,
        address: ContractAddress,
        data: Vec<u8>,
        block: BlockNumber,
    ) -> Result<Vec<u8>, RunError> {
        let value: String = self
            .request(
                "eth_call",
                json!([{
                    "to": address.to_string(),
                    "data": format!("0x{}", encode_hex(&data))
                }, format!("0x{:x}", block.get())]),
            )
            .await?;
        decode_hex(&value)
    }

    async fn block(&self, number: BlockNumber) -> Result<Option<BlockHeader>, RunError> {
        let value: Option<WireBlock> = self
            .request(
                "eth_getBlockByNumber",
                json!([format!("0x{:x}", number.get()), false]),
            )
            .await?;
        value
            .map(|block| {
                Ok(BlockHeader {
                    number: BlockNumber::new(parse_quantity(&block.number)?),
                    hash: block.hash.parse().map_err(
                        |error: pons_domain::IdentifierParseError| {
                            RunError::InvalidResponse(error.to_string())
                        },
                    )?,
                    parent_hash: block.parent_hash.parse().map_err(
                        |error: pons_domain::IdentifierParseError| {
                            RunError::InvalidResponse(error.to_string())
                        },
                    )?,
                    timestamp: parse_quantity(&block.timestamp)?,
                })
            })
            .transpose()
    }

    async fn receipt(&self, hash: TxHash) -> Result<Option<Receipt>, RunError> {
        let value: Option<WireReceipt> = self
            .request("eth_getTransactionReceipt", json!([hash.to_string()]))
            .await?;
        value
            .map(|receipt| {
                Ok(Receipt {
                    tx_hash: receipt.transaction_hash.parse().map_err(
                        |error: pons_domain::IdentifierParseError| {
                            RunError::InvalidResponse(error.to_string())
                        },
                    )?,
                    block_number: BlockNumber::new(parse_quantity(&receipt.block_number)?),
                    block_hash: receipt.block_hash.parse().map_err(
                        |error: pons_domain::IdentifierParseError| {
                            RunError::InvalidResponse(error.to_string())
                        },
                    )?,
                    succeeded: parse_quantity(&receipt.status)? == 1,
                    logs: receipt
                        .logs
                        .into_iter()
                        .map(parse_log)
                        .collect::<Result<_, _>>()?,
                })
            })
            .transpose()
    }

    async fn logs(
        &self,
        from: BlockNumber,
        to: BlockNumber,
        filter: &LogFilter,
    ) -> Result<Vec<ChainLog>, RunError> {
        let values: Vec<WireLog> = self
            .request("eth_getLogs", json!([filter_json(from, to, filter)]))
            .await?;
        values.into_iter().map(parse_log).collect()
    }
}

fn encode_hex(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}
