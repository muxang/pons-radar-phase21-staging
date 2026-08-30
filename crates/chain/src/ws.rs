use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt, stream::SplitStream};
use pons_domain::{BlockNumber, ChainId};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::{
    ChainSubscription, LogFilter, RunError, SubscriptionEvent, SubscriptionFactory,
    http::{WireLog, parse_log, parse_quantity},
};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Clone, Debug)]
pub struct WsRpcProvider {
    url: String,
}

impl WsRpcProvider {
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }

    async fn open(&self) -> Result<Socket, RunError> {
        connect_async(&self.url)
            .await
            .map(|(socket, _)| socket)
            .map_err(|error| RunError::WebSocket(error.to_string()))
    }

    async fn request(
        socket: &mut Socket,
        id: u64,
        method: &str,
        params: Value,
    ) -> Result<Value, RunError> {
        socket
            .send(Message::Text(
                json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
                    .to_string()
                    .into(),
            ))
            .await
            .map_err(|error| RunError::WebSocket(error.to_string()))?;
        while let Some(message) = socket.next().await {
            let message = message.map_err(|error| RunError::WebSocket(error.to_string()))?;
            if let Message::Text(text) = message {
                let value: Value = serde_json::from_str(&text)
                    .map_err(|error| RunError::InvalidResponse(error.to_string()))?;
                if value.get("id").and_then(Value::as_u64) == Some(id) {
                    if let Some(error) = value.get("error") {
                        return Err(RunError::Rpc(error.to_string()));
                    }
                    return value.get("result").cloned().ok_or_else(|| {
                        RunError::InvalidResponse("missing WebSocket RPC result".to_owned())
                    });
                }
            }
        }
        Err(RunError::Disconnected)
    }

    /// Queries the WebSocket endpoint's chain identity without subscribing.
    ///
    /// # Errors
    ///
    /// Returns a transport or malformed-response error.
    pub async fn chain_id(&self) -> Result<ChainId, RunError> {
        let mut socket = self.open().await?;
        let value = Self::request(&mut socket, 1, "eth_chainId", json!([])).await?;
        let quantity = value
            .as_str()
            .ok_or_else(|| RunError::InvalidResponse("chain id is not a string".to_owned()))?;
        Ok(ChainId::new(parse_quantity(quantity)?))
    }
}

struct WsSubscription {
    stream: SplitStream<Socket>,
    heads_id: String,
    logs_id: String,
}

#[async_trait]
impl ChainSubscription for WsSubscription {
    async fn next(&mut self) -> Option<Result<SubscriptionEvent, RunError>> {
        loop {
            let message = self.stream.next().await?;
            let result = match message {
                Ok(Message::Text(text)) => {
                    let value: Value = match serde_json::from_str(&text) {
                        Ok(value) => value,
                        Err(error) => {
                            return Some(Err(RunError::InvalidResponse(error.to_string())));
                        }
                    };
                    let Some(params) = value.get("params") else {
                        continue;
                    };
                    let Some(subscription) = params.get("subscription").and_then(Value::as_str)
                    else {
                        continue;
                    };
                    let Some(payload) = params.get("result") else {
                        continue;
                    };
                    if subscription == self.heads_id {
                        payload
                            .get("number")
                            .and_then(Value::as_str)
                            .ok_or_else(|| {
                                RunError::InvalidResponse("newHeads number missing".to_owned())
                            })
                            .and_then(parse_quantity)
                            .map(|number| SubscriptionEvent::NewHead(BlockNumber::new(number)))
                    } else if subscription == self.logs_id {
                        serde_json::from_value::<WireLog>(payload.clone())
                            .map_err(|error| RunError::InvalidResponse(error.to_string()))
                            .and_then(parse_log)
                            .map(SubscriptionEvent::Log)
                    } else {
                        continue;
                    }
                }
                Ok(Message::Close(_)) => Err(RunError::Disconnected),
                Ok(_) => continue,
                Err(error) => Err(RunError::WebSocket(error.to_string())),
            };
            return Some(result);
        }
    }
}

fn subscription_filter(filter: &LogFilter) -> Value {
    let mut value = json!({});
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
impl SubscriptionFactory for WsRpcProvider {
    async fn connect(
        &self,
        expected_chain_id: ChainId,
        filter: &LogFilter,
    ) -> Result<Box<dyn ChainSubscription>, RunError> {
        let mut socket = self.open().await?;
        let chain_value = Self::request(&mut socket, 1, "eth_chainId", json!([])).await?;
        let actual = ChainId::new(parse_quantity(chain_value.as_str().ok_or_else(|| {
            RunError::InvalidResponse("chain id is not a string".to_owned())
        })?)?);
        if actual != expected_chain_id {
            return Err(RunError::WrongChain {
                expected: expected_chain_id,
                actual,
            });
        }
        let heads_id = Self::request(&mut socket, 2, "eth_subscribe", json!(["newHeads"]))
            .await?
            .as_str()
            .ok_or_else(|| {
                RunError::InvalidResponse("newHeads subscription id missing".to_owned())
            })?
            .to_owned();
        let logs_id = Self::request(
            &mut socket,
            3,
            "eth_subscribe",
            json!(["logs", subscription_filter(filter)]),
        )
        .await?
        .as_str()
        .ok_or_else(|| RunError::InvalidResponse("logs subscription id missing".to_owned()))?
        .to_owned();
        let (_, stream) = socket.split();
        Ok(Box::new(WsSubscription {
            stream,
            heads_id,
            logs_id,
        }))
    }
}
