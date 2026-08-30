use futures_util::{SinkExt, StreamExt};
use pons_chain::{LogFilter, SubscriptionEvent, SubscriptionFactory, WsRpcProvider};
use pons_domain::{BlockNumber, ChainId};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message};

const BLOCK_HASH: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
const TX_HASH: &str = "0x3333333333333333333333333333333333333333333333333333333333333333";
const ADDRESS: &str = "0x4444444444444444444444444444444444444444";

async fn request(socket: &mut tokio_tungstenite::WebSocketStream<TcpStream>) -> Value {
    let message = socket.next().await.unwrap().unwrap();
    let Message::Text(text) = message else {
        panic!("expected text request")
    };
    serde_json::from_str(&text).unwrap()
}

async fn respond(
    socket: &mut tokio_tungstenite::WebSocketStream<TcpStream>,
    id: Value,
    result: Value,
) {
    socket
        .send(Message::Text(
            json!({"jsonrpc":"2.0","id":id,"result":result})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn websocket_provider_verifies_chain_and_subscribes_to_heads_and_logs() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server =
        tokio::spawn(async move {
            // Standalone startup chain check.
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let chain = request(&mut socket).await;
            assert_eq!(chain["method"], "eth_chainId");
            respond(&mut socket, chain["id"].clone(), json!("0x1237")).await;
            socket.close(None).await.unwrap();

            // Reconnecting subscription performs its own fail-closed chain check.
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let chain = request(&mut socket).await;
            respond(&mut socket, chain["id"].clone(), json!("0x1237")).await;
            let heads = request(&mut socket).await;
            assert_eq!(heads["params"], json!(["newHeads"]));
            respond(&mut socket, heads["id"].clone(), json!("heads-sub")).await;
            let logs = request(&mut socket).await;
            assert_eq!(logs["params"][0], "logs");
            respond(&mut socket, logs["id"].clone(), json!("logs-sub")).await;

            socket
                .send(Message::Text(
                    json!({
                        "jsonrpc":"2.0","method":"eth_subscription",
                        "params":{"subscription":"heads-sub","result":{"number":"0x2a"}}
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            socket.send(Message::Text(json!({
            "jsonrpc":"2.0","method":"eth_subscription",
            "params":{"subscription":"logs-sub","result":{
                "blockNumber":"0x2a","blockHash":BLOCK_HASH,"transactionHash":TX_HASH,
                "logIndex":"0x0","address":ADDRESS,"topics":[],"data":"0x","removed":false
            }}
        }).to_string().into())).await.unwrap();
        });

    let provider = WsRpcProvider::new(format!("ws://{address}"));
    assert_eq!(provider.chain_id().await.unwrap(), ChainId::new(4663));
    let mut subscription = provider
        .connect(ChainId::new(4663), &LogFilter::default())
        .await
        .unwrap();
    assert_eq!(
        subscription.next().await.unwrap().unwrap(),
        SubscriptionEvent::NewHead(BlockNumber::new(42))
    );
    assert!(matches!(
        subscription.next().await.unwrap().unwrap(),
        SubscriptionEvent::Log(_)
    ));
    server.await.unwrap();
}
