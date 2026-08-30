use std::sync::Arc;

use axum::{Json, Router, extract::State, routing::post};
use pons_chain::{ChainRpc, HttpRpcProvider, LogFilter};
use pons_domain::{BlockNumber, TxHash};
use serde_json::{Value, json};
use tokio::net::TcpListener;

const BLOCK_HASH: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
const PARENT_HASH: &str = "0x2222222222222222222222222222222222222222222222222222222222222222";
const TX_HASH: &str = "0x3333333333333333333333333333333333333333333333333333333333333333";
const ADDRESS: &str = "0x4444444444444444444444444444444444444444";
const TOPIC: &str = "0x5555555555555555555555555555555555555555555555555555555555555555";

async fn rpc(State(_state): State<Arc<()>>, Json(request): Json<Value>) -> Json<Value> {
    let id = request["id"].clone();
    let log = json!({
        "blockNumber":"0x2a", "blockHash":BLOCK_HASH, "transactionHash":TX_HASH,
        "transactionIndex":"0x3", "logIndex":"0x0", "address":ADDRESS, "topics":[TOPIC], "data":"0xabcd",
        "removed":false
    });
    let result = match request["method"].as_str().unwrap() {
        "eth_chainId" => json!("0x1237"),
        "eth_blockNumber" => json!("0x2a"),
        "eth_getCode" => json!("0x6001"),
        "eth_call" => {
            assert_eq!(request["params"][0]["to"], ADDRESS);
            assert_eq!(request["params"][0]["data"], "0x1234");
            assert_eq!(request["params"][1], "0x2a");
            json!("0xdeadbeef")
        }
        "eth_getBlockByNumber" => {
            json!({"number":"0x2a","hash":BLOCK_HASH,"parentHash":PARENT_HASH,"timestamp":"0x64"})
        }
        "eth_getLogs" => json!([log.clone()]),
        "eth_getTransactionReceipt" => {
            json!({"transactionHash":TX_HASH,"blockNumber":"0x2a","blockHash":BLOCK_HASH,"status":"0x1","logs":[log]})
        }
        method => panic!("unexpected fixture method {method}"),
    };
    Json(json!({"jsonrpc":"2.0","id":id,"result":result}))
}

#[tokio::test]
async fn http_provider_supports_chain_block_logs_and_receipts() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/", post(rpc)).with_state(Arc::new(())),
        )
        .await
        .unwrap();
    });
    let provider = HttpRpcProvider::new(format!("http://{address}"));

    assert_eq!(provider.chain_id().await.unwrap().get(), 4663);
    assert_eq!(provider.block_number().await.unwrap(), BlockNumber::new(42));
    assert_eq!(
        provider.code(ADDRESS.parse().unwrap()).await.unwrap(),
        [0x60, 0x01]
    );
    assert_eq!(
        provider
            .call(
                ADDRESS.parse().unwrap(),
                vec![0x12, 0x34],
                BlockNumber::new(42),
            )
            .await
            .unwrap(),
        [0xde, 0xad, 0xbe, 0xef]
    );
    let block = provider.block(BlockNumber::new(42)).await.unwrap().unwrap();
    assert_eq!(block.number, BlockNumber::new(42));
    assert_eq!(block.timestamp, 100);

    let logs = provider
        .logs(
            BlockNumber::new(40),
            BlockNumber::new(42),
            &LogFilter::default(),
        )
        .await
        .unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].transaction_index, Some(3));
    assert_eq!(logs[0].data, [0xab, 0xcd]);

    let receipt = provider
        .receipt(TX_HASH.parse::<TxHash>().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(receipt.succeeded);
    assert_eq!(receipt.logs, logs);
}
