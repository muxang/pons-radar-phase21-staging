use chrono::{TimeZone, Utc};
use pons_storage::repositories::{
    SignalInput, SignalJob, SignalMarketSnapshot, SignalPositionEvent, SignalSmartTrade,
};
use pons_v2::{SignalEngineConfig, evaluate_signals};
use rust_decimal::Decimal;
use uuid::Uuid;

fn config() -> SignalEngineConfig {
    SignalEngineConfig {
        windows: vec![30, 60, 180, 300, 900],
        minimum_independent: 2,
        minimum_qualified: 2,
        minimum_identity: Decimal::new(90, 2),
        watch: Decimal::from(45),
        strong: Decimal::from(65),
        high: Decimal::from(80),
        cooling: Decimal::from(70),
        cooling_exit: Decimal::new(40, 2),
        distribution_exit: Decimal::new(60, 2),
        minimum_confidence: Decimal::from(60),
        high_max_age_seconds: 900,
        weights: [45, 25, 15, 10, 5],
        timing: [
            Decimal::ONE,
            Decimal::new(90, 2),
            Decimal::new(75, 2),
            Decimal::new(60, 2),
            Decimal::new(40, 2),
        ],
        tiers: [
            Decimal::new(125, 2),
            Decimal::new(110, 2),
            Decimal::ONE,
            Decimal::new(85, 2),
            Decimal::new(90, 2),
        ],
        rule_version: 7,
        weight_version: 8,
        calculation_version: 9,
    }
}
fn at(seconds: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000 + seconds, 0).unwrap()
}
fn trade(
    marker: u8,
    seconds: i64,
    side: &str,
    quote: &str,
    source: &str,
    realtime: bool,
    rank: i64,
) -> SignalSmartTrade {
    SignalSmartTrade {
        id: Uuid::from_u128(u128::from(marker)),
        trader_id: Uuid::from_u128(100 + u128::from(marker)),
        wallet_address: vec![marker; 20],
        side: side.into(),
        quote_amount_raw: quote.into(),
        block_time: at(seconds),
        launch_age_ms: Some(seconds * 1000),
        buyer_rank: Some(rank),
        smart_buyer_rank: Some(rank),
        classification_source: source.into(),
        realtime_alert_eligible: realtime,
        identity_confidence: "1.0".into(),
        manual_tier: Some("B".into()),
        chain_status: "CONFIRMED".into(),
    }
}
fn market(seconds: i64, buyers: i64, holders: i64, exact: bool) -> SignalMarketSnapshot {
    SignalMarketSnapshot {
        effective_at: at(seconds),
        snapshot_kind: format!("T+{seconds}s"),
        unique_buyers: buyers,
        unique_sellers: 0,
        buy_count: buyers,
        sell_count: 0,
        user_net_flow_raw: "100".into(),
        curve_effective_net_flow_raw: "90".into(),
        holder_count: holders,
        curve_progress: Some("0.25".into()),
        state_scope: if exact {
            "BLOCK_STATE_EXACT"
        } else {
            "UNAVAILABLE"
        }
        .into(),
        integrity_status: "OK".into(),
    }
}
fn job(seconds: i64, origin: &str, realtime: bool) -> SignalJob {
    SignalJob {
        token_id: Uuid::new_v4(),
        generation: 1,
        attempts: 1,
        trigger_effective_at: Some(at(seconds)),
        trigger_origin: origin.into(),
        trigger_realtime_eligible: realtime,
    }
}

#[test]
fn consensus_uses_event_time_weights_ranks_positions_and_exact_raw_flows() {
    let input = SignalInput {
        launch_time: at(0),
        trades: vec![
            trade(1, 10, "BUY", "100", "LIVE", true, 1),
            trade(2, 20, "BUY", "200", "LIVE", true, 2),
            trade(3, 30, "BUY", "300", "LIVE", true, 3),
            trade(1, 40, "SELL", "150", "LIVE", true, 1),
        ],
        market: vec![],
        positions: vec![
            SignalPositionEvent {
                event_type: "OPEN_POSITION".into(),
                block_time: at(10),
                classification_source: "LIVE".into(),
                wallet_address: vec![1; 20],
            },
            SignalPositionEvent {
                event_type: "ADD_POSITION".into(),
                block_time: at(20),
                classification_source: "LIVE".into(),
                wallet_address: vec![1; 20],
            },
            SignalPositionEvent {
                event_type: "REDUCE_POSITION".into(),
                block_time: at(40),
                classification_source: "LIVE".into(),
                wallet_address: vec![1; 20],
            },
        ],
    };
    let out = evaluate_signals(&input, &job(40, "LIVE", true), &config()).unwrap();
    let c = out
        .consensus
        .iter()
        .find(|v| v.effective_at == at(40) && v.window_seconds == 900)
        .unwrap();
    assert_eq!((c.raw, c.qualified, c.independent), (3, 3, 3));
    assert_eq!(
        (c.buy_raw.as_str(), c.sell_raw.as_str(), c.net_raw.as_str()),
        ("600", "150", "450")
    );
    assert_eq!((c.open, c.add, c.reduce, c.close), (1, 1, 1, 0));
    assert_eq!(out.rule_version, 7);
    assert!(c.timing.as_array().unwrap().len() == 3 && c.rank.as_array().unwrap().len() == 3);
}

#[test]
fn missing_research_is_unavailable_and_weights_are_renormalized_not_zero_scored() {
    let input = SignalInput {
        launch_time: at(0),
        trades: vec![trade(1, 10, "BUY", "100", "LIVE", true, 1)],
        market: vec![],
        positions: vec![],
    };
    let out = evaluate_signals(&input, &job(10, "LIVE", true), &config()).unwrap();
    let s = &out.signals[0];
    assert_eq!(
        s.component_scores["research_narrative"]["status"],
        "UNAVAILABLE"
    );
    assert!(
        s.weights["renormalized_denominator"]
            .as_str()
            .unwrap()
            .parse::<Decimal>()
            .unwrap()
            < Decimal::from(100)
    );
    assert!(s.score.parse::<Decimal>().unwrap() > Decimal::ZERO);
}

#[test]
fn evidence_quality_changes_confidence_without_changing_chain_facts() {
    let trades = vec![
        trade(1, 10, "BUY", "100", "LIVE", true, 1),
        trade(2, 20, "BUY", "100", "LIVE", true, 2),
        trade(3, 30, "BUY", "100", "LIVE", true, 3),
    ];
    let exact = SignalInput {
        launch_time: at(0),
        trades: trades.clone(),
        market: vec![market(30, 3, 3, true)],
        positions: vec![],
    };
    let missing = SignalInput {
        launch_time: at(0),
        trades,
        market: vec![market(30, 3, 3, false)],
        positions: vec![],
    };
    let a = evaluate_signals(&exact, &job(30, "MARKET_SNAPSHOT", false), &config()).unwrap();
    let b = evaluate_signals(&missing, &job(30, "MARKET_SNAPSHOT", false), &config()).unwrap();
    assert!(
        a.signals
            .last()
            .unwrap()
            .confidence
            .parse::<Decimal>()
            .unwrap()
            > b.signals
                .last()
                .unwrap()
                .confidence
                .parse::<Decimal>()
                .unwrap()
    );
}

#[test]
fn live_and_historical_origins_propagate_without_fake_realtime() {
    let live = SignalInput {
        launch_time: at(0),
        trades: vec![trade(1, 10, "BUY", "100", "LIVE", true, 1)],
        market: vec![],
        positions: vec![],
    };
    let historical = SignalInput {
        launch_time: at(0),
        trades: vec![trade(1, 10, "BUY", "100", "CHAIN_BACKFILL", false, 1)],
        market: vec![],
        positions: vec![],
    };
    assert!(
        evaluate_signals(&live, &job(10, "LIVE", true), &config())
            .unwrap()
            .signals[0]
            .realtime
    );
    let h = evaluate_signals(&historical, &job(10, "CHAIN_BACKFILL", false), &config()).unwrap();
    assert!(!h.signals[0].realtime);
    assert_eq!(h.signals[0].origin, "CHAIN_BACKFILL");
}

#[test]
fn hysteresis_and_exit_rules_prevent_threshold_flapping() {
    let mut trades = vec![
        trade(1, 10, "BUY", "100", "LIVE", true, 1),
        trade(2, 20, "BUY", "100", "LIVE", true, 2),
        trade(3, 30, "BUY", "100", "LIVE", true, 3),
    ];
    let mut positions = vec![SignalPositionEvent {
        event_type: "ADD_POSITION".into(),
        block_time: at(40),
        classification_source: "LIVE".into(),
        wallet_address: vec![1; 20],
    }];
    let mut markets = vec![market(50, 3, 3, true)];
    let first = SignalInput {
        launch_time: at(0),
        trades: trades.clone(),
        market: markets.clone(),
        positions: positions.clone(),
    };
    let out = evaluate_signals(&first, &job(50, "MARKET_SNAPSHOT", false), &config()).unwrap();
    let states: Vec<_> = out.transitions.iter().map(|v| v.to.as_str()).collect();
    assert!(
        states.contains(&"WATCH")
            && states.contains(&"STRONG_WATCH")
            && states.contains(&"HIGH_PRIORITY")
    );
    trades.push(trade(1, 60, "SELL", "300", "LIVE", true, 1));
    trades.push(trade(2, 61, "SELL", "300", "LIVE", true, 2));
    positions.push(SignalPositionEvent {
        event_type: "CLOSE_POSITION".into(),
        block_time: at(60),
        classification_source: "LIVE".into(),
        wallet_address: vec![1; 20],
    });
    positions.push(SignalPositionEvent {
        event_type: "CLOSE_POSITION".into(),
        block_time: at(61),
        classification_source: "LIVE".into(),
        wallet_address: vec![2; 20],
    });
    markets.push(market(62, 3, 3, true));
    let second = evaluate_signals(
        &SignalInput {
            launch_time: at(0),
            trades,
            market: markets,
            positions,
        },
        &job(62, "MARKET_SNAPSHOT", false),
        &config(),
    )
    .unwrap();
    assert!(second.transitions.iter().any(|v| v.to == "DISTRIBUTION"));
    assert_eq!(
        second.transitions.iter().filter(|v| v.from == v.to).count(),
        0
    );
}

#[test]
fn early_high_rank_entry_has_more_weight_than_late_low_rank_entry() {
    let early = SignalInput {
        launch_time: at(0),
        trades: vec![trade(1, 10, "BUY", "1", "LIVE", true, 1)],
        market: vec![],
        positions: vec![],
    };
    let late = SignalInput {
        launch_time: at(0),
        trades: vec![trade(1, 1_000, "BUY", "1", "CHAIN_BACKFILL", false, 400)],
        market: vec![],
        positions: vec![],
    };
    let a = evaluate_signals(&early, &job(10, "LIVE", true), &config()).unwrap();
    let b = evaluate_signals(&late, &job(1_000, "CHAIN_BACKFILL", false), &config()).unwrap();
    assert!(
        a.consensus
            .last()
            .unwrap()
            .weighted
            .parse::<Decimal>()
            .unwrap()
            > b.consensus
                .last()
                .unwrap()
                .weighted
                .parse::<Decimal>()
                .unwrap()
    );
}
