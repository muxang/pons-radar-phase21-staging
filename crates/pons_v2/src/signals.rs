use chrono::{DateTime, Duration as ChronoDuration, TimeDelta, Utc};
use num_bigint::BigUint;
use pons_storage::repositories::{
    ConsensusWrite, SignalInput, SignalJob, SignalRebuild, SignalRepository, SignalSmartTrade,
    SignalWrite, TransitionWrite,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    str::FromStr,
    time::Duration,
};
use thiserror::Error;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct SignalEngineConfig {
    pub windows: Vec<i32>,
    pub minimum_independent: i64,
    pub minimum_qualified: i64,
    pub minimum_identity: Decimal,
    pub watch: Decimal,
    pub strong: Decimal,
    pub high: Decimal,
    pub cooling: Decimal,
    pub cooling_exit: Decimal,
    pub distribution_exit: Decimal,
    pub minimum_confidence: Decimal,
    pub high_max_age_seconds: i64,
    pub weights: [u32; 5],
    pub timing: [Decimal; 5],
    pub tiers: [Decimal; 5],
    pub rule_version: i32,
    pub weight_version: i32,
    pub calculation_version: i32,
}
#[derive(Clone, Copy, Debug)]
pub struct SignalWorkerSettings {
    pub concurrency: usize,
    pub poll_interval: Duration,
    pub retry_minimum: Duration,
    pub retry_maximum: Duration,
}
#[derive(Debug, Error)]
pub enum SignalError {
    #[error("invalid signal configuration: {0}")]
    Invalid(String),
    #[error("signal storage: {0}")]
    Storage(String),
    #[error("signal task: {0}")]
    Task(String),
}
#[derive(Clone)]
pub struct SignalWorker {
    repo: SignalRepository,
    engine: SignalEngineConfig,
    worker: SignalWorkerSettings,
}

impl SignalWorker {
    #[allow(clippy::missing_errors_doc)]
    pub fn new(
        repo: SignalRepository,
        engine: SignalEngineConfig,
        worker: SignalWorkerSettings,
    ) -> Result<Self, SignalError> {
        engine.validate()?;
        if worker.concurrency == 0
            || worker.poll_interval.is_zero()
            || worker.retry_minimum.is_zero()
            || worker.retry_minimum > worker.retry_maximum
        {
            return Err(SignalError::Invalid("worker settings".into()));
        }
        Ok(Self {
            repo,
            engine,
            worker,
        })
    }
    #[allow(clippy::missing_errors_doc)]
    pub async fn run_until(self, c: CancellationToken) -> Result<(), SignalError> {
        let mut set = JoinSet::new();
        for _ in 0..self.worker.concurrency {
            let w = self.clone();
            let c = c.clone();
            set.spawn(async move { w.loop_until(c).await });
        }
        while let Some(v) = set.join_next().await {
            v.map_err(|e| SignalError::Task(e.to_string()))??;
        }
        Ok(())
    }
    async fn loop_until(&self, c: CancellationToken) -> Result<(), SignalError> {
        loop {
            if c.is_cancelled() {
                return Ok(());
            }
            if let Some(j) = self.repo.claim_due().await.map_err(storage)? {
                let result = async {
                    let input = self.repo.load(j.token_id).await.map_err(storage)?;
                    let rebuilt = evaluate(&input, &j, &self.engine)?;
                    self.repo.persist(&j, &rebuilt).await.map_err(storage)
                }
                .await;
                if let Err(error) = result {
                    let delay = self
                        .worker
                        .retry_minimum
                        .saturating_mul(
                            2_u32.saturating_pow(
                                u32::try_from(j.attempts.saturating_sub(1))
                                    .unwrap_or(0)
                                    .min(20),
                            ),
                        )
                        .min(self.worker.retry_maximum);
                    self.repo
                        .retry(
                            &j,
                            &error.to_string(),
                            Utc::now() + TimeDelta::from_std(delay).unwrap_or(TimeDelta::MAX),
                        )
                        .await
                        .map_err(storage)?;
                }
                continue;
            }
            tokio::select! {()=c.cancelled()=>return Ok(()),()=tokio::time::sleep(self.worker.poll_interval)=>{}}
        }
    }
}
impl SignalEngineConfig {
    fn validate(&self) -> Result<(), SignalError> {
        if self.windows != [30, 60, 180, 300, 900]
            || self.weights.iter().all(|v| *v == 0)
            || self.watch >= self.strong
            || self.strong >= self.high
        {
            return Err(SignalError::Invalid(
                "windows, weights, or thresholds".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Summary {
    raw: i64,
    qualified: i64,
    independent: i64,
    buy: BigUint,
    sell: BigUint,
    net: String,
    first: Option<i64>,
    median: Option<i64>,
    open: i64,
    add: i64,
    reduce: i64,
    close: i64,
    wallet_exit: Decimal,
    quote_exit: Decimal,
    weighted: Decimal,
    timing: Value,
    rank: Value,
    position: Value,
    finality: String,
}

#[allow(clippy::too_many_lines)]
/// Rebuilds consensus and signal history from durable token inputs.
///
/// # Errors
///
/// Returns an error when configured windows are invalid or stored U256/Decimal inputs cannot be
/// decoded without loss.
pub fn evaluate(
    input: &SignalInput,
    job: &SignalJob,
    cfg: &SignalEngineConfig,
) -> Result<SignalRebuild, SignalError> {
    let mut points = BTreeSet::new();
    for v in &input.trades {
        points.insert(v.block_time);
    }
    for v in &input.market {
        points.insert(v.effective_at);
    }
    for v in &input.positions {
        points.insert(v.block_time);
    }
    let points: Vec<_> = points.into_iter().collect();
    let mut consensus = Vec::new();
    let mut signals = Vec::new();
    let mut transitions = Vec::new();
    let mut state = "NO_SIGNAL".to_owned();
    let mut strong_streak = 0;
    let mut high_streak = 0;
    for at in points {
        let (origin, realtime) = origin(input, job, at);
        let mut selected = None;
        for window in &cfg.windows {
            let s = consensus_at(input, at, *window, cfg)?;
            let inputs = json!({"effective_at":at,"window_seconds":window,"event_time_basis":true,"confirmed_smart_trades_only":true,"independence_model":"UNCLUSTERED_V1","configuration":{"rule_version":cfg.rule_version,"timing_buckets_seconds":[60,180,300,900],"minimum_identity_confidence":cfg.minimum_identity.to_string()}});
            let body = json!({"raw":s.raw,"qualified":s.qualified,"independent":s.independent,"buy":s.buy.to_string(),"sell":s.sell.to_string(),"net":s.net,"weighted":s.weighted.to_string(),"inputs":inputs});
            consensus.push(ConsensusWrite {
                effective_at: at,
                window_seconds: *window,
                raw: s.raw,
                qualified: s.qualified,
                independent: s.independent,
                buy_raw: s.buy.to_string(),
                sell_raw: s.sell.to_string(),
                net_raw: s.net.clone(),
                first_age: s.first,
                median_age: s.median,
                open: s.open,
                add: s.add,
                reduce: s.reduce,
                close: s.close,
                wallet_exit: s.wallet_exit.to_string(),
                quote_exit: s.quote_exit.to_string(),
                weighted: s.weighted.to_string(),
                timing: s.timing.clone(),
                rank: s.rank.clone(),
                position: s.position.clone(),
                inputs,
                hash: hash(&body),
                origin: origin.clone(),
                realtime,
                finality: s.finality.clone(),
            });
            selected = Some(s);
        }
        let Some(s) = selected else {
            return Err(SignalError::Invalid(
                "the 15 minute consensus window is required".into(),
            ));
        };
        let latest = input
            .market
            .iter()
            .filter(|v| v.effective_at <= at)
            .max_by_key(|v| v.effective_at);
        let previous = latest.and_then(|_| {
            input
                .market
                .iter()
                .filter(|v| v.effective_at <= at - ChronoDuration::seconds(900))
                .max_by_key(|v| v.effective_at)
        });
        let smart = clamp(
            s.weighted * Decimal::from(100) / Decimal::from(3),
            Decimal::ZERO,
            Decimal::ONE_HUNDRED,
        );
        let momentum = market_momentum(latest, previous);
        let capital = capital_score(latest);
        let holder = holder_score(latest, previous);
        let components = [Some(smart), momentum, capital, holder, None];
        let names = [
            "smart_consensus",
            "pons_momentum",
            "capital_flow",
            "holder_distribution",
            "research_narrative",
        ];
        let mut used = Decimal::ZERO;
        let mut total = Decimal::ZERO;
        let mut scores = serde_json::Map::new();
        let mut applied = serde_json::Map::new();
        for i in 0..5 {
            if let Some(v) = components[i] {
                used += v * Decimal::from(cfg.weights[i]);
                total += Decimal::from(cfg.weights[i]);
                scores.insert(
                    names[i].into(),
                    json!({"status":"AVAILABLE","score":v.to_string()}),
                );
                applied.insert(names[i].into(), json!(cfg.weights[i]));
            } else {
                scores.insert(names[i].into(), json!({"status":"UNAVAILABLE"}));
                applied.insert(names[i].into(), Value::Null);
            }
        }
        let score = if total.is_zero() {
            Decimal::ZERO
        } else {
            used / total
        };
        let exact = latest
            .is_some_and(|v| v.state_scope == "BLOCK_STATE_EXACT" && v.integrity_status == "OK");
        let sample = clamp(
            Decimal::from(s.qualified) * Decimal::from(100) / Decimal::from(3),
            Decimal::ZERO,
            Decimal::ONE_HUNDRED,
        );
        let availability =
            total / Decimal::from(cfg.weights.iter().sum::<u32>()) * Decimal::from(100);
        let confidence = (sample * Decimal::from(40)
            + Decimal::from(if exact { 100 } else { 50 }) * Decimal::from(35)
            + availability * Decimal::from(25))
            / Decimal::from(100);
        let age = (at - input.launch_time).num_seconds().max(0);
        let high_rule = s.independent >= cfg.minimum_independent
            && s.qualified >= cfg.minimum_qualified
            && signed_positive(&s.net)
            && age <= cfg.high_max_age_seconds
            && confidence >= cfg.minimum_confidence;
        let distribution = s.wallet_exit >= cfg.distribution_exit
            || (s.quote_exit >= cfg.distribution_exit && s.reduce + s.close >= 2);
        let rules = json!([{"rule_id":"HIGH_PRIORITY_CONSENSUS","rule_version":cfg.rule_version,"matched":high_rule,"values":{"independent":s.independent,"qualified":s.qualified,"net":s.net,"age_seconds":age,"confidence":confidence.to_string()},"thresholds":{"independent":cfg.minimum_independent,"qualified":cfg.minimum_qualified,"positive_net":true,"max_age_seconds":cfg.high_max_age_seconds,"minimum_confidence":cfg.minimum_confidence.to_string()}},{"rule_id":"SMART_DISTRIBUTION","rule_version":cfg.rule_version,"matched":distribution,"values":{"wallet_exit_ratio":s.wallet_exit.to_string(),"quote_exit_ratio":s.quote_exit.to_string(),"reduce_close":s.reduce+s.close},"thresholds":{"exit_ratio":cfg.distribution_exit.to_string()}}]);
        if score >= cfg.strong {
            strong_streak += 1;
        } else {
            strong_streak = 0;
        }
        if score >= cfg.high && high_rule {
            high_streak += 1;
        } else {
            high_streak = 0;
        }
        let mut next = state.clone();
        if state == "DISTRIBUTION" && s.wallet_exit >= Decimal::ONE && !signed_positive(&s.net) {
            next = "CLOSED".into();
        } else if distribution {
            next = "DISTRIBUTION".into();
        } else {
            match state.as_str() {
                "NO_SIGNAL" if score >= cfg.watch && s.qualified > 0 => next = "WATCH".into(),
                "WATCH" if strong_streak >= 2 => next = "STRONG_WATCH".into(),
                "STRONG_WATCH" if high_streak >= 2 => next = "HIGH_PRIORITY".into(),
                "HIGH_PRIORITY"
                    if score < cfg.cooling
                        || s.wallet_exit >= cfg.cooling_exit
                        || !signed_positive(&s.net) =>
                {
                    next = "COOLING".into();
                }
                "COOLING" if strong_streak >= 2 && s.wallet_exit < cfg.cooling_exit => {
                    next = "STRONG_WATCH".into();
                }
                _ => {}
            }
        }
        let reasons = json!({"positive":[if s.qualified>0{"QUALIFIED_SMART_BUYERS"}else{"NO_QUALIFIED_BUYERS"},if signed_positive(&s.net){"POSITIVE_SMART_NET_FLOW"}else{"NON_POSITIVE_SMART_NET_FLOW"}],"hysteresis":{"strong_streak":strong_streak,"high_streak":high_streak}});
        let negatives = json!([
            if holder.is_none() {
                "HOLDER_GROWTH_UNAVAILABLE"
            } else {
                ""
            },
            if latest.is_none() {
                "MARKET_STATE_UNAVAILABLE"
            } else {
                ""
            },
            "RESEARCH_NARRATIVE_UNAVAILABLE"
        ]);
        let comp_inputs = json!({"consensus":{"qualified":s.qualified,"independent":s.independent,"net_raw":s.net,"exit_ratio":s.wallet_exit.to_string()},"market":latest,"previous_market":previous,"missing_data_policy":"RENORMALIZE_AVAILABLE_COMPONENT_WEIGHTS"});
        let comp_conf = json!({"sample_size":sample.to_string(),"market_evidence":if exact{"100"}else{"50"},"component_availability":availability.to_string()});
        let body = json!({"effective_at":at,"state":next,"score":score.to_string(),"confidence":confidence.to_string(),"components":scores,"rules":rules,"version":cfg.calculation_version});
        let index = signals.len();
        signals.push(SignalWrite{effective_at:at,state:next.clone(),score:score.to_string(),confidence:confidence.to_string(),component_scores:Value::Object(scores),component_inputs:comp_inputs,component_confidence:comp_conf,weights:json!({"configured":{"smart":cfg.weights[0],"momentum":cfg.weights[1],"capital":cfg.weights[2],"holder":cfg.weights[3],"research":cfg.weights[4]},"applied":applied,"renormalized_denominator":total.to_string(),"weight_version":cfg.weight_version}),reasons:reasons.clone(),negatives,rules:rules.clone(),hash:hash(&body),origin:origin.clone(),realtime,finality:s.finality.clone()});
        if next != state {
            transitions.push(TransitionWrite {
                signal_index: index,
                effective_at: at,
                from: state.clone(),
                to: next.clone(),
                score: score.to_string(),
                confidence: confidence.to_string(),
                reasons,
                rules,
                origin: origin.clone(),
                realtime,
            });
            state = next;
        }
    }
    Ok(SignalRebuild {
        consensus,
        signals,
        transitions,
        rule_version: cfg.rule_version,
        weight_version: cfg.weight_version,
        calculation_version: cfg.calculation_version,
    })
}

#[allow(clippy::too_many_lines)]
fn consensus_at(
    input: &SignalInput,
    at: DateTime<Utc>,
    window: i32,
    cfg: &SignalEngineConfig,
) -> Result<Summary, SignalError> {
    let start = at - ChronoDuration::seconds(i64::from(window));
    let rows: Vec<_> = input
        .trades
        .iter()
        .filter(|v| v.block_time > start && v.block_time <= at)
        .collect();
    let buys: Vec<_> = rows.iter().copied().filter(|v| v.side == "BUY").collect();
    let sells: Vec<_> = rows.iter().copied().filter(|v| v.side == "SELL").collect();
    let unique: HashSet<_> = buys.iter().map(|v| &v.wallet_address).collect();
    let qualified_rows: Vec<_> = buys
        .iter()
        .copied()
        .filter(|v| decimal(&v.identity_confidence).is_ok_and(|x| x >= cfg.minimum_identity))
        .collect();
    let qualified: HashSet<_> = qualified_rows.iter().map(|v| &v.wallet_address).collect();
    let buy = sum(&buys);
    let sell = sum(&sells);
    let net = signed(&buy, &sell);
    let mut ages: Vec<i64> = buys.iter().filter_map(|v| v.launch_age_ms).collect();
    ages.sort_unstable();
    let first = ages.first().copied();
    let median = (!ages.is_empty()).then(|| ages[ages.len() / 2]);
    let mut position_counts = HashMap::new();
    for v in input
        .positions
        .iter()
        .filter(|v| v.block_time > start && v.block_time <= at)
    {
        *position_counts
            .entry(v.event_type.as_str())
            .or_insert(0_i64) += 1;
    }
    let open = *position_counts.get("OPEN_POSITION").unwrap_or(&0);
    let add = *position_counts.get("ADD_POSITION").unwrap_or(&0);
    let reduce = *position_counts.get("REDUCE_POSITION").unwrap_or(&0);
    let close = *position_counts.get("CLOSE_POSITION").unwrap_or(&0);
    let entered: i64 = input
        .trades
        .iter()
        .filter(|v| v.block_time <= at && v.side == "BUY")
        .map(|v| &v.wallet_address)
        .collect::<HashSet<_>>()
        .len()
        .try_into()
        .unwrap_or(i64::MAX);
    let exited: i64 = input
        .positions
        .iter()
        .filter(|v| v.block_time <= at && v.event_type == "CLOSE_POSITION")
        .map(|v| &v.wallet_address)
        .collect::<HashSet<_>>()
        .len()
        .try_into()
        .unwrap_or(i64::MAX);
    let wallet_exit = ratio_i64(exited, entered);
    let quote_exit = ratio_big(&sell, &buy);
    let mut weighted = Decimal::ZERO;
    let mut timing_rows = Vec::new();
    let mut rank_rows = Vec::new();
    for v in &qualified_rows {
        let timing = timing_weight(v.launch_age_ms.unwrap_or(i64::MAX), cfg);
        let rank = rank_weight(v.buyer_rank);
        let tier = tier_weight(v.manual_tier.as_deref(), cfg);
        let identity = decimal(&v.identity_confidence)?;
        let conviction = if input.positions.iter().any(|p| {
            p.wallet_address == v.wallet_address
                && p.block_time <= at
                && p.event_type == "ADD_POSITION"
        }) {
            Decimal::new(110, 2)
        } else {
            Decimal::ONE
        };
        let contribution = tier * identity * timing * rank * conviction;
        weighted += contribution;
        timing_rows.push(json!({"smart_trade_id":v.id,"launch_age_ms":v.launch_age_ms,"weight":timing.to_string()}));
        rank_rows.push(json!({"buyer_rank":v.buyer_rank,"smart_buyer_rank":v.smart_buyer_rank,"weight":rank.to_string()}));
    }
    let finality = if rows.iter().all(|v| v.chain_status == "CONFIRMED") {
        "CONFIRMED"
    } else {
        "PENDING"
    }
    .into();
    Ok(Summary {
        raw: i64::try_from(unique.len()).unwrap_or(i64::MAX),
        qualified: i64::try_from(qualified.len()).unwrap_or(i64::MAX),
        independent: i64::try_from(qualified.len()).unwrap_or(i64::MAX),
        buy,
        sell,
        net,
        first,
        median,
        open,
        add,
        reduce,
        close,
        wallet_exit,
        quote_exit,
        weighted,
        timing: Value::Array(timing_rows),
        rank: Value::Array(rank_rows),
        position: json!({"open":open,"add":add,"reduce":reduce,"close":close,"conviction_model":"POSITION_EVENTS_V1"}),
        finality,
    })
}
fn origin(input: &SignalInput, job: &SignalJob, at: DateTime<Utc>) -> (String, bool) {
    let source = input
        .trades
        .iter()
        .filter(|v| v.block_time == at)
        .map(|v| v.classification_source.as_str())
        .next()
        .unwrap_or("MARKET_SNAPSHOT");
    let realtime =
        job.trigger_realtime_eligible && job.trigger_effective_at == Some(at) && source == "LIVE";
    (source.into(), realtime)
}
fn market_momentum(
    latest: Option<&pons_storage::repositories::SignalMarketSnapshot>,
    previous: Option<&pons_storage::repositories::SignalMarketSnapshot>,
) -> Option<Decimal> {
    let l = latest?;
    let p = previous?;
    let buyers = (l.unique_buyers - p.unique_buyers).max(0);
    let activity = (l.buy_count - l.sell_count).max(0);
    let progress = l
        .curve_progress
        .as_deref()
        .and_then(|v| decimal(v).ok())
        .unwrap_or(Decimal::ZERO);
    Some(clamp(
        Decimal::from(buyers * 15 + activity * 5) + progress * Decimal::from(50),
        Decimal::ZERO,
        Decimal::ONE_HUNDRED,
    ))
}
fn capital_score(
    latest: Option<&pons_storage::repositories::SignalMarketSnapshot>,
) -> Option<Decimal> {
    let v = latest?;
    let n = Decimal::from_str(&v.user_net_flow_raw).ok()?;
    Some(match n.cmp(&Decimal::ZERO) {
        std::cmp::Ordering::Greater => Decimal::from(75),
        std::cmp::Ordering::Less => Decimal::from(20),
        std::cmp::Ordering::Equal => Decimal::from(50),
    })
}
fn holder_score(
    latest: Option<&pons_storage::repositories::SignalMarketSnapshot>,
    previous: Option<&pons_storage::repositories::SignalMarketSnapshot>,
) -> Option<Decimal> {
    let l = latest?;
    let p = previous?;
    Some(clamp(
        Decimal::from(50 + (l.holder_count - p.holder_count) * 10),
        Decimal::ZERO,
        Decimal::ONE_HUNDRED,
    ))
}
fn timing_weight(age: i64, c: &SignalEngineConfig) -> Decimal {
    match age {
        ..=60_000 => c.timing[0],
        60_001..=180_000 => c.timing[1],
        180_001..=300_000 => c.timing[2],
        300_001..=900_000 => c.timing[3],
        _ => c.timing[4],
    }
}
fn rank_weight(rank: Option<i64>) -> Decimal {
    match rank.unwrap_or(i64::MAX) {
        ..=10 => Decimal::ONE,
        11..=50 => Decimal::new(85, 2),
        51..=200 => Decimal::new(65, 2),
        _ => Decimal::new(40, 2),
    }
}
fn tier_weight(tier: Option<&str>, c: &SignalEngineConfig) -> Decimal {
    match tier {
        Some("S") => c.tiers[0],
        Some("A") => c.tiers[1],
        Some("B") => c.tiers[2],
        Some("C") => c.tiers[3],
        _ => c.tiers[4],
    }
}
fn sum(rows: &[&SignalSmartTrade]) -> BigUint {
    rows.iter()
        .filter_map(|v| v.quote_amount_raw.parse::<BigUint>().ok())
        .sum()
}
fn signed(a: &BigUint, b: &BigUint) -> String {
    if a >= b {
        (a - b).to_string()
    } else {
        format!("-{}", b - a)
    }
}
fn signed_positive(v: &str) -> bool {
    !v.starts_with('-') && v != "0"
}
fn ratio_big(a: &BigUint, b: &BigUint) -> Decimal {
    if b == &BigUint::from(0_u8) {
        return Decimal::ZERO;
    }
    let scaled = a * BigUint::from(1_000_000_u32) / b;
    Decimal::from_str(&scaled.to_string()).unwrap_or(Decimal::ZERO) / Decimal::from(1_000_000)
}
fn ratio_i64(a: i64, b: i64) -> Decimal {
    if b <= 0 {
        Decimal::ZERO
    } else {
        Decimal::from(a) / Decimal::from(b)
    }
}
fn decimal(v: &str) -> Result<Decimal, SignalError> {
    Decimal::from_str(v).map_err(|e| SignalError::Invalid(e.to_string()))
}
fn clamp(v: Decimal, min: Decimal, max: Decimal) -> Decimal {
    v.max(min).min(max)
}
fn hash(v: &Value) -> [u8; 32] {
    Sha256::digest(serde_json::to_vec(v).expect("JSON value serializes")).into()
}
#[allow(clippy::needless_pass_by_value)]
fn storage(e: sqlx::Error) -> SignalError {
    SignalError::Storage(e.to_string())
}
