use chrono::{Duration, TimeZone, Utc};
use pons_storage::repositories::{BacktestRepository, NewBacktestExperiment};
use serde_json::json;
use sqlx::PgPool;

async fn token(pool: &PgPool, seed: u8, launch: chrono::DateTime<Utc>) -> uuid::Uuid {
    sqlx::query_scalar("INSERT INTO tokens(chain_id,address,curve_address,deployer,launch_block,launch_time,lifecycle,created_at)VALUES(4663,$1,$2,$3,1,$4,'ACTIVE_CURVE',$4)RETURNING id")
        .bind(vec![seed; 20]).bind(vec![seed.wrapping_add(1); 20]).bind(vec![seed.wrapping_add(2); 20]).bind(launch)
        .fetch_one(pool).await.unwrap()
}

async fn signal(
    pool: &PgPool,
    token_id: uuid::Uuid,
    state: &str,
    at: chrono::DateTime<Utc>,
    seed: u8,
) {
    sqlx::query("INSERT INTO signal_snapshots(token_id,rebuild_generation,effective_at,state,score,confidence,component_scores,component_inputs,component_confidence,applied_weights,reason_codes,negative_reasons,matched_rules,rule_version,weight_version,calculation_version,content_hash,classification_origin,realtime_alert_eligible,signal_finality,calculated_at)VALUES($1,1,$2,$3,88,90,'{}','{}','{}','{}','[]','[]','[]',1,1,1,$4,'LIVE',true,'CONFIRMED',$2)")
        .bind(token_id).bind(at).bind(state).bind([seed; 32].as_slice()).execute(pool).await.unwrap();
}

async fn market_snapshot(
    pool: &PgPool,
    token_id: uuid::Uuid,
    at: chrono::DateTime<Utc>,
    block: i64,
    price: i64,
) {
    sqlx::query("INSERT INTO token_market_snapshots(token_id,snapshot_kind,snapshot_at,snapshot_block,age_since_launch_ms,buy_count,sell_count,unique_buyers,unique_sellers,user_buy_volume_raw,user_sell_volume_raw,user_net_flow_raw,curve_effective_in_raw,curve_effective_out_raw,curve_effective_net_flow_raw,smart_unique_buyers,smart_unique_sellers,smart_buy_quote_raw,smart_sell_quote_raw,smart_net_flow_raw,holder_count,calculation_version,state_exact,state_scope,evidence,spot_price_quote,created_at)VALUES($1,$2,$3,$4,0,0,0,0,0,'0','0',0,'0','0',0,0,0,'0','0',0,0,1,true,'BLOCK_STATE_EXACT','{}',$5,$3)")
        .bind(token_id).bind(format!("TEST-{block}")).bind(at).bind(block).bind(price).execute(pool).await.unwrap();
}

fn config(mode: &str) -> NewBacktestExperiment {
    let start = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    NewBacktestExperiment {
        name: format!("{mode} validation"),
        knowledge_mode: mode.into(),
        dataset_start: start,
        dataset_end: start + Duration::days(40),
        train_start: start,
        train_end: start + Duration::days(20),
        validation_start: start + Duration::days(20),
        validation_end: start + Duration::days(40),
        feature_set: json!({"version":1}),
        signal_rule_version: 1,
        score_calculation_version: 1,
        weights: json!({}),
        thresholds: json!({}),
        bucket_definitions: json!({"version":1}),
        outcome_definition: json!({"horizons":[300,900,3600,21600,86400]}),
        minimum_sample_size: 2,
        number_of_trials: 1,
        selection_criteria: "predeclared".into(),
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn knowledge_time_decisions_exclude_late_content_ai_and_preserve_production(pool: PgPool) {
    let admin: uuid::Uuid = sqlx::query_scalar(
        "INSERT INTO users(username,password_hash)VALUES('backtest-admin','argon2id')RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let launch = Utc.timestamp_opt(1_700_000_100, 0).unwrap();
    let known = launch + Duration::minutes(1);
    let late = known + Duration::days(2);
    let token:uuid::Uuid=sqlx::query_scalar("INSERT INTO tokens(chain_id,address,curve_address,deployer,launch_block,launch_time,lifecycle,created_at)VALUES(4663,$1,$2,$3,1,$4,'ACTIVE_CURVE',$4)RETURNING id").bind(vec![91_u8;20]).bind(vec![92_u8;20]).bind(vec![93_u8;20]).bind(launch).fetch_one(&pool).await.unwrap();
    let signal:uuid::Uuid=sqlx::query_scalar("INSERT INTO signal_snapshots(token_id,rebuild_generation,effective_at,state,score,confidence,component_scores,component_inputs,component_confidence,applied_weights,reason_codes,negative_reasons,matched_rules,rule_version,weight_version,calculation_version,content_hash,classification_origin,realtime_alert_eligible,signal_finality,calculated_at)VALUES($1,1,$2,'HIGH_PRIORITY',88,90,'{}','{}','{}','{}','[]','[]','[]',1,1,1,$3,'LIVE',true,'CONFIRMED',$4)RETURNING id").bind(token).bind(launch+Duration::seconds(30)).bind([3_u8;32].as_slice()).bind(known).fetch_one(&pool).await.unwrap();
    sqlx::query("INSERT INTO current_signal_states(token_id,state,score,confidence,effective_at,rebuild_generation,signal_snapshot_id)VALUES($1,'HIGH_PRIORITY',88,90,$2,1,$3)").bind(token).bind(launch+Duration::seconds(30)).bind(signal).execute(&pool).await.unwrap();
    let trader: uuid::Uuid =
        sqlx::query_scalar("INSERT INTO traders(handle)VALUES('late-content-trader')RETURNING id")
            .fetch_one(&pool)
            .await
            .unwrap();
    let content:uuid::Uuid=sqlx::query_scalar("INSERT INTO trader_content_items(trader_id,provider_id,platform,content_type,published_at,observed_at,content_hash,provenance,authorization_basis)VALUES($1,(SELECT id FROM content_providers WHERE provider_key='manual-reference'),'FOMO','TRADE_THESIS',$2,$3,$4,'{}','MANUAL_REFERENCE')RETURNING id").bind(trader).bind(launch).bind(late).bind([4_u8;32].as_slice()).fetch_one(&pool).await.unwrap();
    sqlx::query("INSERT INTO token_content_links(content_id,token_id,relation_type,confidence,evidence)VALUES($1,$2,'MANUAL_LINK',1,'{}')").bind(content).bind(token).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO ai_research_reports(token_id,provider,model,report_version,prompt_version,prompt_schema_version,input_schema_version,output_schema_version,research_mode,knowledge_cutoff,evidence_generated_at,evidence_hash,input_hash,structured_report,category,summary,confidence,trigger_type,trigger_origin,created_at)VALUES($1,'MOCK','v1',1,1,1,1,1,'KNOWLEDGE_TIME',$2,$2,$3,$4,'{}','UNKNOWN','late report',10,'MANUAL','HISTORICAL_VALIDATION',$2)").bind(token).bind(late).bind([5_u8;32].as_slice()).bind([6_u8;32].as_slice()).execute(&pool).await.unwrap();
    let repo = BacktestRepository::new(pool.clone());
    let cfg = config("KNOWLEDGE_TIME");
    let experiment = repo.create(&cfg, admin, "0.1.0", "test", 1).await.unwrap();
    assert_eq!(
        repo.create(&cfg, admin, "0.1.0", "test", 1).await.unwrap(),
        experiment
    );
    let run = repo.enqueue(experiment).await.unwrap();
    let job = repo.claim_due().await.unwrap().unwrap();
    assert_eq!(job.id, run);
    repo.execute(&job).await.unwrap();
    let decision:(chrono::DateTime<Utc>,chrono::DateTime<Utc>)=sqlx::query_as("SELECT event_effective_at,decision_as_of FROM historical_decision_points WHERE run_id=$1 AND cohort='HIGH_PRIORITY'").bind(run).fetch_one(&pool).await.unwrap();
    assert_eq!(decision.1, known);
    assert!(decision.1 > decision.0);
    let factors: serde_json::Value =
        sqlx::query_scalar("SELECT factor_result FROM backtest_experiment_runs WHERE id=$1")
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(factors["ai_incremental"]["historically_known_reports"], 0);
    assert_eq!(factors["content"]["known_content_items"], 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*)FROM current_signal_states WHERE token_id=$1")
            .bind(token)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert!(
        sqlx::query("UPDATE backtest_experiments SET name='mutated'WHERE id=$1")
            .bind(experiment)
            .execute(&pool)
            .await
            .is_err()
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn reconstructed_mode_is_explicit_research_only_and_jobs_recover(pool: PgPool) {
    let admin = sqlx::query_scalar(
        "INSERT INTO users(username,password_hash)VALUES('research-admin','argon2id')RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let repo = BacktestRepository::new(pool.clone());
    let id = repo
        .create(
            &config("EVENT_TIME_RECONSTRUCTED"),
            admin,
            "0.1.0",
            "test",
            1,
        )
        .await
        .unwrap();
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT research_only FROM backtest_experiments WHERE id=$1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap()
    );
    let run = repo.enqueue(id).await.unwrap();
    sqlx::query("UPDATE backtest_experiment_runs SET status='RUNNING',locked_at=now()-interval'10 minutes'WHERE id=$1").bind(run).execute(&pool).await.unwrap();
    assert_eq!(repo.claim_due().await.unwrap().unwrap().id, run);
}

#[sqlx::test(migrations = "../../migrations")]
async fn unique_state_samples_decision_outcomes_matched_baseline_and_token_split(pool: PgPool) {
    let admin: uuid::Uuid = sqlx::query_scalar(
        "INSERT INTO users(username,password_hash)VALUES('sample-admin','argon2id')RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let start = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let launch = start + Duration::days(1);
    let signaled = token(&pool, 101, launch).await;
    let ordinary = token(&pool, 111, launch + Duration::minutes(10)).await;
    let censored = token(&pool, 121, launch + Duration::minutes(20)).await;
    let validation = token(&pool, 131, start + Duration::days(25)).await;
    signal(&pool, signaled, "WATCH", launch + Duration::minutes(1), 11).await;
    signal(
        &pool,
        signaled,
        "STRONG_WATCH",
        launch + Duration::minutes(3),
        12,
    )
    .await;
    signal(
        &pool,
        signaled,
        "HIGH_PRIORITY",
        launch + Duration::minutes(5),
        13,
    )
    .await;
    signal(
        &pool,
        signaled,
        "HIGH_PRIORITY",
        launch + Duration::minutes(6),
        14,
    )
    .await;
    signal(
        &pool,
        signaled,
        "HIGH_PRIORITY",
        launch + Duration::minutes(7),
        15,
    )
    .await;
    signal(
        &pool,
        validation,
        "WATCH",
        start + Duration::days(25) + Duration::minutes(1),
        16,
    )
    .await;
    market_snapshot(&pool, signaled, launch + Duration::minutes(5), 100, 100).await;
    market_snapshot(&pool, signaled, launch + Duration::minutes(60), 101, 50).await;
    market_snapshot(&pool, signaled, launch + Duration::minutes(65), 102, 200).await;
    sqlx::query("UPDATE tokens SET lifecycle='GRADUATED',updated_at=$2 WHERE id=$1")
        .bind(censored)
        .bind(launch + Duration::minutes(30))
        .execute(&pool)
        .await
        .unwrap();

    let repo = BacktestRepository::new(pool.clone());
    let experiment = repo
        .create(&config("KNOWLEDGE_TIME"), admin, "0.1.0", "sample", 1)
        .await
        .unwrap();
    let run = repo.enqueue(experiment).await.unwrap();
    let job = repo.claim_due().await.unwrap().unwrap();
    repo.execute(&job).await.unwrap();

    for cohort in ["WATCH", "STRONG_WATCH", "HIGH_PRIORITY"] {
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*)FROM historical_decision_points WHERE run_id=$1 AND token_id=$2 AND cohort=$3").bind(run).bind(signaled).bind(cohort).fetch_one(&pool).await.unwrap(),1);
    }
    let high:(chrono::DateTime<Utc>,i64,String,String)=sqlx::query_as("SELECT decision_at,launch_age_ms,outcome_anchor,sample_identity FROM historical_decision_points WHERE run_id=$1 AND token_id=$2 AND cohort='HIGH_PRIORITY'").bind(run).bind(signaled).fetch_one(&pool).await.unwrap();
    assert_eq!(high.0, launch + Duration::minutes(5));
    assert_eq!(high.1, 300_000);
    assert_eq!(high.2, "DECISION_TIME");
    assert!(high.3.contains("HIGH_PRIORITY"));
    let result: serde_json::Value =
        sqlx::query_scalar("SELECT train_result FROM backtest_experiment_runs WHERE id=$1")
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap();
    let factors: serde_json::Value =
        sqlx::query_scalar("SELECT factor_result FROM backtest_experiment_runs WHERE id=$1")
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(factors["sample_unit"], "TOKEN_X_EXPERIMENT_DECISION_POINT");
    assert_eq!(factors["repeated_measures"], false);
    let one_hour = result
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["cohort"] == "HIGH_PRIORITY" && v["horizon_seconds"] == 3600)
        .unwrap();
    assert_eq!(one_hour["p50_median_return_proxy"], "100.00000000");
    assert!(sqlx::query_scalar::<_,i64>("SELECT count(*)FROM historical_decision_points WHERE run_id=$1 AND token_id=$2 AND cohort='ALL_PONS_LAUNCHES'").bind(run).bind(ordinary).fetch_one(&pool).await.unwrap()>0);
    assert!(sqlx::query_scalar::<_,i64>("SELECT count(*)FROM historical_decision_points WHERE run_id=$1 AND cohort='AGE_MATCHED_BASELINE'AND matched_cohort='HIGH_PRIORITY'AND age_bucket='5-15m'AND launch_age_ms=300000").bind(run).fetch_one(&pool).await.unwrap()>0);
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*)FROM(SELECT token_id FROM historical_decision_points WHERE run_id=$1 GROUP BY token_id HAVING count(DISTINCT split)>1)x").bind(run).fetch_one(&pool).await.unwrap(),0);
    assert!(sqlx::query_scalar::<_,i64>("SELECT count(*)FROM historical_decision_points WHERE run_id=$1 AND token_id=$2 AND split='OUT_OF_SAMPLE'").bind(run).bind(validation).fetch_one(&pool).await.unwrap()>0);
    let experiment_semantics:(String,String,serde_json::Value,Vec<u8>)=sqlx::query_as("SELECT sample_unit,outcome_anchor,baseline_definition,input_hash FROM backtest_experiments WHERE id=$1").bind(experiment).fetch_one(&pool).await.unwrap();
    assert_eq!(experiment_semantics.0, "UNIQUE_TOKEN_FIRST_STATE_ENTRY");
    assert_eq!(experiment_semantics.1, "DECISION_TIME");
    assert_eq!(experiment_semantics.2["version"], 1);
    assert_eq!(experiment_semantics.3.len(), 32);
    let censored_metrics = result
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["cohort"] == "ALL_PONS_LAUNCHES" && v["horizon_seconds"] == 3600)
        .unwrap();
    assert!(censored_metrics["censored_count"].as_i64().unwrap() >= 1);
}
