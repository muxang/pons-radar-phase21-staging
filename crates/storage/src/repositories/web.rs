#![allow(clippy::needless_raw_string_hashes)]

use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct WebRepository {
    pool: PgPool,
}

#[derive(Clone, Debug, Default)]
pub struct TokenListQuery<'a> {
    pub search: Option<&'a str>,
    pub signal: Option<&'a str>,
    pub lifecycle: Option<&'a str>,
    pub has_smart_money: Option<bool>,
    pub min_progress: Option<&'a str>,
    pub max_age_seconds: Option<i64>,
    pub sort: &'a str,
    pub descending: bool,
    pub limit: i64,
    pub offset: i64,
}

#[allow(clippy::missing_errors_doc, clippy::too_many_lines)]
impl WebRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn dashboard(&self) -> Result<Value, sqlx::Error> {
        let high = self.signal_tokens("HIGH_PRIORITY", 12).await?;
        let strong = self.signal_tokens("STRONG_WATCH", 12).await?;
        let launches: Vec<Value> = sqlx::query_scalar(r#"SELECT jsonb_build_object('id',t.id,'address','0x'||encode(t.address,'hex'),'curve_address','0x'||encode(t.curve_address,'hex'),'launch_time',t.launch_time,'lifecycle',t.lifecycle,'name',m.name,'symbol',m.symbol,'logo_uri',m.token_logo) FROM tokens t LEFT JOIN token_metadata_current m ON m.token_id=t.id ORDER BY t.launch_time DESC NULLS LAST,t.id LIMIT 12"#).fetch_all(&self.pool).await?;
        let smart: Vec<Value> = sqlx::query_scalar(r#"SELECT jsonb_build_object('id',s.id,'token_id',s.token_id,'trader_id',s.trader_id,'handle',r.handle,'symbol',m.symbol,'side',s.side,'token_amount_raw',s.token_amount_raw,'quote_amount_raw',s.quote_amount_raw,'block_time',s.block_time,'classification_source',s.classification_source,'realtime_alert_eligible',s.realtime_alert_eligible,'chain_finality',tt.status) FROM smart_trades s JOIN traders r ON r.id=s.trader_id JOIN token_trades tt ON tt.id=s.token_trade_id LEFT JOIN token_metadata_current m ON m.token_id=s.token_id WHERE s.confirmation_level IN('BUY_CONFIRMED','SELL_CONFIRMED') ORDER BY s.block_time DESC,s.block_number DESC,s.log_index DESC LIMIT 30"#).fetch_all(&self.pool).await?;
        Ok(
            json!({"high_priority":high,"strong_watch":strong,"launches":launches,"recent_smart_money":smart}),
        )
    }

    async fn signal_tokens(&self, state: &str, limit: i64) -> Result<Vec<Value>, sqlx::Error> {
        sqlx::query_scalar(r#"SELECT jsonb_build_object('token_id',t.id,'address','0x'||encode(t.address,'hex'),'name',m.name,'symbol',m.symbol,'launch_time',t.launch_time,'lifecycle',t.lifecycle,'state',s.state,'score',s.score::text,'confidence',s.confidence::text,'effective_at',s.effective_at,'provisional',ss.signal_finality='PENDING') FROM current_signal_states s JOIN tokens t ON t.id=s.token_id LEFT JOIN token_metadata_current m ON m.token_id=t.id JOIN signal_snapshots ss ON ss.id=s.signal_snapshot_id WHERE s.state=$1 ORDER BY s.score DESC,s.effective_at DESC LIMIT $2"#).bind(state).bind(limit).fetch_all(&self.pool).await
    }

    pub async fn tokens(&self, q: &TokenListQuery<'_>) -> Result<Value, sqlx::Error> {
        let min_progress = q
            .min_progress
            .and_then(|value| value.parse::<rust_decimal::Decimal>().ok());
        let order = match q.sort {
            "score" => "COALESCE(cs.score,-1)",
            "progress" => "COALESCE(obs.curve_progress,-1)",
            "buyers" => "COALESCE(ms.unique_buyers,0)",
            "holders" => "COALESCE(ms.raw_holder_count,0)",
            _ => "t.launch_time",
        };
        let direction = if q.descending { "DESC" } else { "ASC" };
        let sql = format!(
            r#"SELECT jsonb_build_object('id',t.id,'address','0x'||encode(t.address,'hex'),'curve_address','0x'||encode(t.curve_address,'hex'),'name',m.name,'symbol',m.symbol,'logo_uri',m.token_logo,'launch_time',t.launch_time,'lifecycle',t.lifecycle,'curve_progress',obs.curve_progress::text,'spot_price_quote',obs.spot_price_quote::text,'curve_implied_fdv_quote',obs.curve_implied_fdv_quote::text,'state_scope',CASE WHEN obs.state_exact THEN 'BLOCK_STATE_EXACT' ELSE 'UNAVAILABLE' END,'unique_buyers',COALESCE(ms.unique_buyers,0),'unique_sellers',COALESCE(ms.unique_sellers,0),'holders',COALESCE(ms.raw_holder_count,0),'user_net_flow_raw',ms.user_net_flow_raw::text,'smart_buyers',COALESCE(ms.smart_unique_buyers,0),'smart_net_flow_raw',ms.smart_net_flow_raw::text,'signal_state',COALESCE(cs.state,'NO_SIGNAL'),'score',cs.score::text,'confidence',cs.confidence::text,'provisional',ss.signal_finality='PENDING') FROM tokens t LEFT JOIN token_metadata_current m ON m.token_id=t.id LEFT JOIN token_market_state ms ON ms.token_id=t.id LEFT JOIN LATERAL(SELECT * FROM curve_state_observations o WHERE o.token_id=t.id ORDER BY block_number DESC LIMIT 1)obs ON true LEFT JOIN current_signal_states cs ON cs.token_id=t.id LEFT JOIN signal_snapshots ss ON ss.id=cs.signal_snapshot_id WHERE ($1::text IS NULL OR m.symbol ILIKE '%'||$1||'%' OR m.name ILIKE '%'||$1||'%' OR '0x'||encode(t.address,'hex') ILIKE '%'||$1||'%') AND ($2::text IS NULL OR cs.state=$2) AND ($3::text IS NULL OR t.lifecycle=$3) AND ($4::boolean IS NULL OR (COALESCE(ms.smart_unique_buyers,0)>0)=$4) AND ($5::numeric IS NULL OR obs.curve_progress >= $5) AND ($6::bigint IS NULL OR t.launch_time >= now()-make_interval(secs=>$6)) ORDER BY {order} {direction} NULLS LAST,t.id LIMIT $7 OFFSET $8"#
        );
        let items: Vec<Value> = sqlx::query_scalar(&sql)
            .bind(q.search)
            .bind(q.signal)
            .bind(q.lifecycle)
            .bind(q.has_smart_money)
            .bind(min_progress)
            .bind(q.max_age_seconds)
            .bind(q.limit)
            .bind(q.offset)
            .fetch_all(&self.pool)
            .await?;
        let total:i64=sqlx::query_scalar("SELECT count(*) FROM tokens t LEFT JOIN token_metadata_current m ON m.token_id=t.id LEFT JOIN token_market_state ms ON ms.token_id=t.id LEFT JOIN current_signal_states cs ON cs.token_id=t.id LEFT JOIN LATERAL(SELECT curve_progress FROM curve_state_observations o WHERE o.token_id=t.id ORDER BY block_number DESC LIMIT 1)obs ON true WHERE ($1::text IS NULL OR m.symbol ILIKE '%'||$1||'%' OR m.name ILIKE '%'||$1||'%' OR '0x'||encode(t.address,'hex') ILIKE '%'||$1||'%') AND ($2::text IS NULL OR cs.state=$2) AND ($3::text IS NULL OR t.lifecycle=$3) AND ($4::boolean IS NULL OR (COALESCE(ms.smart_unique_buyers,0)>0)=$4) AND ($5::numeric IS NULL OR obs.curve_progress >= $5) AND ($6::bigint IS NULL OR t.launch_time >= now()-make_interval(secs=>$6))").bind(q.search).bind(q.signal).bind(q.lifecycle).bind(q.has_smart_money).bind(min_progress).bind(q.max_age_seconds).fetch_one(&self.pool).await?;
        Ok(
            json!({"items":items,"total":total,"limit":q.limit,"offset":q.offset,"next_offset":if q.offset+q.limit<total{Some(q.offset+q.limit)}else{None}}),
        )
    }

    pub async fn token(&self, address: &[u8]) -> Result<Option<Value>, sqlx::Error> {
        sqlx::query_scalar(r#"SELECT jsonb_build_object('id',t.id,'chain_id',t.chain_id::text,'address','0x'||encode(t.address,'hex'),'curve_address','0x'||encode(t.curve_address,'hex'),'factory_address','0x'||encode(t.factory_address,'hex'),'deployer','0x'||encode(t.deployer,'hex'),'pair_token','0x'||encode(t.pair_token,'hex'),'launch_tx','0x'||encode(t.launch_tx,'hex'),'launch_block',t.launch_block::text,'launch_log_index',t.launch_log_index::text,'launch_time',t.launch_time,'lifecycle',t.lifecycle,'metadata',CASE WHEN m.token_id IS NULL THEN NULL ELSE jsonb_build_object('name',m.name,'symbol',m.symbol,'decimals',m.decimals,'total_supply_raw',m.total_supply_raw,'logo_uri',m.token_logo,'description',m.token_description,'twitter',m.twitter,'telegram',m.telegram,'discord',m.discord,'website',m.website,'farcaster',m.farcaster,'normalized_socials',m.normalized_socials,'integrity_warning',m.integrity_warning,'observed_block',m.observed_block::text,'observed_at',m.observed_at)END,'market',CASE WHEN ms.token_id IS NULL THEN NULL ELSE to_jsonb(ms)-'token_id' END,'curve_state',CASE WHEN obs.token_id IS NULL THEN NULL ELSE jsonb_build_object('block_number',obs.block_number::text,'sellable_tokens_raw',obs.sellable_tokens_raw,'reserved_tokens_raw',obs.reserved_tokens_raw,'real_quote_reserve_raw',obs.real_quote_reserve_raw,'graduation_threshold_raw',obs.graduation_threshold_raw,'curve_progress',obs.curve_progress::text,'spot_price_quote',obs.spot_price_quote::text,'curve_implied_fdv_quote',obs.curve_implied_fdv_quote::text,'evidence_scope',CASE WHEN obs.state_exact THEN 'BLOCK_STATE_EXACT' ELSE 'UNAVAILABLE' END,'integrity_warning',obs.integrity_warning,'evidence',obs.evidence)END,'signal',CASE WHEN ss.id IS NULL THEN NULL ELSE jsonb_build_object('state',ss.state,'score',ss.score::text,'confidence',ss.confidence::text,'component_scores',ss.component_scores,'component_inputs',ss.component_inputs,'component_confidence',ss.component_confidence,'applied_weights',ss.applied_weights,'reason_codes',ss.reason_codes,'negative_reasons',ss.negative_reasons,'matched_rules',ss.matched_rules,'rule_version',ss.rule_version,'weight_version',ss.weight_version,'calculation_version',ss.calculation_version,'finality',ss.signal_finality,'effective_at',ss.effective_at)END) FROM tokens t LEFT JOIN token_metadata_current m ON m.token_id=t.id LEFT JOIN token_market_state ms ON ms.token_id=t.id LEFT JOIN LATERAL(SELECT * FROM curve_state_observations o WHERE o.token_id=t.id ORDER BY block_number DESC LIMIT 1)obs ON true LEFT JOIN current_signal_states cs ON cs.token_id=t.id LEFT JOIN signal_snapshots ss ON ss.id=cs.signal_snapshot_id WHERE t.chain_id=4663 AND t.address=$1"#).bind(address).fetch_optional(&self.pool).await
    }

    pub async fn timeline(
        &self,
        token: Uuid,
        before: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<Value, sqlx::Error> {
        let items:Vec<Value>=sqlx::query_scalar(r#"WITH timeline AS (
SELECT t.launch_time effective_at,'TOKEN_LAUNCHED' kind,t.id source_id,'LIVE' source,true realtime,jsonb_build_object('tx_hash','0x'||encode(t.launch_tx,'hex'),'block',t.launch_block::text,'log_index',t.launch_log_index::text) data FROM tokens t WHERE t.id=$1
UNION ALL SELECT s.block_time,CASE WHEN s.side='BUY'THEN 'SMART_BUY'ELSE'SMART_SELL'END,s.id,s.classification_source,s.realtime_alert_eligible,jsonb_build_object('trader_id',s.trader_id,'wallet','0x'||encode(s.wallet_address,'hex'),'amount_raw',s.token_amount_raw,'quote_raw',s.quote_amount_raw,'tx_hash','0x'||encode(s.tx_hash,'hex'),'confirmation',s.confirmation_level,'chain_finality',tt.status,'evidence',s.evidence) FROM smart_trades s JOIN token_trades tt ON tt.id=s.token_trade_id WHERE s.token_id=$1
UNION ALL SELECT p.block_time,p.event_type,p.id,p.classification_source,p.classification_source='LIVE',jsonb_build_object('smart_trade_id',p.smart_trade_id,'amount_raw',p.token_amount_raw,'balance_after_raw',p.balance_after_raw,'block',p.block_number::text,'log_index',p.log_index::text) FROM position_events p JOIN wallet_token_positions w ON w.id=p.position_id WHERE w.token_id=$1
UNION ALL SELECT x.effective_at,'SIGNAL_'||x.to_state,x.id,x.classification_origin,x.realtime_alert_eligible,jsonb_build_object('from',x.from_state,'to',x.to_state,'score',x.score::text,'confidence',x.confidence::text,'reasons',x.reason_codes,'rules',x.matched_rules) FROM signal_transitions x WHERE x.token_id=$1 AND x.current_generation
UNION ALL SELECT m.observed_at,'METADATA_CHANGED',m.id,'METADATA',false,jsonb_build_object('observed_block',m.observed_block::text,'integrity_warning',m.integrity_warning) FROM token_metadata_snapshots m WHERE m.token_id=$1
UNION ALL SELECT c.published_at,'TRADER_CONTENT',c.id,'CONTENT_IMPORT',c.realtime_alert_eligible,jsonb_build_object('trader_id',c.trader_id,'trader_handle',r.handle,'content_type',c.content_type,'title',c.title,'summary',c.summary,'stance',c.stance,'narratives',c.narratives,'external_reference',c.external_reference,'observed_at',c.observed_at,'observed_later',c.observed_at>c.published_at+interval '1 minute','relations',COALESCE((SELECT jsonb_agg(jsonb_build_object('relation_type',x.relation_type,'delta_ms',x.delta_ms,'position_event_id',x.position_event_id)ORDER BY x.trade_event_time NULLS LAST)FROM content_trade_relations x WHERE x.content_id=c.id AND x.token_id=$1),'[]'))FROM token_content_links l JOIN trader_content_items c ON c.id=l.content_id LEFT JOIN traders r ON r.id=c.trader_id WHERE l.token_id=$1
UNION ALL SELECT a.knowledge_cutoff,'AI_RESEARCH_COMPLETED',a.id,'AI_RESEARCH',false,jsonb_build_object('category',a.category,'summary',a.summary,'confidence',a.confidence,'provider',a.provider,'model',a.model,'knowledge_cutoff',a.knowledge_cutoff,'prompt_version',a.prompt_version,'evidence_hash',encode(a.evidence_hash,'hex'))FROM ai_research_reports a WHERE a.token_id=$1 AND a.status='COMPLETED')
SELECT jsonb_build_object('id',source_id,'type',kind,'event_effective_at',effective_at,'classification_source',source,'realtime_alert_eligible',realtime,'historical',source LIKE '%BACKFILL%' OR NOT realtime,'data',data) FROM timeline WHERE effective_at IS NOT NULL AND ($2::timestamptz IS NULL OR effective_at<$2) ORDER BY effective_at DESC,source_id DESC LIMIT $3"#).bind(token).bind(before).bind(limit).fetch_all(&self.pool).await?;
        Ok(
            json!({"items":items,"next_before":items.last().and_then(|v|v.get("event_effective_at")).cloned()}),
        )
    }

    pub async fn smart_money(
        &self,
        token: Uuid,
        limit: i64,
        offset: i64,
    ) -> Result<Value, sqlx::Error> {
        let traders:Vec<Value>=sqlx::query_scalar(r#"SELECT jsonb_build_object('position_id',p.id,'trader_id',p.trader_id,'handle',r.handle,'display_name',r.display_name,'manual_tier',r.manual_tier,'wallet_id',p.trader_wallet_id,'wallet','0x'||encode(p.wallet_address,'hex'),'identity_confidence',w.identity_confidence::text,'position_state',CASE WHEN p.open THEN 'OPEN' ELSE 'CLOSED' END,'position_basis',p.position_basis,'balance_raw',p.balance_raw,'total_buy_raw',p.total_quote_in_raw,'total_sell_raw',p.total_quote_out_raw,'first_entry_at',p.first_entry_at,'entry_execution_price',s.entry_price_quote::text,'buyer_rank',s.buyer_rank,'smart_buyer_rank',s.smart_buyer_rank,'entry_evidence_scope',s.execution_price_scope,'integrity_status',p.integrity_status) FROM wallet_token_positions p JOIN traders r ON r.id=p.trader_id JOIN trader_wallets w ON w.id=p.trader_wallet_id LEFT JOIN LATERAL(SELECT * FROM smart_trades s WHERE s.token_id=p.token_id AND s.trader_wallet_id=p.trader_wallet_id AND s.side='BUY' ORDER BY block_number,log_index LIMIT 1)s ON true WHERE p.token_id=$1 ORDER BY p.first_entry_at,p.id LIMIT $2 OFFSET $3"#).bind(token).bind(limit).bind(offset).fetch_all(&self.pool).await?;
        let events:Vec<Value>=sqlx::query_scalar(r#"SELECT jsonb_build_object('id',p.id,'trader_id',w.trader_id,'wallet','0x'||encode(w.wallet_address,'hex'),'type',p.event_type,'amount_raw',p.token_amount_raw,'quote_raw',p.quote_amount_raw,'balance_before_raw',p.balance_before_raw,'balance_after_raw',p.balance_after_raw,'event_effective_at',p.block_time,'classification_source',p.classification_source) FROM position_events p JOIN wallet_token_positions w ON w.id=p.position_id WHERE w.token_id=$1 ORDER BY p.block_time DESC,p.block_number DESC,p.log_index DESC LIMIT 200"#).bind(token).fetch_all(&self.pool).await?;
        Ok(
            json!({"items":traders,"position_events":events,"basis":"PONS_V2_CONFIRMED_TRADES","limit":limit,"offset":offset}),
        )
    }

    pub async fn snapshots(
        &self,
        token: Uuid,
        since: DateTime<Utc>,
        limit: i64,
    ) -> Result<Vec<Value>, sqlx::Error> {
        sqlx::query_scalar(r#"SELECT jsonb_build_object('id',m.id,'snapshot_kind',m.snapshot_kind,'snapshot_at',m.snapshot_at,'snapshot_block',m.snapshot_block::text,'curve_progress',m.curve_progress::text,'unique_buyers',m.unique_buyers,'unique_sellers',m.unique_sellers,'holder_count',m.holder_count,'user_net_flow_raw',m.user_net_flow_raw::text,'smart_unique_buyers',m.smart_unique_buyers,'smart_net_flow_raw',m.smart_net_flow_raw::text,'spot_price_quote',m.spot_price_quote::text,'curve_implied_fdv_quote',m.curve_implied_fdv_quote::text,'evidence_scope',CASE WHEN m.state_exact THEN 'BLOCK_STATE_EXACT' ELSE 'UNAVAILABLE' END,'opportunity_score',s.score::text,'confidence',s.confidence::text) FROM token_market_snapshots m LEFT JOIN LATERAL(SELECT score,confidence FROM signal_snapshots s WHERE s.token_id=m.token_id AND s.current_generation AND s.effective_at<=m.snapshot_at ORDER BY effective_at DESC LIMIT 1)s ON true WHERE m.token_id=$1 AND m.snapshot_at >= $2 ORDER BY m.snapshot_at LIMIT $3"#).bind(token).bind(since).bind(limit).fetch_all(&self.pool).await
    }

    pub async fn research(&self, token: Uuid) -> Result<Value, sqlx::Error> {
        let original:Option<Value>=sqlx::query_scalar(r#"SELECT jsonb_build_object('name',name,'symbol',symbol,'logo_uri',token_logo,'description',token_description,'twitter',twitter,'telegram',telegram,'discord',discord,'website',website,'farcaster',farcaster,'deployer_matches_launch',deployer_matches_launch,'integrity_warning',integrity_warning,'observed_block',observed_block::text,'observed_at',observed_at,'capture_mode',capture_mode,'exact_launch_snapshot',exact_launch_snapshot,'requested_block',requested_block::text) FROM token_metadata_original WHERE token_id=$1"#).bind(token).fetch_optional(&self.pool).await?;
        let current:Option<Value>=sqlx::query_scalar(r#"SELECT jsonb_build_object('name',name,'symbol',symbol,'logo_uri',token_logo,'description',token_description,'twitter',twitter,'telegram',telegram,'discord',discord,'website',website,'farcaster',farcaster,'normalized_socials',normalized_socials,'deployer_matches_launch',deployer_matches_launch,'integrity_warning',integrity_warning,'observed_block',observed_block::text,'observed_at',observed_at,'capture_mode',capture_mode,'exact_launch_snapshot',exact_launch_snapshot,'requested_block',requested_block::text) FROM token_metadata_current WHERE token_id=$1"#).bind(token).fetch_optional(&self.pool).await?;
        let history:Vec<Value>=sqlx::query_scalar(r#"SELECT jsonb_build_object('id',id,'metadata',metadata,'deployer_matches_launch',deployer_matches_launch,'integrity_warning',integrity_warning,'observed_block',observed_block::text,'observed_at',observed_at,'capture_mode',capture_mode,'exact_launch_snapshot',exact_launch_snapshot,'requested_block',requested_block::text) FROM token_metadata_snapshots WHERE token_id=$1 ORDER BY observed_at DESC LIMIT 100"#).bind(token).fetch_all(&self.pool).await?;
        Ok(
            json!({"original":original,"current":current,"history":history,"external_research":{"status":"NOT_CONFIGURED","fomo_content":"COMING_LATER"}}),
        )
    }

    pub async fn traders(&self, limit: i64, offset: i64) -> Result<Value, sqlx::Error> {
        let items:Vec<Value>=sqlx::query_scalar(r#"SELECT jsonb_build_object('id',t.id,'handle',t.handle,'display_name',t.display_name,'manual_tier',t.manual_tier,'status',t.status,'notes',t.notes,'wallet_count',count(DISTINCT w.id),'confirmed_smart_trades',count(DISTINCT s.id),'open_positions',count(DISTINCT p.id)FILTER(WHERE p.open),'pons_score',max(cs.pons_score)::text,'pons_score_confidence',max(cs.pons_score_confidence)::text,'pons_score_sample_size',max(cs.sample_size)) FROM traders t LEFT JOIN trader_wallets w ON w.trader_id=t.id LEFT JOIN smart_trades s ON s.trader_id=t.id AND s.confirmation_level IN('BUY_CONFIRMED','SELL_CONFIRMED') LEFT JOIN wallet_token_positions p ON p.trader_id=t.id LEFT JOIN current_trader_scores cs ON cs.trader_id=t.id GROUP BY t.id ORDER BY t.handle LIMIT $1 OFFSET $2"#).bind(limit).bind(offset).fetch_all(&self.pool).await?;
        let total: i64 = sqlx::query_scalar("SELECT count(*) FROM traders")
            .fetch_one(&self.pool)
            .await?;
        Ok(json!({"items":items,"total":total,"limit":limit,"offset":offset}))
    }

    pub async fn trader(&self, id: Uuid) -> Result<Option<Value>, sqlx::Error> {
        sqlx::query_scalar(r#"SELECT jsonb_build_object('id',t.id,'handle',t.handle,'display_name',t.display_name,'manual_tier',t.manual_tier,'status',t.status,'notes',t.notes,'wallets',(SELECT COALESCE(jsonb_agg(jsonb_build_object('id',w.id,'address','0x'||encode(w.address,'hex'),'role',w.role,'source',w.source,'confidence',w.identity_confidence::text,'verified',w.verified,'enabled',w.enabled,'valid_from',w.valid_from,'valid_to',w.valid_to,'notes',w.notes)ORDER BY w.created_at),'[]')FROM trader_wallets w WHERE w.trader_id=t.id),'recent_trades',(SELECT COALESCE(jsonb_agg(x.obj ORDER BY x.block_time DESC),'[]')FROM(SELECT s.block_time,jsonb_build_object('id',s.id,'token_id',s.token_id,'token_address','0x'||encode(k.address,'hex'),'symbol',m.symbol,'side',s.side,'token_amount_raw',s.token_amount_raw,'quote_amount_raw',s.quote_amount_raw,'confirmation',s.confirmation_level,'chain_finality',tt.status,'event_effective_at',s.block_time) obj FROM smart_trades s JOIN tokens k ON k.id=s.token_id JOIN token_trades tt ON tt.id=s.token_trade_id LEFT JOIN token_metadata_current m ON m.token_id=k.id WHERE s.trader_id=t.id ORDER BY s.block_time DESC LIMIT 100)x),'positions',(SELECT COALESCE(jsonb_agg(jsonb_build_object('id',p.id,'token_id',p.token_id,'token_address','0x'||encode(k.address,'hex'),'symbol',m.symbol,'wallet','0x'||encode(p.wallet_address,'hex'),'open',p.open,'balance_raw',p.balance_raw,'position_basis',p.position_basis,'first_entry_at',p.first_entry_at,'last_trade_at',p.last_trade_at,'integrity_status',p.integrity_status)ORDER BY p.last_trade_at DESC),'[]')FROM wallet_token_positions p JOIN tokens k ON k.id=p.token_id LEFT JOIN token_metadata_current m ON m.token_id=k.id WHERE p.trader_id=t.id)) FROM traders t WHERE t.id=$1"#).bind(id).fetch_optional(&self.pool).await
    }

    pub async fn system(&self) -> Result<Value, sqlx::Error> {
        sqlx::query_scalar(r#"SELECT jsonb_build_object('postgres','HEALTHY','outbox_seq',(SELECT COALESCE(max(seq),0)FROM event_outbox),'wss_clients',NULL,'deployments',(SELECT COALESCE(jsonb_agg(jsonb_build_object('id',id,'address','0x'||encode(address,'hex'),'health',health,'enabled',enabled,'trust',trust_basis,'last_verified_at',last_verified_at,'verification_error',verification_error)),'[]')FROM protocol_deployments),'cursors',(SELECT COALESCE(jsonb_agg(jsonb_build_object('stream',stream,'block',last_processed_block::text,'updated_at',updated_at)),'[]')FROM chain_cursors),'tracked_curves',(SELECT count(*)FROM pons_curves),'workers',jsonb_build_object('metadata',(SELECT jsonb_build_object('pending',count(*)FILTER(WHERE status<>'SUCCEEDED'),'last_error',max(last_error))FROM token_metadata_jobs),'confirmation',(SELECT jsonb_build_object('pending',count(*)FILTER(WHERE status NOT IN('CONFIRMED','REJECTED')),'last_error',max(last_error))FROM trade_confirmation_jobs),'position',(SELECT jsonb_build_object('pending',count(*)FILTER(WHERE status<>'COMPLETED'),'last_error',max(last_error))FROM position_rebuild_jobs),'market',(SELECT jsonb_build_object('pending',count(*)FILTER(WHERE status<>'COMPLETED'),'last_error',max(last_error))FROM market_rebuild_jobs),'signal',(SELECT jsonb_build_object('pending',count(*)FILTER(WHERE status<>'COMPLETED'),'last_error',max(last_error))FROM signal_rebuild_jobs),'alert',(SELECT jsonb_build_object('cursor',last_processed_seq,'updated_at',updated_at)FROM alert_engine_cursor WHERE singleton)))"#).fetch_one(&self.pool).await
    }
}
