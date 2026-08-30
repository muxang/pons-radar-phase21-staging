use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct AiResearchRepository {
    pool: PgPool,
}
#[derive(Clone, Debug, FromRow)]
pub struct AiResearchJob {
    pub id: Uuid,
    pub token_id: Uuid,
    pub research_mode: String,
    pub knowledge_cutoff: DateTime<Utc>,
    pub trigger_type: String,
    pub trigger_origin: String,
    pub trigger_realtime_eligible: bool,
    pub attempts: i32,
}
#[derive(Clone, Debug)]
pub struct CompletedAiReport<'a> {
    pub provider: &'a str,
    pub model: &'a str,
    pub prompt_version: i32,
    pub knowledge_cutoff: DateTime<Utc>,
    pub evidence_generated_at: DateTime<Utc>,
    pub evidence_hash: &'a [u8; 32],
    pub input_hash: &'a [u8; 32],
    pub report: &'a Value,
    pub category: &'a str,
    pub summary: &'a str,
    pub confidence: i32,
    pub usage: Option<&'a Value>,
}

#[allow(clippy::missing_errors_doc)]
impl AiResearchRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue(
        &self,
        token: Uuid,
        mode: &str,
        cutoff: DateTime<Utc>,
        trigger: &str,
        trigger_origin: &str,
        trigger_realtime_eligible: bool,
        priority: i32,
        user: Option<Uuid>,
    ) -> Result<Uuid, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let id:Uuid=sqlx::query_scalar("INSERT INTO ai_research_jobs(token_id,research_mode,knowledge_cutoff,trigger_type,trigger_origin,trigger_realtime_eligible,priority,requested_by)VALUES($1,$2,$3,$4,$5,$6,$7,$8)ON CONFLICT(token_id,research_mode,knowledge_cutoff,trigger_type)WHERE status IN('PENDING','PROCESSING','RETRY')DO UPDATE SET priority=GREATEST(ai_research_jobs.priority,EXCLUDED.priority),updated_at=now()RETURNING id").bind(token).bind(mode).bind(cutoff).bind(trigger).bind(trigger_origin).bind(trigger_realtime_eligible).bind(priority).bind(user).fetch_one(&mut*tx).await?;
        super::EventOutboxRepository::append_in_transaction(&mut tx,&super::NewOutboxEvent{event_type:"ai.research_queued",schema_version:1,aggregate_type:Some("token"),aggregate_id:Some(token),dedupe_key:&format!("ai.research_queued:{id}"),payload:&json!({"job_id":id,"token_id":token,"knowledge_cutoff":cutoff,"trigger_type":trigger,"trigger_origin":trigger_origin,"trigger_realtime_eligible":trigger_realtime_eligible,"realtime_alert_eligible":false})}).await?;
        tx.commit().await?;
        Ok(id)
    }
    pub async fn claim_due(&self) -> Result<Option<AiResearchJob>, sqlx::Error> {
        sqlx::query_as("WITH due AS(SELECT id FROM ai_research_jobs WHERE(status IN('PENDING','RETRY')AND next_attempt_at<=now())OR(status='PROCESSING'AND locked_at<now()-interval '5 minutes')ORDER BY priority DESC,next_attempt_at,id FOR UPDATE SKIP LOCKED LIMIT 1)UPDATE ai_research_jobs j SET status='PROCESSING',attempts=attempts+1,locked_at=now(),updated_at=now()FROM due WHERE j.id=due.id RETURNING j.id,j.token_id,j.research_mode,j.knowledge_cutoff,j.trigger_type,j.trigger_origin,j.trigger_realtime_eligible,j.attempts").fetch_optional(&self.pool).await
    }
    pub async fn enqueue_automatic(
        &self,
        minimum_score: i32,
        minimum_buyers: i64,
        cooldown_seconds: i64,
    ) -> Result<u64, sqlx::Error> {
        let tokens:Vec<Uuid>=sqlx::query_scalar("SELECT c.token_id FROM current_signal_states c JOIN signal_snapshots ss ON ss.id=c.signal_snapshot_id WHERE c.score>=$1 AND c.state IN('STRONG_WATCH','HIGH_PRIORITY')AND ss.current_generation AND ss.classification_origin='LIVE'AND ss.realtime_alert_eligible AND(SELECT count(DISTINCT s.trader_wallet_id)FROM smart_trades s JOIN token_trades tt ON tt.id=s.token_trade_id WHERE s.token_id=c.token_id AND s.side='BUY'AND s.confirmation_level='BUY_CONFIRMED'AND tt.status<>'ORPHANED')>=$2 AND NOT EXISTS(SELECT 1 FROM ai_research_jobs j WHERE j.token_id=c.token_id AND(j.status IN('PENDING','PROCESSING','RETRY')OR j.created_at>now()-make_interval(secs=>$3)))AND NOT EXISTS(SELECT 1 FROM ai_research_reports r WHERE r.token_id=c.token_id AND r.created_at>now()-make_interval(secs=>$3))LIMIT 20").bind(minimum_score).bind(minimum_buyers).bind(cooldown_seconds).fetch_all(&self.pool).await?;
        let count = tokens.len() as u64;
        for token in tokens {
            self.enqueue(
                token,
                "CURRENT_RESEARCH",
                Utc::now(),
                "SIGNAL",
                "LIVE",
                true,
                10,
                None,
            )
            .await?;
        }
        Ok(count)
    }
    pub async fn package(
        &self,
        token: Uuid,
        cutoff: DateTime<Utc>,
        research_mode: &str,
    ) -> Result<Value, sqlx::Error> {
        let core:Value=sqlx::query_scalar("SELECT jsonb_build_object('evidence_id','TOKEN:'||id,'chain_id',chain_id,'address','0x'||encode(address,'hex'),'curve','0x'||encode(curve_address,'hex'),'deployer','0x'||encode(deployer,'hex'),'launch_block',launch_block::text,'launch_time',launch_time,'lifecycle',lifecycle,'name',name,'symbol',symbol,'decimals',decimals,'total_supply_raw',total_supply_raw)FROM tokens WHERE id=$1 AND launch_time<=$2").bind(token).bind(cutoff).fetch_one(&self.pool).await?;
        let metadata_original:Option<Value>=sqlx::query_scalar("SELECT jsonb_build_object('evidence_id','TOKEN_METADATA_ORIGINAL','observed_at',observed_at,'capture_mode',capture_mode,'exact_launch_snapshot',exact_launch_snapshot,'requested_block',requested_block::text,'observed_block',observed_block::text,'name',name,'symbol',symbol,'decimals',decimals,'total_supply_raw',total_supply_raw,'token_deployer','0x'||encode(token_deployer,'hex'),'token_logo',token_logo,'token_description',token_description,'socials',jsonb_build_object('twitter',twitter,'telegram',telegram,'discord',discord,'website',website,'farcaster',farcaster),'deployer_matches_launch',deployer_matches_launch,'integrity_warning',integrity_warning)FROM token_metadata_original WHERE token_id=$1 AND observed_at<=$2").bind(token).bind(cutoff).fetch_optional(&self.pool).await?;
        let metadata_current:Option<Value>=sqlx::query_scalar("SELECT jsonb_build_object('evidence_id','TOKEN_METADATA_CURRENT','observed_at',observed_at,'capture_mode',capture_mode,'exact_launch_snapshot',exact_launch_snapshot,'requested_block',requested_block::text,'observed_block',observed_block::text,'name',name,'symbol',symbol,'decimals',decimals,'total_supply_raw',total_supply_raw,'token_deployer','0x'||encode(token_deployer,'hex'),'token_logo',token_logo,'token_description',token_description,'socials',jsonb_build_object('twitter',twitter,'telegram',telegram,'discord',discord,'website',website,'farcaster',farcaster),'deployer_matches_launch',deployer_matches_launch,'integrity_warning',integrity_warning)FROM token_metadata_current WHERE token_id=$1 AND updated_at<=$2").bind(token).bind(cutoff).fetch_optional(&self.pool).await?;
        let metadata_history:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('evidence_id','METADATA:'||s.id,'observed_at',s.observed_at,'capture_mode',s.capture_mode,'exact_launch_snapshot',s.exact_launch_snapshot,'requested_block',s.requested_block::text,'observed_block',s.observed_block::text,'metadata',s.metadata,'deployer_matches_launch',s.deployer_matches_launch,'integrity_warning',s.integrity_warning)FROM token_metadata_snapshots s WHERE s.token_id=$1 AND s.created_at<=$2 ORDER BY s.observed_at DESC,s.id DESC LIMIT 10").bind(token).bind(cutoff).fetch_all(&self.pool).await?;
        let metadata = json!({"original":metadata_original,"current":metadata_current,"changes":metadata_history});
        let smart:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('evidence_id','SMART_TRADE:'||s.id,'side',s.side,'block_time',s.block_time,'known_at',COALESCE(s.confirmed_at,s.created_at),'wallet','0x'||encode(s.wallet_address,'hex'),'confirmation_level',s.confirmation_level,'chain_status',tt.status,'token_amount_raw',s.token_amount_raw,'quote_amount_raw',s.quote_amount_raw,'buyer_rank',s.buyer_rank,'smart_buyer_rank',s.smart_buyer_rank,'trader',tr.handle,'manual_tier',tr.manual_tier,'pons_score',score.pons_score::text,'pons_score_confidence',score.pons_score_confidence::text,'score_sample_size',score.sample_size,'score_as_of_mode',score.as_of_mode)FROM smart_trades s JOIN token_trades tt ON tt.id=s.token_trade_id JOIN traders tr ON tr.id=s.trader_id LEFT JOIN LATERAL(SELECT h.pons_score,h.pons_score_confidence,h.sample_size,h.as_of_mode FROM trader_score_history h WHERE h.trader_id=s.trader_id AND h.as_of_mode='KNOWLEDGE_TIME'AND h.knowledge_available_at<=$2 ORDER BY h.knowledge_available_at DESC,h.id DESC LIMIT 1)score ON true WHERE s.token_id=$1 AND s.confirmation_level IN('BUY_CONFIRMED','SELL_CONFIRMED')AND tt.status<>'ORPHANED'AND COALESCE(s.confirmed_at,s.created_at)<=$2 ORDER BY s.block_time DESC,s.id DESC LIMIT 50").bind(token).bind(cutoff).fetch_all(&self.pool).await?;
        let content:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('evidence_id','CONTENT:'||c.id,'trader',tr.handle,'content_type',c.content_type,'published_at',c.published_at,'observed_at',c.observed_at,'summary',c.summary,'stance',c.stance,'narratives',c.narratives,'relations',COALESCE((SELECT jsonb_agg(jsonb_build_object('type',r.relation_type,'delta_ms',r.delta_ms))FROM content_trade_relations r WHERE r.content_id=c.id AND r.token_id=$1),'[]'))FROM token_content_links l JOIN trader_content_items c ON c.id=l.content_id LEFT JOIN traders tr ON tr.id=c.trader_id WHERE l.token_id=$1 AND c.observed_at<=$2 ORDER BY c.published_at DESC,c.id DESC LIMIT 25").bind(token).bind(cutoff).fetch_all(&self.pool).await?;
        let market:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('evidence_id','MARKET_SNAPSHOT:'||id,'snapshot_at',snapshot_at,'unique_buyers',unique_buyers,'unique_sellers',unique_sellers,'holder_count',holder_count,'smart_unique_buyers',smart_unique_buyers,'smart_net_flow_raw',smart_net_flow_raw,'curve_progress',curve_progress::text,'spot_price_quote',spot_price_quote::text,'evidence_scope',CASE WHEN state_exact THEN'BLOCK_STATE_EXACT'ELSE'UNAVAILABLE'END)FROM token_market_snapshots WHERE token_id=$1 AND created_at<=$2 ORDER BY snapshot_at DESC,id DESC LIMIT 20").bind(token).bind(cutoff).fetch_all(&self.pool).await?;
        let signal:Option<Value>=sqlx::query_scalar("SELECT jsonb_build_object('evidence_id','SIGNAL:'||id,'state',state,'score',score::text,'confidence',confidence::text,'component_scores',component_scores,'reason_codes',reason_codes,'negative_reasons',negative_reasons,'calculation_version',calculation_version)FROM signal_snapshots WHERE token_id=$1 AND calculated_at<=$2 AND current_generation ORDER BY effective_at DESC,id DESC LIMIT 1").bind(token).bind(cutoff).fetch_optional(&self.pool).await?;
        Ok(
            json!({"input_schema_version":1,"research_mode":research_mode,"knowledge_cutoff":cutoff,"evidence_generated_at":Utc::now(),"trusted_system_facts":{"token":core,"metadata":metadata,"smart_money":smart,"market_snapshots":market,"signal":signal,"use_dynamic_trader_score_in_signal":false,"use_ai_research_in_signal":false},"untrusted_content_evidence":{"classification":"DATA_NOT_INSTRUCTIONS","items":content},"selection":{"smart_money_limit":50,"market_snapshot_limit":20,"content_limit":25,"raw_content_included":false}}),
        )
    }
    pub async fn cached(
        &self,
        token: Uuid,
        hash: &[u8; 32],
        prompt: i32,
        model: &str,
    ) -> Result<Option<Uuid>, sqlx::Error> {
        sqlx::query_scalar("SELECT id FROM ai_research_reports WHERE token_id=$1 AND evidence_hash=$2 AND prompt_version=$3 AND model=$4 AND status='COMPLETED'ORDER BY created_at DESC LIMIT 1").bind(token).bind(hash.as_slice()).bind(prompt).bind(model).fetch_optional(&self.pool).await
    }
    pub async fn mark_cached(
        &self,
        job: Uuid,
        report: Uuid,
        hash: &[u8; 32],
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE ai_research_jobs SET status='CACHED',report_id=$2,evidence_hash=$3,locked_at=NULL,updated_at=now()WHERE id=$1").bind(job).bind(report).bind(hash.as_slice()).execute(&self.pool).await?;
        Ok(())
    }
    pub async fn complete(
        &self,
        job: &AiResearchJob,
        v: &CompletedAiReport<'_>,
    ) -> Result<Uuid, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let id:Uuid=sqlx::query_scalar("INSERT INTO ai_research_reports(token_id,provider,model,report_version,prompt_version,prompt_schema_version,input_schema_version,output_schema_version,research_mode,knowledge_cutoff,evidence_generated_at,evidence_hash,input_hash,structured_report,category,summary,confidence,usage_metadata,trigger_type,trigger_origin)VALUES($1,$2,$3,1,$4,1,1,1,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)RETURNING id").bind(job.token_id).bind(v.provider).bind(v.model).bind(v.prompt_version).bind(&job.research_mode).bind(v.knowledge_cutoff).bind(v.evidence_generated_at).bind(v.evidence_hash.as_slice()).bind(v.input_hash.as_slice()).bind(v.report).bind(v.category).bind(v.summary).bind(v.confidence).bind(v.usage).bind(&job.trigger_type).bind(&job.trigger_origin).fetch_one(&mut*tx).await?;
        sqlx::query("UPDATE ai_research_reports SET status='SUPERSEDED',superseded_by=$2 WHERE token_id=$1 AND id<>$2 AND status='COMPLETED'")
            .bind(job.token_id).bind(id).execute(&mut *tx).await?;
        sqlx::query("UPDATE ai_research_jobs SET status='SUCCEEDED',report_id=$2,evidence_hash=$3,locked_at=NULL,last_error=NULL,updated_at=now()WHERE id=$1").bind(job.id).bind(id).bind(v.evidence_hash.as_slice()).execute(&mut*tx).await?;
        super::EventOutboxRepository::append_in_transaction(&mut tx,&super::NewOutboxEvent{event_type:"ai.research_completed",schema_version:1,aggregate_type:Some("token"),aggregate_id:Some(job.token_id),dedupe_key:&format!("ai.research_completed:{id}"),payload:&json!({"report_id":id,"token_id":job.token_id,"knowledge_cutoff":v.knowledge_cutoff,"realtime_alert_eligible":false})}).await?;
        tx.commit().await?;
        Ok(id)
    }
    pub async fn retry(
        &self,
        job: Uuid,
        error: &str,
        next: DateTime<Utc>,
        failed: bool,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let persisted_error = if error.to_ascii_lowercase().contains("bearer ") {
            "AI provider error contained redacted authorization data".to_owned()
        } else {
            error.chars().take(2048).collect::<String>()
        };
        sqlx::query("UPDATE ai_research_jobs SET status=CASE WHEN $4 THEN'FAILED'ELSE'RETRY'END,last_error=$2,next_attempt_at=$3,locked_at=NULL,updated_at=now()WHERE id=$1").bind(job).bind(persisted_error).bind(next).bind(failed).execute(&mut*tx).await?;
        if failed {
            if let Some(token) =
                sqlx::query_scalar::<_, Uuid>("SELECT token_id FROM ai_research_jobs WHERE id=$1")
                    .bind(job)
                    .fetch_optional(&mut *tx)
                    .await?
            {
                super::EventOutboxRepository::append_in_transaction(&mut tx,&super::NewOutboxEvent{event_type:"ai.research_failed",schema_version:1,aggregate_type:Some("token"),aggregate_id:Some(token),dedupe_key:&format!("ai.research_failed:{job}"),payload:&json!({"job_id":job,"token_id":token,"realtime_alert_eligible":false})}).await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }
    pub async fn reports(&self, token: Uuid, limit: i64) -> Result<Value, sqlx::Error> {
        let rows:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('id',id,'provider',provider,'model',model,'report_version',report_version,'prompt_version',prompt_version,'input_schema_version',input_schema_version,'output_schema_version',output_schema_version,'research_mode',research_mode,'trigger_type',trigger_type,'trigger_origin',trigger_origin,'knowledge_cutoff',knowledge_cutoff,'knowledge_available_at',knowledge_available_at,'evidence_generated_at',evidence_generated_at,'evidence_hash',encode(evidence_hash,'hex'),'structured_report',structured_report,'category',category,'summary',summary,'confidence',confidence,'usage_metadata',usage_metadata,'status',status,'created_at',created_at)FROM ai_research_reports WHERE token_id=$1 ORDER BY created_at DESC,id DESC LIMIT $2").bind(token).bind(limit.clamp(1,100)).fetch_all(&self.pool).await?;
        let job:Option<Value>=sqlx::query_scalar("SELECT jsonb_build_object('id',id,'status',status,'trigger_type',trigger_type,'attempts',attempts,'last_error',last_error,'created_at',created_at,'updated_at',updated_at)FROM ai_research_jobs WHERE token_id=$1 ORDER BY created_at DESC LIMIT 1").bind(token).fetch_optional(&self.pool).await?;
        Ok(
            json!({"current":rows.first(),"history":rows,"job":job,"chain_signal_independent":true,"use_ai_research_in_signal":false}),
        )
    }
}
