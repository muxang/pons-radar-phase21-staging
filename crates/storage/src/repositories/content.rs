use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

pub const CONTENT_RELATION_CALCULATION_VERSION: i32 = 1;

#[derive(Clone, Debug)]
pub struct ContentRepository {
    pool: PgPool,
}

#[derive(Clone, Debug)]
pub struct NewContentReference<'a> {
    pub trader_id: Uuid,
    pub token_id: Option<Uuid>,
    pub content_type: &'a str,
    pub platform: &'a str,
    pub external_reference: Option<&'a str>,
    pub published_at: DateTime<Utc>,
    pub title: Option<&'a str>,
    pub summary: Option<&'a str>,
    pub stance: Option<&'a str>,
    pub narratives: &'a Value,
    pub content_hash: &'a [u8; 32],
    pub provenance: &'a Value,
}

#[derive(Clone, Debug, FromRow)]
pub struct ContentRelationJob {
    pub trader_id: Uuid,
    pub token_id: Uuid,
    pub generation: i64,
    pub attempts: i32,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContentRebuildResult {
    pub relations: u64,
    pub generation: i64,
}

#[derive(FromRow)]
struct ContentRow {
    id: Uuid,
    published_at: DateTime<Utc>,
    observed_at: DateTime<Utc>,
    content_type: String,
    stance: Option<String>,
}
#[derive(FromRow)]
struct TradeRow {
    id: Uuid,
    side: String,
    block_time: DateTime<Utc>,
}
#[derive(FromRow)]
struct PositionRow {
    id: Uuid,
    event_type: String,
    block_time: DateTime<Utc>,
    smart_trade_id: Uuid,
}

#[allow(clippy::missing_errors_doc)]
impl ContentRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn providers(&self) -> Result<Vec<Value>, sqlx::Error> {
        sqlx::query_scalar("SELECT jsonb_build_object('id',id,'provider_key',provider_key,'provider_type',provider_type,'display_name',display_name,'authorization_basis',authorization_basis,'capabilities',capabilities,'provenance',provenance,'automatic_fetch_enabled',automatic_fetch_enabled,'raw_storage_allowed',raw_storage_allowed,'health',health,'last_success_at',last_success_at,'last_error',last_error)FROM content_providers ORDER BY provider_key").fetch_all(&self.pool).await
    }

    pub async fn create_manual(
        &self,
        v: &NewContentReference<'_>,
    ) -> Result<(Uuid, bool), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let provider:Uuid=sqlx::query_scalar("SELECT id FROM content_providers WHERE provider_key='manual-reference' AND provider_type='MANUAL_REFERENCE' AND authorization_basis='MANUAL_REFERENCE'").fetch_one(&mut*tx).await?;
        let existing: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM trader_content_items WHERE provider_id=$1 AND content_hash=$2",
        )
        .bind(provider)
        .bind(v.content_hash.as_slice())
        .fetch_optional(&mut *tx)
        .await?;
        let (id, created) = if let Some(id) = existing {
            (id, false)
        } else {
            let id:Uuid=sqlx::query_scalar("INSERT INTO trader_content_items(trader_id,provider_id,platform,content_type,external_reference,published_at,content_hash,content_availability,title,summary,stance,narratives,structured_analysis,provenance,authorization_basis,raw_content_available,raw_content_authorized,realtime_alert_eligible)VALUES($1,$2,$3,$4,$5,$6,$7,'SUMMARY_AVAILABLE',$8,$9,$10,$11,'{}',$12,'MANUAL_REFERENCE',false,false,false)RETURNING id").bind(v.trader_id).bind(provider).bind(v.platform).bind(v.content_type).bind(v.external_reference).bind(v.published_at).bind(v.content_hash.as_slice()).bind(v.title).bind(v.summary).bind(v.stance).bind(v.narratives).bind(v.provenance).fetch_one(&mut*tx).await?;
            (id, true)
        };
        if let Some(token) = v.token_id {
            sqlx::query("INSERT INTO token_content_links(content_id,token_id,relation_type,confidence,evidence)VALUES($1,$2,'MANUAL_LINK',1,'{\"operator_selected_token\":true}')ON CONFLICT DO NOTHING").bind(id).bind(token).execute(&mut*tx).await?;
        }
        if created {
            super::EventOutboxRepository::append_in_transaction(&mut tx,&super::NewOutboxEvent{event_type:"content.created",schema_version:1,aggregate_type:Some("trader_content"),aggregate_id:Some(id),dedupe_key:&format!("content.created:{id}"),payload:&json!({"content_id":id,"trader_id":v.trader_id,"token_id":v.token_id,"published_at":v.published_at,"observed_at":Utc::now(),"classification_source":"MANUAL_REFERENCE","realtime_alert_eligible":false})}).await?;
        }
        tx.commit().await?;
        Ok((id, created))
    }

    pub async fn claim_due(&self) -> Result<Option<ContentRelationJob>, sqlx::Error> {
        sqlx::query_as("WITH due AS(SELECT trader_id,token_id FROM content_relation_jobs WHERE(status IN('PENDING','RETRY')AND next_attempt_at<=now())OR(status='PROCESSING'AND locked_at<now()-interval '5 minutes')ORDER BY next_attempt_at,trader_id,token_id FOR UPDATE SKIP LOCKED LIMIT 1)UPDATE content_relation_jobs j SET status='PROCESSING',claimed_generation=generation,attempts=attempts+1,locked_at=now(),updated_at=now()FROM due WHERE j.trader_id=due.trader_id AND j.token_id=due.token_id RETURNING j.trader_id,j.token_id,j.generation,j.attempts").fetch_optional(&self.pool).await
    }

    #[allow(clippy::too_many_lines)]
    pub async fn rebuild(
        &self,
        job: &ContentRelationJob,
    ) -> Result<ContentRebuildResult, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::text||$2::text,0))")
            .bind(job.trader_id)
            .bind(job.token_id)
            .execute(&mut *tx)
            .await?;
        let generation:i64=sqlx::query_scalar("SELECT generation FROM content_relation_jobs WHERE trader_id=$1 AND token_id=$2 FOR UPDATE").bind(job.trader_id).bind(job.token_id).fetch_one(&mut*tx).await?;
        let contents:Vec<ContentRow>=sqlx::query_as("SELECT c.id,c.published_at,c.observed_at,c.content_type,c.stance FROM trader_content_items c JOIN token_content_links l ON l.content_id=c.id WHERE c.trader_id=$1 AND l.token_id=$2 ORDER BY c.published_at,c.id").bind(job.trader_id).bind(job.token_id).fetch_all(&mut*tx).await?;
        let trades:Vec<TradeRow>=sqlx::query_as("SELECT s.id,s.side,s.block_time FROM smart_trades s JOIN token_trades t ON t.id=s.token_trade_id WHERE s.trader_id=$1 AND s.token_id=$2 AND s.confirmation_level IN('BUY_CONFIRMED','SELL_CONFIRMED')AND t.status<>'ORPHANED'ORDER BY s.block_time,s.block_number,s.log_index,s.id").bind(job.trader_id).bind(job.token_id).fetch_all(&mut*tx).await?;
        let positions:Vec<PositionRow>=sqlx::query_as("SELECT p.id,p.event_type,p.block_time,p.smart_trade_id FROM position_events p JOIN wallet_token_positions w ON w.id=p.position_id WHERE w.trader_id=$1 AND w.token_id=$2 ORDER BY p.block_time,p.block_number,COALESCE(p.transaction_index,p.log_index),p.log_index,p.id").bind(job.trader_id).bind(job.token_id).fetch_all(&mut*tx).await?;
        sqlx::query("DELETE FROM content_trade_relations WHERE trader_id=$1 AND token_id=$2")
            .bind(job.trader_id)
            .bind(job.token_id)
            .execute(&mut *tx)
            .await?;
        let mut relations = 0_u64;
        for content in &contents {
            for trade in &trades {
                if content.content_type != "TRADE_THESIS" {
                    continue;
                }
                let before = content.published_at <= trade.block_time;
                let relation = match (trade.side.as_str(), before) {
                    ("BUY", true) => "THESIS_BEFORE_BUY",
                    ("BUY", false) => "THESIS_AFTER_BUY",
                    ("SELL", true) => "THESIS_BEFORE_SELL",
                    ("SELL", false) => "THESIS_AFTER_SELL",
                    _ => continue,
                };
                let delta = (trade.block_time - content.published_at).num_milliseconds();
                insert_relation(&mut tx,content,job,relation,Some(trade.block_time),Some(delta),Some(trade.id),None,json!({"ordering_basis":"published_at_vs_chain_block_time","content_observed_at":content.observed_at,"trade_side":trade.side})).await?;
                relations += 1;
                if let Some(alignment) = alignment(content.stance.as_deref(), &trade.side) {
                    insert_relation(&mut tx,content,job,alignment,Some(trade.block_time),Some(delta),Some(trade.id),None,json!({"rule":"structured_stance_vs_confirmed_trade_side","stance":content.stance,"trade_side":trade.side})).await?;
                    relations += 1;
                }
            }
            let latest = positions
                .iter()
                .rfind(|p| p.block_time <= content.published_at);
            if let Some(position) = latest.filter(|p| {
                matches!(
                    p.event_type.as_str(),
                    "OPEN_POSITION" | "ADD_POSITION" | "REDUCE_POSITION"
                )
            }) {
                insert_relation(&mut tx,content,job,"THESIS_WHILE_HOLDING",Some(position.block_time),Some((position.block_time-content.published_at).num_milliseconds()),Some(position.smart_trade_id),Some(position.id),json!({"position_event":position.event_type,"basis":"position_state_at_published_at"})).await?;
                relations += 1;
            } else {
                insert_relation(
                    &mut tx,
                    content,
                    job,
                    "CONTENT_WITHOUT_POSITION",
                    None,
                    None,
                    None,
                    None,
                    json!({"basis":"no_open_trade_derived_position_at_published_at"}),
                )
                .await?;
                relations += 1;
            }
        }
        sqlx::query("UPDATE content_relation_jobs SET status=CASE WHEN generation=$3 THEN'COMPLETED'ELSE'PENDING'END,locked_at=NULL,last_error=NULL,updated_at=now()WHERE trader_id=$1 AND token_id=$2").bind(job.trader_id).bind(job.token_id).bind(generation).execute(&mut*tx).await?;
        tx.commit().await?;
        Ok(ContentRebuildResult {
            relations,
            generation,
        })
    }

    pub async fn retry(
        &self,
        job: &ContentRelationJob,
        error: &str,
        next: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE content_relation_jobs SET status='RETRY',last_error=$3,next_attempt_at=$4,locked_at=NULL,updated_at=now()WHERE trader_id=$1 AND token_id=$2").bind(job.trader_id).bind(job.token_id).bind(error.chars().take(2048).collect::<String>()).bind(next).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn token_content(
        &self,
        token: Uuid,
        limit: i64,
        offset: i64,
    ) -> Result<Value, sqlx::Error> {
        let items:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('id',c.id,'trader_id',c.trader_id,'trader_handle',t.handle,'provider',p.provider_key,'platform',c.platform,'content_type',c.content_type,'external_reference',c.external_reference,'published_at',c.published_at,'observed_at',c.observed_at,'observed_later',c.observed_at>c.published_at+interval '1 minute','title',c.title,'summary',c.summary,'stance',c.stance,'narratives',c.narratives,'structured_analysis',c.structured_analysis,'authorization_basis',c.authorization_basis,'content_availability',c.content_availability,'raw_content_available',c.raw_content_available,'raw_content_authorized',c.raw_content_authorized,'realtime_alert_eligible',c.realtime_alert_eligible,'link',jsonb_build_object('relation_type',l.relation_type,'confidence',l.confidence::text,'evidence',l.evidence),'relations',COALESCE((SELECT jsonb_agg(jsonb_build_object('relation_type',r.relation_type,'trade_event_time',r.trade_event_time,'delta_ms',r.delta_ms,'smart_trade_id',r.smart_trade_id,'position_event_id',r.position_event_id,'evidence',r.evidence,'calculation_version',r.calculation_version)ORDER BY r.trade_event_time NULLS LAST),'[]')FROM content_trade_relations r WHERE r.content_id=c.id AND r.token_id=l.token_id))FROM token_content_links l JOIN trader_content_items c ON c.id=l.content_id LEFT JOIN traders t ON t.id=c.trader_id JOIN content_providers p ON p.id=c.provider_id WHERE l.token_id=$1 ORDER BY c.published_at DESC,c.id LIMIT $2 OFFSET $3").bind(token).bind(limit).bind(offset).fetch_all(&self.pool).await?;
        Ok(json!({"items":items,"limit":limit,"offset":offset}))
    }

    pub async fn trader_content(
        &self,
        trader: Uuid,
        limit: i64,
        offset: i64,
    ) -> Result<Value, sqlx::Error> {
        let items:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('id',c.id,'provider',p.provider_key,'platform',c.platform,'content_type',c.content_type,'external_reference',c.external_reference,'published_at',c.published_at,'observed_at',c.observed_at,'title',c.title,'summary',c.summary,'stance',c.stance,'narratives',c.narratives,'authorization_basis',c.authorization_basis,'tokens',COALESCE((SELECT jsonb_agg(jsonb_build_object('token_id',l.token_id,'address','0x'||encode(t.address,'hex'),'relation_type',l.relation_type,'confidence',l.confidence::text))FROM token_content_links l JOIN tokens t ON t.id=l.token_id WHERE l.content_id=c.id),'[]'),'relations',COALESCE((SELECT jsonb_agg(jsonb_build_object('relation_type',r.relation_type,'token_id',r.token_id,'delta_ms',r.delta_ms,'evidence',r.evidence))FROM content_trade_relations r WHERE r.content_id=c.id),'[]'))FROM trader_content_items c JOIN content_providers p ON p.id=c.provider_id WHERE c.trader_id=$1 ORDER BY c.published_at DESC,c.id LIMIT $2 OFFSET $3").bind(trader).bind(limit).bind(offset).fetch_all(&self.pool).await?;
        let stats:Value=sqlx::query_scalar("SELECT jsonb_build_object('content_count',count(DISTINCT c.id),'thesis_count',count(DISTINCT c.id)FILTER(WHERE c.content_type='TRADE_THESIS'),'thesis_before_buy_count',count(DISTINCT r.content_id)FILTER(WHERE r.relation_type='THESIS_BEFORE_BUY'),'thesis_after_buy_count',count(DISTINCT r.content_id)FILTER(WHERE r.relation_type='THESIS_AFTER_BUY'),'content_without_position_count',count(DISTINCT r.content_id)FILTER(WHERE r.relation_type='CONTENT_WITHOUT_POSITION'),'aligned_count',count(DISTINCT r.id)FILTER(WHERE r.relation_type='CONTENT_POSITION_ALIGNED'),'divergent_count',count(DISTINCT r.id)FILTER(WHERE r.relation_type='CONTENT_POSITION_DIVERGENT'))FROM trader_content_items c LEFT JOIN content_trade_relations r ON r.content_id=c.id WHERE c.trader_id=$1").bind(trader).fetch_one(&self.pool).await?;
        Ok(json!({"items":items,"stats":stats,"limit":limit,"offset":offset}))
    }
}

fn alignment(stance: Option<&str>, side: &str) -> Option<&'static str> {
    match (stance, side) {
        (Some("BULLISH"), "BUY") | (Some("BEARISH"), "SELL") => Some("CONTENT_POSITION_ALIGNED"),
        (Some("BULLISH"), "SELL") | (Some("BEARISH"), "BUY") => Some("CONTENT_POSITION_DIVERGENT"),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
async fn insert_relation(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    content: &ContentRow,
    job: &ContentRelationJob,
    relation: &str,
    trade_time: Option<DateTime<Utc>>,
    delta: Option<i64>,
    smart: Option<Uuid>,
    position: Option<Uuid>,
    evidence: Value,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO content_trade_relations(content_id,trader_id,token_id,relation_type,content_time,trade_event_time,delta_ms,smart_trade_id,position_event_id,evidence,calculation_version)VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)").bind(content.id).bind(job.trader_id).bind(job.token_id).bind(relation).bind(content.published_at).bind(trade_time).bind(delta).bind(smart).bind(position).bind(evidence).bind(CONTENT_RELATION_CALCULATION_VERSION).execute(&mut**tx).await?;
    Ok(())
}
