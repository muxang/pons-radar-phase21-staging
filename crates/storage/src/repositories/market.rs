use alloy_primitives::U256;
use chrono::{DateTime, Utc};
use pons_domain::{BlockHash, BlockNumber, ChainId, LogIndex, TokenAddress, TxHash, WalletAddress};
use serde_json::Value;
use sqlx::{FromRow, PgPool};
use std::collections::HashMap;
use uuid::Uuid;

pub const MARKET_CALCULATION_VERSION: i32 = 2;
#[derive(Clone, Debug)]
pub struct MarketRepository {
    pool: PgPool,
}
#[derive(Clone, Debug, FromRow)]
pub struct MarketJob {
    pub token_id: Uuid,
    pub generation: i64,
    pub attempts: i32,
}
#[derive(Clone, Debug)]
pub struct MarketSubject {
    pub token: TokenAddress,
    pub curve: pons_domain::CurveAddress,
    pub pair_token: Option<TokenAddress>,
    pub token_decimals: Option<u8>,
    pub total_supply_raw: Option<String>,
}
pub struct CurveObservation<'a> {
    pub token_id: Uuid,
    pub block_number: BlockNumber,
    pub quote_reserve_raw: &'a str,
    pub token_reserve_raw: &'a str,
    pub sellable_tokens_raw: &'a str,
    pub reserved_tokens_raw: &'a str,
    pub real_quote_reserve_raw: &'a str,
    pub graduation_threshold_raw: &'a str,
    pub ready_to_graduate: bool,
    pub token_decimals: u8,
    pub quote_decimals: u8,
    pub curve_progress: &'a str,
    pub quote_progress: &'a str,
    pub spot_price_quote: &'a str,
    pub curve_implied_fdv_quote: &'a str,
    pub integrity_warning: Option<&'a str>,
    pub evidence: &'a Value,
}
pub struct PersistTransfer<'a> {
    pub chain_id: ChainId,
    pub token_id: Uuid,
    pub token: TokenAddress,
    pub from: WalletAddress,
    pub to: WalletAddress,
    pub amount_raw: &'a str,
    pub block_number: BlockNumber,
    pub block_hash: BlockHash,
    pub tx_hash: TxHash,
    pub transaction_index: Option<u64>,
    pub log_index: LogIndex,
    pub block_time: DateTime<Utc>,
    pub topics: &'a Value,
    pub data: &'a [u8],
}
#[derive(FromRow)]
struct TransferRow {
    wallet: Vec<u8>,
    amount_raw: String,
    incoming: bool,
}

#[allow(clippy::missing_errors_doc)]
impl MarketRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    pub async fn persist_transfer(&self, v: &PersistTransfer<'_>) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let raw:Uuid=sqlx::query_scalar("INSERT INTO raw_chain_logs(chain_id,block_number,block_hash,tx_hash,log_index,address,topics,data,status) VALUES($1::numeric,$2::numeric,$3,$4,$5::numeric,$6,$7,$8,'PENDING') ON CONFLICT(chain_id,tx_hash,log_index) DO UPDATE SET tx_hash=EXCLUDED.tx_hash RETURNING id").bind(v.chain_id.get().to_string()).bind(v.block_number.get().to_string()).bind(v.block_hash.as_bytes().as_slice()).bind(v.tx_hash.as_bytes().as_slice()).bind(v.log_index.get().to_string()).bind(v.token.as_bytes().as_slice()).bind(v.topics).bind(v.data).fetch_one(&mut*tx).await?;
        let event_id:Vec<u8>=sqlx::query_scalar("INSERT INTO normalized_events(raw_log_id,chain_id,tx_hash,log_index,event_type,parser_version,schema_version,payload) VALUES($1,$2::numeric,$3,$4::numeric,'ERC20_TRANSFER',1,1,$5) ON CONFLICT(raw_log_id,event_type,parser_version,schema_version) DO UPDATE SET payload=normalized_events.payload RETURNING event_id").bind(raw).bind(v.chain_id.get().to_string()).bind(v.tx_hash.as_bytes().as_slice()).bind(v.log_index.get().to_string()).bind(serde_json::json!({"token":v.token.to_string(),"from":v.from.to_string(),"to":v.to.to_string(),"amount_raw":v.amount_raw})).fetch_one(&mut*tx).await?;
        sqlx::query("INSERT INTO token_transfers(chain_id,token_id,token_address,from_address,to_address,amount_raw,block_number,block_hash,tx_hash,transaction_index,log_index,block_time,raw_log_id,normalized_event_id) VALUES($1::numeric,$2,$3,$4,$5,$6,$7::numeric,$8,$9,$10::numeric,$11::numeric,$12,$13,$14) ON CONFLICT(chain_id,tx_hash,log_index) DO NOTHING").bind(v.chain_id.get().to_string()).bind(v.token_id).bind(v.token.as_bytes().as_slice()).bind(v.from.as_bytes().as_slice()).bind(v.to.as_bytes().as_slice()).bind(v.amount_raw).bind(v.block_number.get().to_string()).bind(v.block_hash.as_bytes().as_slice()).bind(v.tx_hash.as_bytes().as_slice()).bind(v.transaction_index.map(|n|n.to_string())).bind(v.log_index.get().to_string()).bind(v.block_time).bind(raw).bind(event_id).execute(&mut*tx).await?;
        tx.commit().await
    }
    pub async fn claim_due(&self) -> Result<Option<MarketJob>, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let row=sqlx::query_as("WITH d AS(SELECT token_id FROM market_rebuild_jobs WHERE(status IN('PENDING','RETRY')AND next_attempt_at<=now())OR(status='PROCESSING'AND locked_at<now()-interval '5 minutes')ORDER BY next_attempt_at FOR UPDATE SKIP LOCKED LIMIT 1)UPDATE market_rebuild_jobs j SET status='PROCESSING',attempts=attempts+1,locked_at=now()FROM d WHERE j.token_id=d.token_id RETURNING j.token_id,j.generation,j.attempts").fetch_optional(&mut*tx).await?;
        tx.commit().await?;
        Ok(row)
    }
    pub async fn enqueue_due_snapshots(&self) -> Result<(), sqlx::Error> {
        sqlx::query("WITH due AS(SELECT t.id FROM tokens t CROSS JOIN(VALUES(30),(60),(180),(300),(900),(1800),(3600))v(s)WHERE t.launch_time+make_interval(secs=>v.s)<=now()AND NOT EXISTS(SELECT 1 FROM token_market_snapshots x WHERE x.token_id=t.id AND x.snapshot_kind='T+'||v.s||'s'))INSERT INTO market_rebuild_jobs(token_id)SELECT DISTINCT id FROM due ON CONFLICT(token_id)DO UPDATE SET generation=market_rebuild_jobs.generation+1,status='PENDING',next_attempt_at=now(),locked_at=NULL,last_error=NULL,updated_at=now()").execute(&self.pool).await?;
        Ok(())
    }
    pub async fn subject(&self, token_id: Uuid) -> Result<MarketSubject, sqlx::Error> {
        #[allow(clippy::type_complexity)]
        let row: (Vec<u8>, Vec<u8>, Option<Vec<u8>>, Option<i16>, Option<String>) = sqlx::query_as("SELECT t.address,t.curve_address,t.pair_token,COALESCE(m.decimals,t.decimals),COALESCE(m.total_supply_raw,t.total_supply_raw) FROM tokens t LEFT JOIN token_metadata_current m ON m.token_id=t.id WHERE t.id=$1").bind(token_id).fetch_one(&self.pool).await?;
        Ok(MarketSubject {
            token: TokenAddress::from_slice(&row.0).map_err(|e| decode(&e.to_string()))?,
            curve: pons_domain::CurveAddress::from_slice(&row.1)
                .map_err(|e| decode(&e.to_string()))?,
            pair_token: row
                .2
                .map(|v| TokenAddress::from_slice(&v).map_err(|e| decode(&e.to_string())))
                .transpose()?,
            token_decimals: row.3.and_then(|v| u8::try_from(v).ok()),
            total_supply_raw: row.4,
        })
    }
    pub async fn observation_blocks(
        &self,
        token_id: Uuid,
    ) -> Result<Vec<BlockNumber>, sqlx::Error> {
        let values:Vec<String>=sqlx::query_scalar("SELECT block::text FROM(SELECT DISTINCT block_number block FROM(SELECT block_number FROM token_trades WHERE token_id=$1 AND status<>'ORPHANED' UNION SELECT block_number FROM token_transfers WHERE token_id=$1 AND status<>'ORPHANED')x EXCEPT SELECT block_number FROM curve_state_observations WHERE token_id=$1)y ORDER BY block").bind(token_id).fetch_all(&self.pool).await?;
        values
            .into_iter()
            .map(|v| {
                v.parse::<u64>()
                    .map(BlockNumber::new)
                    .map_err(|e| decode(&e.to_string()))
            })
            .collect()
    }
    pub async fn save_observation(&self, v: &CurveObservation<'_>) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO curve_state_observations(token_id,block_number,quote_reserve_raw,token_reserve_raw,sellable_tokens_raw,reserved_tokens_raw,real_quote_reserve_raw,graduation_threshold_raw,ready_to_graduate,token_decimals,quote_decimals,curve_progress,quote_progress,spot_price_quote,curve_implied_fdv_quote,integrity_warning,state_exact,state_scope,evidence)VALUES($1,$2::numeric,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::numeric,$13::numeric,$14::numeric,$15::numeric,$16,true,'BLOCK_STATE_EXACT',$17||jsonb_build_object('state_scope','BLOCK_STATE_EXACT','state_position','END_OF_BLOCK'))ON CONFLICT(token_id,block_number)DO UPDATE SET quote_reserve_raw=EXCLUDED.quote_reserve_raw,token_reserve_raw=EXCLUDED.token_reserve_raw,sellable_tokens_raw=EXCLUDED.sellable_tokens_raw,reserved_tokens_raw=EXCLUDED.reserved_tokens_raw,real_quote_reserve_raw=EXCLUDED.real_quote_reserve_raw,graduation_threshold_raw=EXCLUDED.graduation_threshold_raw,ready_to_graduate=EXCLUDED.ready_to_GRADUATE,token_decimals=EXCLUDED.token_decimals,quote_decimals=EXCLUDED.quote_decimals,curve_progress=EXCLUDED.curve_progress,quote_progress=EXCLUDED.quote_progress,spot_price_quote=EXCLUDED.spot_price_quote,curve_implied_fdv_quote=EXCLUDED.curve_implied_fdv_quote,integrity_warning=EXCLUDED.integrity_warning,state_exact=true,state_scope='BLOCK_STATE_EXACT',evidence=EXCLUDED.evidence,observed_at=now()").bind(v.token_id).bind(v.block_number.get().to_string()).bind(v.quote_reserve_raw).bind(v.token_reserve_raw).bind(v.sellable_tokens_raw).bind(v.reserved_tokens_raw).bind(v.real_quote_reserve_raw).bind(v.graduation_threshold_raw).bind(v.ready_to_graduate).bind(i16::from(v.token_decimals)).bind(i16::from(v.quote_decimals)).bind(v.curve_progress).bind(v.quote_progress).bind(v.spot_price_quote).bind(v.curve_implied_fdv_quote).bind(v.integrity_warning).bind(v.evidence).execute(&self.pool).await?;
        Ok(())
    }
    pub async fn retry(
        &self,
        j: &MarketJob,
        error: &str,
        next: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE market_rebuild_jobs SET status='RETRY',next_attempt_at=$2,last_error=$3,locked_at=NULL WHERE token_id=$1").bind(j.token_id).bind(next).bind(error.chars().take(2048).collect::<String>()).execute(&self.pool).await?;
        Ok(())
    }
    #[allow(clippy::too_many_lines)]
    pub async fn rebuild(&self, j: &MarketJob) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::text,1))")
            .bind(j.token_id)
            .execute(&mut *tx)
            .await?;
        let (curve, factory): (Vec<u8>, Option<Vec<u8>>) =
            sqlx::query_as("SELECT curve_address,factory_address FROM tokens WHERE id=$1")
                .bind(j.token_id)
                .fetch_one(&mut *tx)
                .await?;
        let rows:Vec<TransferRow>=sqlx::query_as("SELECT wallet,amount_raw,incoming FROM(SELECT from_address wallet,amount_raw,false incoming,block_number,COALESCE(transaction_index,log_index) transaction_index,log_index,0 leg FROM token_transfers WHERE token_id=$1 AND status<>'ORPHANED' AND from_address<>decode(repeat('00',20),'hex') UNION ALL SELECT to_address,amount_raw,true,block_number,COALESCE(transaction_index,log_index),log_index,1 FROM token_transfers WHERE token_id=$1 AND status<>'ORPHANED' AND to_address<>decode(repeat('00',20),'hex'))l ORDER BY block_number,transaction_index,log_index,leg").bind(j.token_id).fetch_all(&mut*tx).await?;
        let mut balances: HashMap<Vec<u8>, U256> = HashMap::new();
        let mut warning = false;
        for r in rows {
            let n = r
                .amount_raw
                .parse::<U256>()
                .map_err(|e| decode(&e.to_string()))?;
            let b = balances.entry(r.wallet).or_default();
            if r.incoming {
                *b = b
                    .checked_add(n)
                    .ok_or_else(|| decode("holder balance overflow"))?;
            } else if n > *b {
                warning = true;
            } else {
                *b -= n;
            }
        }
        sqlx::query("DELETE FROM token_wallet_balances WHERE token_id=$1")
            .bind(j.token_id)
            .execute(&mut *tx)
            .await?;
        let zero = [0_u8; 20];
        let mut holders = 0_i64;
        for (wallet, balance) in balances {
            if balance == U256::ZERO {
                continue;
            }
            let excluded = wallet == curve || wallet == zero || factory.as_ref() == Some(&wallet);
            if !excluded {
                holders += 1;
            }
            sqlx::query("INSERT INTO token_wallet_balances(token_id,wallet_address,balance_raw,excluded_from_holder_count,exclusion_reason,calculation_version)VALUES($1,$2,$3,$4,$5,$6)").bind(j.token_id).bind(&wallet).bind(balance.to_string()).bind(excluded).bind(excluded.then_some("PROTOCOL_INFRASTRUCTURE")).bind(MARKET_CALCULATION_VERSION).execute(&mut*tx).await?;
        }
        sqlx::query(r"WITH a AS(SELECT count(*)FILTER(WHERE side='BUY') buys,count(*)FILTER(WHERE side='SELL') sells,count(DISTINCT recipient)FILTER(WHERE side='BUY') ub,count(DISTINCT actor)FILTER(WHERE side='SELL') us,COALESCE(sum(quote_amount_raw::numeric)FILTER(WHERE side='BUY'),0) bv,COALESCE(sum(quote_amount_raw::numeric)FILTER(WHERE side='SELL'),0) sv,COALESCE(sum((quote_amount_raw::numeric-fee_raw::numeric-tax_raw::numeric))FILTER(WHERE side='BUY'),0) ei,COALESCE(sum((quote_amount_raw::numeric+fee_raw::numeric+tax_raw::numeric))FILTER(WHERE side='SELL'),0) eo FROM token_trades WHERE token_id=$1 AND status<>'ORPHANED'),r AS(SELECT count(*) rc,COALESCE(sum(amount_raw::numeric),0) rv FROM curve_accounting_events WHERE token_id=$1),s AS(SELECT count(DISTINCT wallet_address)FILTER(WHERE side='BUY') sub,count(DISTINCT wallet_address)FILTER(WHERE side='SELL') sus,COALESCE(sum(quote_amount_raw::numeric)FILTER(WHERE side='BUY'),0) sb,COALESCE(sum(quote_amount_raw::numeric)FILTER(WHERE side='SELL'),0) ss FROM smart_trades st WHERE token_id=$1 AND confirmation_level IN('BUY_CONFIRMED','SELL_CONFIRMED')AND EXISTS(SELECT 1 FROM token_trades t WHERE t.id=st.token_trade_id AND t.status<>'ORPHANED'))INSERT INTO token_market_state(token_id,calculation_version,buy_count,sell_count,unique_buyers,unique_sellers,user_buy_volume_raw,user_sell_volume_raw,user_net_flow_raw,curve_effective_in_raw,curve_effective_out_raw,curve_effective_net_flow_raw,refund_count,refund_quote_total_raw,smart_unique_buyers,smart_unique_sellers,smart_buy_quote_raw,smart_sell_quote_raw,smart_net_flow_raw,raw_holder_count,integrity_status)SELECT $1,$2,buys,sells,ub,us,bv::text,sv::text,bv-sv,ei::text,eo::text,ei-eo,rc,rv::text,sub,sus,sb::text,ss::text,sb-ss,$3,$4 FROM a,r,s ON CONFLICT(token_id)DO UPDATE SET calculation_version=EXCLUDED.calculation_version,buy_count=EXCLUDED.buy_count,sell_count=EXCLUDED.sell_count,unique_buyers=EXCLUDED.unique_buyers,unique_sellers=EXCLUDED.unique_sellers,user_buy_volume_raw=EXCLUDED.user_buy_volume_raw,user_sell_volume_raw=EXCLUDED.user_sell_volume_raw,user_net_flow_raw=EXCLUDED.user_net_flow_raw,curve_effective_in_raw=EXCLUDED.curve_effective_in_raw,curve_effective_out_raw=EXCLUDED.curve_effective_out_raw,curve_effective_net_flow_raw=EXCLUDED.curve_effective_net_flow_raw,refund_count=EXCLUDED.refund_count,refund_quote_total_raw=EXCLUDED.refund_quote_total_raw,smart_unique_buyers=EXCLUDED.smart_unique_buyers,smart_unique_sellers=EXCLUDED.smart_unique_sellers,smart_buy_quote_raw=EXCLUDED.smart_buy_quote_raw,smart_sell_quote_raw=EXCLUDED.smart_sell_quote_raw,smart_net_flow_raw=EXCLUDED.smart_net_flow_raw,raw_holder_count=EXCLUDED.raw_holder_count,integrity_status=EXCLUDED.integrity_status,rebuilt_at=now()").bind(j.token_id).bind(MARKET_CALCULATION_VERSION).bind(holders).bind(if warning{"MARKET_DATA_INTEGRITY_WARNING"}else{"OK"}).execute(&mut*tx).await?;
        build_snapshots(&mut tx, j.token_id).await?;
        sqlx::query("UPDATE market_rebuild_jobs SET status=CASE WHEN generation=$2 THEN'COMPLETED'ELSE'PENDING'END,locked_at=NULL,last_error=NULL WHERE token_id=$1").bind(j.token_id).bind(j.generation).execute(&mut*tx).await?;
        tx.commit().await
    }
}
async fn build_snapshots(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    token: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM token_market_snapshots WHERE token_id=$1")
        .bind(token)
        .execute(&mut **tx)
        .await?;
    sqlx::query(r"
WITH scheduled AS(
 SELECT 'T+'||v.s||'s' kind,t.launch_time+make_interval(secs=>v.s) snapshot_at,NULL::numeric block
 FROM tokens t CROSS JOIN(VALUES(30),(60),(180),(300),(900),(1800),(3600))v(s) WHERE t.id=$1 AND t.launch_time+make_interval(secs=>v.s)<=now()
), smart_buys AS(
 SELECT 'SMART_BUY_'||row_number()OVER(ORDER BY tr.block_number,COALESCE(tr.transaction_index,tr.log_index),tr.log_index) kind,
 tr.block_time snapshot_at,tr.block_number block FROM smart_trades st JOIN token_trades tr ON tr.id=st.token_trade_id
 WHERE st.token_id=$1 AND st.side='BUY' AND st.confirmation_level='BUY_CONFIRMED' AND tr.status<>'ORPHANED' LIMIT 3
), opens AS(
 SELECT 'POSITION_OPEN' kind,p.block_time snapshot_at,p.block_number block FROM position_events p JOIN wallet_token_positions x ON x.id=p.position_id
 WHERE x.token_id=$1 AND p.event_type='OPEN_POSITION'
), targets AS(SELECT*FROM scheduled UNION ALL SELECT*FROM smart_buys UNION ALL SELECT*FROM opens),
valid AS(SELECT kind,snapshot_at,COALESCE(block,(SELECT max(block_number)FROM token_trades WHERE token_id=$1 AND block_time<=snapshot_at)) block FROM targets),
usable AS(SELECT*FROM valid WHERE block IS NOT NULL)
INSERT INTO token_market_snapshots(token_id,snapshot_kind,snapshot_at,snapshot_block,age_since_launch_ms,buy_count,sell_count,unique_buyers,unique_sellers,user_buy_volume_raw,user_sell_volume_raw,user_net_flow_raw,curve_effective_in_raw,curve_effective_out_raw,curve_effective_net_flow_raw,smart_unique_buyers,smart_unique_sellers,smart_buy_quote_raw,smart_sell_quote_raw,smart_net_flow_raw,holder_count,sellable_tokens_raw,reserved_tokens_raw,real_quote_reserve_raw,graduation_threshold_raw,curve_progress,quote_progress,spot_price_quote,curve_implied_fdv_quote,price_basis,calculation_version,state_exact,state_scope,evidence)
SELECT $1,v.kind,v.snapshot_at,v.block,(extract(epoch FROM(v.snapshot_at-t.launch_time))*1000)::bigint,
 count(*)FILTER(WHERE tr.side='BUY'),count(*)FILTER(WHERE tr.side='SELL'),count(DISTINCT tr.recipient)FILTER(WHERE tr.side='BUY'),count(DISTINCT tr.actor)FILTER(WHERE tr.side='SELL'),
 COALESCE(sum(tr.quote_amount_raw::numeric)FILTER(WHERE tr.side='BUY'),0)::text,COALESCE(sum(tr.quote_amount_raw::numeric)FILTER(WHERE tr.side='SELL'),0)::text,COALESCE(sum(CASE WHEN tr.side='BUY'THEN tr.quote_amount_raw::numeric ELSE-tr.quote_amount_raw::numeric END),0),
 COALESCE(sum(tr.quote_amount_raw::numeric-tr.fee_raw::numeric-tr.tax_raw::numeric)FILTER(WHERE tr.side='BUY'),0)::text,COALESCE(sum(tr.quote_amount_raw::numeric+tr.fee_raw::numeric+tr.tax_raw::numeric)FILTER(WHERE tr.side='SELL'),0)::text,
 COALESCE(sum(CASE WHEN tr.side='BUY'THEN tr.quote_amount_raw::numeric-tr.fee_raw::numeric-tr.tax_raw::numeric ELSE-(tr.quote_amount_raw::numeric+tr.fee_raw::numeric+tr.tax_raw::numeric)END),0),
 (SELECT count(DISTINCT st.wallet_address)FROM smart_trades st JOIN token_trades z ON z.id=st.token_trade_id WHERE st.token_id=$1 AND st.side='BUY'AND st.confirmation_level='BUY_CONFIRMED'AND z.status<>'ORPHANED'AND z.block_number<=v.block),
 (SELECT count(DISTINCT st.wallet_address)FROM smart_trades st JOIN token_trades z ON z.id=st.token_trade_id WHERE st.token_id=$1 AND st.side='SELL'AND st.confirmation_level='SELL_CONFIRMED'AND z.status<>'ORPHANED'AND z.block_number<=v.block),
 (SELECT COALESCE(sum(st.quote_amount_raw::numeric),0)::text FROM smart_trades st JOIN token_trades z ON z.id=st.token_trade_id WHERE st.token_id=$1 AND st.side='BUY'AND st.confirmation_level='BUY_CONFIRMED'AND z.status<>'ORPHANED'AND z.block_number<=v.block),
 (SELECT COALESCE(sum(st.quote_amount_raw::numeric),0)::text FROM smart_trades st JOIN token_trades z ON z.id=st.token_trade_id WHERE st.token_id=$1 AND st.side='SELL'AND st.confirmation_level='SELL_CONFIRMED'AND z.status<>'ORPHANED'AND z.block_number<=v.block),
 (SELECT COALESCE(sum(CASE WHEN st.side='BUY'THEN st.quote_amount_raw::numeric ELSE-st.quote_amount_raw::numeric END),0)FROM smart_trades st JOIN token_trades z ON z.id=st.token_trade_id WHERE st.token_id=$1 AND st.confirmation_level IN('BUY_CONFIRMED','SELL_CONFIRMED')AND z.status<>'ORPHANED'AND z.block_number<=v.block),
 (SELECT count(*)FROM(SELECT wallet,sum(delta) balance FROM(SELECT tf.to_address wallet,tf.amount_raw::numeric delta FROM token_transfers tf WHERE tf.token_id=$1 AND tf.status<>'ORPHANED'AND tf.block_number<=v.block UNION ALL SELECT tf.from_address,-tf.amount_raw::numeric FROM token_transfers tf WHERE tf.token_id=$1 AND tf.status<>'ORPHANED'AND tf.block_number<=v.block)legs GROUP BY wallet HAVING sum(delta)>0)balances WHERE wallet<>decode(repeat('00',20),'hex')AND wallet<>t.curve_address AND wallet<>COALESCE(t.factory_address,decode(repeat('00',20),'hex'))),
 o.sellable_tokens_raw,o.reserved_tokens_raw,o.real_quote_reserve_raw,o.graduation_threshold_raw,o.curve_progress,o.quote_progress,o.spot_price_quote,o.curve_implied_fdv_quote,
 CASE WHEN o.state_exact THEN'PONS_V2_GET_RESERVES_MARGINAL_V1'END,$2,COALESCE(o.state_exact,false),CASE WHEN o.state_exact THEN'BLOCK_STATE_EXACT'ELSE'UNAVAILABLE'END,
 COALESCE(o.evidence,jsonb_build_object('curve_state','UNAVAILABLE_AT_BLOCK','requested_block',v.block,'state_scope','UNAVAILABLE'))
FROM usable v JOIN tokens t ON t.id=$1
LEFT JOIN curve_state_observations o ON o.token_id=$1 AND o.block_number=v.block
LEFT JOIN token_trades tr ON tr.token_id=$1 AND tr.status<>'ORPHANED'AND tr.block_number<=v.block
GROUP BY v.kind,v.snapshot_at,v.block,t.launch_time,t.curve_address,t.factory_address,o.sellable_tokens_raw,o.reserved_tokens_raw,o.real_quote_reserve_raw,o.graduation_threshold_raw,o.curve_progress,o.quote_progress,o.spot_price_quote,o.curve_implied_fdv_quote,o.state_exact,o.evidence
").bind(token).bind(MARKET_CALCULATION_VERSION).execute(&mut**tx).await?;
    sqlx::query(r"
WITH values AS(
 SELECT st.id,tr.side,tr.quote_amount_raw::numeric quote,tr.token_amount_raw::numeric amount,
 tr.fee_raw::numeric fee,tr.tax_raw::numeric tax,
 COALESCE(mc.decimals,t.decimals) token_decimals,
 CASE WHEN t.pair_token IS NULL OR t.pair_token=decode(repeat('00',20),'hex')THEN 18 ELSE o.quote_decimals END quote_decimals,
 o.curve_progress,o.curve_implied_fdv_quote,o.state_exact,tr.block_number,tr.transaction_index,tr.log_index
 FROM smart_trades st JOIN token_trades tr ON tr.id=st.token_trade_id JOIN tokens t ON t.id=st.token_id
 LEFT JOIN token_metadata_current mc ON mc.token_id=t.id
 LEFT JOIN curve_state_observations o ON o.token_id=st.token_id AND o.block_number=tr.block_number
 WHERE st.token_id=$1 AND st.confirmation_level IN('BUY_CONFIRMED','SELL_CONFIRMED')
)
UPDATE smart_trades st SET
 entry_price_quote=CASE WHEN v.amount>0 AND v.token_decimals IS NOT NULL AND v.quote_decimals IS NOT NULL THEN v.quote*power(10::numeric,v.token_decimals)/(v.amount*power(10::numeric,v.quote_decimals))END,
 entry_net_execution_price_quote=CASE WHEN v.amount>0 AND v.token_decimals IS NOT NULL AND v.quote_decimals IS NOT NULL THEN
  (CASE WHEN v.side='BUY'THEN v.quote-v.fee-v.tax ELSE v.quote+v.fee+v.tax END)*power(10::numeric,v.token_decimals)/(v.amount*power(10::numeric,v.quote_decimals))END,
 execution_price_scope=CASE WHEN v.amount>0 AND v.token_decimals IS NOT NULL AND v.quote_decimals IS NOT NULL THEN'EVENT_POSITION_EXACT'ELSE'UNAVAILABLE'END,
 entry_curve_progress=CASE WHEN v.state_exact THEN v.curve_progress END,
 entry_implied_fdv_quote=CASE WHEN v.state_exact THEN v.curve_implied_fdv_quote END,
 entry_context_scope=CASE WHEN v.state_exact THEN'BLOCK_STATE_EXACT'ELSE'UNAVAILABLE'END,
 entry_market_state_exact=false,
 evidence=jsonb_set(st.evidence,'{market_evidence}',jsonb_build_object(
  'execution_price_scope',CASE WHEN v.amount>0 AND v.token_decimals IS NOT NULL AND v.quote_decimals IS NOT NULL THEN'EVENT_POSITION_EXACT'ELSE'UNAVAILABLE'END,
  'context_scope',CASE WHEN v.state_exact THEN'BLOCK_STATE_EXACT'ELSE'UNAVAILABLE'END,
  'context_position','END_OF_BLOCK','block_number',v.block_number,'transaction_index',v.transaction_index,'log_index',v.log_index,
  'gross_quote_raw',v.quote::text,'effective_quote_raw',(CASE WHEN v.side='BUY'THEN v.quote-v.fee-v.tax ELSE v.quote+v.fee+v.tax END)::text,'token_amount_raw',v.amount::text
 ),true)
FROM values v WHERE st.id=v.id
").bind(token).execute(&mut**tx).await?;
    sqlx::query(r"
WITH first_buy AS(
 SELECT DISTINCT ON(st.trader_wallet_id)st.trader_wallet_id,st.entry_price_quote,st.entry_net_execution_price_quote,
 st.execution_price_scope,st.entry_curve_progress,st.entry_implied_fdv_quote,st.entry_context_scope
 FROM smart_trades st JOIN token_trades tr ON tr.id=st.token_trade_id
 WHERE st.token_id=$1 AND st.side='BUY'AND st.confirmation_level='BUY_CONFIRMED'AND tr.status<>'ORPHANED'
 ORDER BY st.trader_wallet_id,tr.block_number,COALESCE(tr.transaction_index,tr.log_index),tr.log_index,st.id
)
UPDATE wallet_token_positions p SET first_entry_price=f.entry_price_quote,
 first_entry_net_execution_price=f.entry_net_execution_price_quote,first_entry_price_scope=f.execution_price_scope,
 first_entry_curve_progress=f.entry_curve_progress,first_entry_market_cap=f.entry_implied_fdv_quote,
 first_entry_market_scope=f.entry_context_scope FROM first_buy f
WHERE p.token_id=$1 AND p.trader_wallet_id=f.trader_wallet_id
").bind(token).execute(&mut**tx).await?;
    Ok(())
}
fn decode(v: &str) -> sqlx::Error {
    sqlx::Error::Decode(Box::new(std::io::Error::other(v.to_owned())))
}
