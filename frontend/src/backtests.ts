export type BacktestMode='KNOWLEDGE_TIME'|'EVENT_TIME_RECONSTRUCTED';
export function modeLabel(mode:BacktestMode){return mode==='KNOWLEDGE_TIME'?'Knowledge-Time · Predictive Validation':'Retrospective Research Only'}
export function sampleLabel(status:unknown,n:unknown){return status==='INSUFFICIENT_SAMPLE'?`Insufficient sample (N=${String(n)})`:`N=${String(n)}`}
export function productionIsolation(value:Record<string,unknown>){return value.production_signal_unchanged===true&&value.candidate_configuration_only===true}
export function sampleSemantics(){return{sampleUnit:'Unique Token / First State Entry',outcomeAnchor:'Decision Time',baseline:'Age Matched'}as const}
