import type { ComponentChildren } from 'preact';
import { displayDecimal, safeExternalUrl, short } from './api';
import { Link } from './router';

export function AsyncPanel<T>({ state, empty = 'No data', children }: { state: { data?: T; loading: boolean; error?: Error }; empty?: string; children: (data: T) => ComponentChildren }) {
  if (state.loading) return <div class="state loading">Loading authoritative data…</div>;
  if (state.error) return <div class="state error"><strong>Unable to load</strong><span>{state.error.message}</span></div>;
  if (state.data === undefined || state.data === null) return <div class="state empty">{empty}</div>;
  return <>{children(state.data)}</>;
}

export function Badge({ children, tone = 'neutral' }: { children: ComponentChildren; tone?: string }) { return <span class={`badge ${tone}`}>{children}</span>; }
export function Metric({ label, value, evidence }: { label: string; value: unknown; evidence?: string }) { return <div class="metric"><span>{label}</span><strong>{displayDecimal(value)}</strong>{evidence && <small>{evidence}</small>}</div>; }
export function Address({ value }: { value: unknown }) { const text = String(value ?? ''); return <button class="copy" title={text} onClick={() => void navigator.clipboard?.writeText(text)}>{short(text, 7)}</button>; }
export function ExternalLink({ value, label }: { value: unknown; label: string }) { const safe = safeExternalUrl(value); return safe ? <a href={safe} target="_blank" rel="noopener noreferrer">{label}</a> : <span class="unsafe" title={String(value ?? '')}>{value ? `${label} (unsafe URL)` : `${label}: unavailable`}</span>; }

export function TokenCard({ token }: { token: Record<string, unknown> }) {
  return <Link href={`/tokens/${token.address}`} class="token-card">
    <div><strong>{String(token.symbol ?? 'Unknown')}</strong><span>{String(token.name ?? short(token.address))}</span></div>
    <Badge tone={String(token.signal_state ?? token.state).toLowerCase()}>{String(token.signal_state ?? token.state ?? 'NO_SIGNAL')}</Badge>
    <div class="token-metrics"><span>Score {displayDecimal(token.score)}</span><span>Confidence {displayDecimal(token.confidence)}</span><span>Progress {displayDecimal(token.curve_progress)}</span></div>
  </Link>;
}

export interface TimelineItem { id: string; type: string; event_effective_at: string; classification_source?: string; realtime_alert_eligible?: boolean; historical?: boolean; data: Record<string, unknown>; }
const timelinePresentation: Record<string, { icon: string; label: string }> = {
  TOKEN_LAUNCHED: { icon: '✦', label: 'Token launched' }, SMART_BUY: { icon: '↗', label: 'Smart BUY' }, SMART_SELL: { icon: '↘', label: 'Smart SELL' },
  OPEN_POSITION: { icon: '◉', label: 'Position opened' }, ADD_POSITION: { icon: '+', label: 'Position added' }, REDUCE_POSITION: { icon: '−', label: 'Position reduced' }, CLOSE_POSITION: { icon: '×', label: 'Position closed' }, METADATA_CHANGED: { icon: '◇', label: 'Metadata changed' },
};
timelinePresentation.TRADER_CONTENT = { icon: 'T', label: 'Trader Content / Thesis' };
timelinePresentation.AI_RESEARCH_COMPLETED = { icon: 'AI', label: 'AI Research completed' };
export function Timeline({ items }: { items: TimelineItem[] }) { return <div class="timeline">{items.map((item) => { const view = timelinePresentation[item.type] ?? (item.type.startsWith('SIGNAL_') ? { icon: '◆', label: item.type.replace('SIGNAL_', 'Signal → ') } : { icon: '•', label: item.type }); return <article key={`${item.type}-${item.id}`}><i>{view.icon}</i><div><header><strong>{view.label}</strong>{item.historical && <Badge tone="historical">Historical / Backfilled</Badge>}</header><time>{new Date(item.event_effective_at).toLocaleString()}</time><Evidence data={item.data} compact /></div></article>; })}</div>; }

export function Evidence({ data, compact = false }: { data: Record<string, unknown>; compact?: boolean }) { const entries = Object.entries(data).filter(([, value]) => value !== null && value !== undefined); return <dl class={compact ? 'evidence compact' : 'evidence'}>{entries.slice(0, compact ? 5 : 30).map(([key, value]) => <div key={key}><dt>{key.replaceAll('_', ' ')}</dt><dd>{typeof value === 'object' ? <code>{JSON.stringify(value)}</code> : String(value)}</dd></div>)}</dl>; }

export function SparkChart({ items, field, label }: { items: Record<string, unknown>[]; field: string; label: string }) {
  const values = items.map((item) => Number(item[field] ?? 0)).filter(Number.isFinite); const max = Math.max(...values, 1); const points = values.map((value, index) => `${values.length < 2 ? 0 : index * 100 / (values.length - 1)},${40 - value * 38 / max}`).join(' ');
  return <figure class="chart"><figcaption>{label}</figcaption>{values.length ? <svg viewBox="0 0 100 42" preserveAspectRatio="none" aria-label={label}><polyline points={points} /></svg> : <div class="state empty">Unavailable</div>}</figure>;
}
