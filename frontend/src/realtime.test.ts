import { describe, expect, it } from 'vitest';
import { SequenceStore, reconnectDelay, type EventEnvelope } from './realtime';

const event = (seq: number): EventEnvelope => ({ seq, type: 'fixture', schema_version: 1, server_version: '1', frontend_build_id: 'x', timestamp: '', data: {} });

describe('sequence replay semantics', () => {
  it('deduplicates replay/live overlap and advances monotonically', () => {
    const persisted: number[] = [];
    const store = new SequenceStore(100, (seq) => persisted.push(seq));
    expect(store.accept(event(101))).toBe(true);
    expect(store.accept(event(101))).toBe(false);
    expect(store.accept(event(100))).toBe(false);
    expect(store.accept(event(102))).toBe(true);
    expect(store.lastSeen).toBe(102);
    expect(persisted).toEqual([101, 102]);
  });
  it('keeps independent per-tab cursors', () => {
    const tabA = new Map<string,string>(); const tabB = new Map<string,string>();
    const a = new SequenceStore(Number(tabA.get('seq')??0),v=>tabA.set('seq',String(v)));
    const b = new SequenceStore(Number(tabB.get('seq')??0),v=>tabB.set('seq',String(v)));
    a.accept(event(9)); expect(a.lastSeen).toBe(9); expect(b.lastSeen).toBe(0);
  });
  it('uses capped exponential reconnect delay with bounded jitter', () => {
    expect(reconnectDelay(0, 0.5)).toBe(1000);
    expect(reconnectDelay(1, 0.5)).toBe(2000);
    expect(reconnectDelay(5, 0.5)).toBe(30000);
    expect(reconnectDelay(99, 0)).toBe(24000);
    expect(reconnectDelay(99, 1)).toBe(36000);
  });
});
