import { describe, expect, it } from 'vitest';
import { aiResearchViewModel } from './research';

describe('AI research presentation model', () => {
  it('keeps current report, immutable history, and chain signal separate', () => {
    const current = { id: 'r2', summary: 'Current structured report', confidence: 62 };
    const history = [current, { id: 'r1', summary: 'Earlier report', confidence: 41 }];
    const view = aiResearchViewModel({
      current,
      history,
      job: { status: 'SUCCEEDED' },
      use_ai_research_in_signal: false,
    });
    expect(view.current).toBe(current);
    expect(view.history).toHaveLength(2);
    expect(view.chainSignalIndependent).toBe(true);
    expect(view.empty).toBe(false);
  });

  it('represents queued and empty states without inventing a report', () => {
    const view = aiResearchViewModel({
      current: null,
      history: [],
      job: { status: 'PENDING' },
      use_ai_research_in_signal: false,
    });
    expect(view.empty).toBe(true);
    expect(view.job?.status).toBe('PENDING');
  });
});
