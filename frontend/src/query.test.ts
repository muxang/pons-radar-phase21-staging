import { afterEach, describe, expect, it, vi } from 'vitest';
import { REALTIME_INVALIDATION_WINDOW_MS, invalidateForEvent, queryClient } from './query';

describe('realtime query invalidation', () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('coalesces a burst of trade events into one tokens and dashboard refresh', () => {
    vi.useFakeTimers();
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries').mockResolvedValue();

    for (let index = 0; index < 100; index += 1) {
      invalidateForEvent('trade.buy', { token: '0xabc', index });
    }

    expect(invalidate).not.toHaveBeenCalled();
    vi.advanceTimersByTime(REALTIME_INVALIDATION_WINDOW_MS);
    expect(invalidate).toHaveBeenCalledTimes(3);
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['dashboard'] });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['tokens'] });
    expect(invalidate.mock.calls.filter(([filter]) => filter !== undefined && 'predicate' in filter)).toHaveLength(1);
  });

  it('coalesces replayed events from different domains in the same window', () => {
    vi.useFakeTimers();
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries').mockResolvedValue();

    invalidateForEvent('signal.high_priority', { token_address: '0xabc' });
    invalidateForEvent('alert.created', {});
    invalidateForEvent('system.update_applied', {});
    invalidateForEvent('content.created', { token: '0xabc' });
    vi.advanceTimersByTime(REALTIME_INVALIDATION_WINDOW_MS);

    expect(invalidate).toHaveBeenCalledTimes(6);
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['alerts'] });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['system'] });
  });
});
