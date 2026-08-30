import { QueryClient, QueryObserver, type QueryKey } from '@tanstack/query-core';
import { useEffect, useMemo, useState } from 'preact/hooks';

export const queryClient = new QueryClient({ defaultOptions: { queries: { staleTime: 15_000, retry: 1 } } });

export interface QueryState<T> { data?: T; error?: Error; loading: boolean; }

export function useQuery<T>(key: QueryKey, queryFn: () => Promise<T>, enabled = true): QueryState<T> {
  const observer = useMemo(() => new QueryObserver<T>(queryClient, { queryKey: key, queryFn, enabled }), [JSON.stringify(key), enabled]);
  const [state, setState] = useState<QueryState<T>>(() => {
    const current = observer.getCurrentResult();
    return { data: current.data, error: current.error as Error | undefined, loading: current.isPending };
  });
  useEffect(() => observer.subscribe((result) => setState({ data: result.data, error: result.error as Error | undefined, loading: result.isPending })), [observer]);
  return state;
}

export function invalidateForEvent(type: string, data: unknown) {
  const payload = data && typeof data === 'object' ? data as Record<string, unknown> : {};
  void queryClient.invalidateQueries({ queryKey: ['dashboard'] });
  if (type.startsWith('alert.')) void queryClient.invalidateQueries({ queryKey: ['alerts'] });
  if (type.startsWith('system.')) void queryClient.invalidateQueries({ queryKey: ['system'] });
  if (type.startsWith('content.')) {
    void queryClient.invalidateQueries({ predicate: (q) => ['token', 'trader'].includes(String(q.queryKey[0])) });
  }
  const token = payload.token ?? payload.token_address;
  if (token) void queryClient.invalidateQueries({ predicate: (q) => q.queryKey.includes(String(token)) });
  if (type.startsWith('token.') || type.startsWith('trade.') || type.startsWith('smart_trade.') || type.startsWith('position.') || type.startsWith('signal.')) {
    void queryClient.invalidateQueries({ queryKey: ['tokens'] });
  }
}
