import { QueryClient, QueryObserver, type QueryKey } from '@tanstack/query-core';
import { useEffect, useMemo, useState } from 'preact/hooks';

export const queryClient = new QueryClient({ defaultOptions: { queries: { staleTime: 15_000, retry: 1 } } });

export interface QueryState<T> { data?: T; error?: Error; loading: boolean; }

type InvalidationScope = 'dashboard' | 'alerts' | 'system' | 'content' | 'tokens';
const pendingScopes = new Set<InvalidationScope>();
const pendingTokens = new Set<string>();
let invalidationTimer: ReturnType<typeof setTimeout> | undefined;

export const REALTIME_INVALIDATION_WINDOW_MS = 1_500;

export function useQuery<T>(key: QueryKey, queryFn: () => Promise<T>, enabled = true): QueryState<T> {
  const observer = useMemo(() => new QueryObserver<T>(queryClient, { queryKey: key, queryFn, enabled }), [JSON.stringify(key), enabled]);
  const [state, setState] = useState<QueryState<T>>(() => {
    const current = observer.getCurrentResult();
    return { data: current.data, error: current.error as Error | undefined, loading: current.isPending };
  });
  useEffect(() => observer.subscribe((result) => setState({ data: result.data, error: result.error as Error | undefined, loading: result.isPending })), [observer]);
  return state;
}

function flushRealtimeInvalidations() {
  invalidationTimer = undefined;
  const scopes = new Set(pendingScopes);
  const tokens = new Set(pendingTokens);
  pendingScopes.clear();
  pendingTokens.clear();
  if (scopes.has('dashboard')) void queryClient.invalidateQueries({ queryKey: ['dashboard'] });
  if (scopes.has('alerts')) void queryClient.invalidateQueries({ queryKey: ['alerts'] });
  if (scopes.has('system')) void queryClient.invalidateQueries({ queryKey: ['system'] });
  if (scopes.has('content')) {
    void queryClient.invalidateQueries({ predicate: (q) => ['token', 'trader'].includes(String(q.queryKey[0])) });
  }
  if (tokens.size) {
    void queryClient.invalidateQueries({ predicate: (q) => [...tokens].some((token) => q.queryKey.includes(token)) });
  }
  if (scopes.has('tokens')) void queryClient.invalidateQueries({ queryKey: ['tokens'] });
}

export function invalidateForEvent(type: string, data: unknown) {
  const payload = data && typeof data === 'object' ? data as Record<string, unknown> : {};
  pendingScopes.add('dashboard');
  if (type.startsWith('alert.')) pendingScopes.add('alerts');
  if (type.startsWith('system.')) pendingScopes.add('system');
  if (type.startsWith('content.')) pendingScopes.add('content');
  const token = payload.token ?? payload.token_address;
  if (token) pendingTokens.add(String(token));
  if (type.startsWith('token.') || type.startsWith('trade.') || type.startsWith('smart_trade.') || type.startsWith('position.') || type.startsWith('signal.')) {
    pendingScopes.add('tokens');
  }
  if (invalidationTimer === undefined) invalidationTimer = setTimeout(flushRealtimeInvalidations, REALTIME_INVALIDATION_WINDOW_MS);
}
