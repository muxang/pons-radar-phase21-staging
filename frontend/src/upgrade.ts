import { useEffect } from 'preact/hooks';

export interface BuildIdentity {
  app_version: string;
  frontend_build_id: string;
  api_schema_version: number;
}
export type RefreshLevel = 'NONE' | 'REQUIRED' | 'CRITICAL';
export interface RefreshRequirement {
  level: RefreshLevel;
  client: BuildIdentity;
  server: BuildIdentity;
}
export const FRONTEND_UPDATE_CONFIG = {
  auto_refresh_read_only_pages: (import.meta.env.VITE_AUTO_REFRESH_READ_ONLY_PAGES ?? 'true') === 'true',
  auto_refresh_countdown_seconds: Number(import.meta.env.VITE_AUTO_REFRESH_COUNTDOWN_SECONDS ?? '10'),
  block_auto_refresh_when_unsaved: true,
};

export function compareBuilds(client: BuildIdentity, server: BuildIdentity): RefreshRequirement {
  const schemaMismatch = client.api_schema_version !== server.api_schema_version;
  const buildMismatch = client.frontend_build_id !== server.frontend_build_id;
  return { level: schemaMismatch ? 'CRITICAL' : buildMismatch ? 'REQUIRED' : 'NONE', client, server };
}

export class UnsavedChangesRegistry {
  private readonly dirty = new Set<string>();
  private readonly listeners = new Set<(dirty: boolean) => void>();
  set(key: string, value: boolean) {
    value ? this.dirty.add(key) : this.dirty.delete(key);
    this.listeners.forEach((listener) => listener(this.hasChanges));
  }
  clear() { this.dirty.clear(); this.listeners.forEach((listener) => listener(false)); }
  get hasChanges() { return this.dirty.size > 0; }
  get keys() { return [...this.dirty]; }
  subscribe(listener: (dirty: boolean) => void) { this.listeners.add(listener); return () => this.listeners.delete(listener); }
}
export const unsavedChanges = new UnsavedChangesRegistry();

export function useUnsavedChanges(key: string, dirty: boolean) {
  useEffect(() => { unsavedChanges.set(key, dirty); return () => unsavedChanges.set(key, false); }, [key, dirty]);
}

let refreshLevel: RefreshLevel = 'NONE';
export function setMutationRefreshLevel(level: RefreshLevel) { refreshLevel = level; }
export function mutationBlocked(path: string, method = 'GET') {
  const mutation = !['GET', 'HEAD', 'OPTIONS'].includes(method.toUpperCase());
  return refreshLevel !== 'NONE' && mutation && !path.startsWith('/auth/');
}

export const readOnlyPath = (path: string) => !path.startsWith('/admin');

export class AutoRefreshCountdown {
  private remaining: number;
  private timer?: ReturnType<typeof setInterval>;
  constructor(
    seconds: number,
    private readonly tick: (remaining: number) => void,
    private readonly reload: () => void,
    private readonly intervals: Pick<typeof globalThis, 'setInterval' | 'clearInterval'> = globalThis,
  ) { this.remaining = seconds; }
  start() {
    this.tick(this.remaining);
    this.timer = this.intervals.setInterval(() => {
      this.remaining -= 1; this.tick(this.remaining);
      if (this.remaining <= 0) { this.cancel(); this.reload(); }
    }, 1000);
  }
  cancel() { if (this.timer !== undefined) this.intervals.clearInterval(this.timer); this.timer = undefined; }
}

export function shouldAutoRefresh(enabled: boolean, path: string, dirty: boolean, cancelled: boolean) {
  return enabled && readOnlyPath(path) && !dirty && !cancelled;
}

export class UpgradeBroadcast {
  private readonly channel?: BroadcastChannel;
  constructor(onDetected: (server: BuildIdentity) => void) {
    if (typeof BroadcastChannel !== 'undefined') {
      this.channel = new BroadcastChannel('pons.upgrade');
      this.channel.onmessage = (event) => onDetected(event.data as BuildIdentity);
    }
  }
  detected(server: BuildIdentity) { this.channel?.postMessage(server); }
  close() { this.channel?.close(); }
}
