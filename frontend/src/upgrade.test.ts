import { describe, expect, it } from 'vitest';
import { claimUpgradeAnnouncement } from './alerts';
import { AutoRefreshCountdown, compareBuilds, mutationBlocked, setMutationRefreshLevel, shouldAutoRefresh, UnsavedChangesRegistry } from './upgrade';

const client = { app_version: '1.0.0', frontend_build_id: 'old', api_schema_version: 1 };
const server = (build = 'old', schema = 1) => ({ app_version: build === 'old' ? '1.0.0' : '1.1.0', frontend_build_id: build, api_schema_version: schema });
class MemoryStorage implements Storage {
  private values = new Map<string,string>(); get length(){return this.values.size} clear(){this.values.clear()} getItem(k:string){return this.values.get(k)??null} key(i:number){return [...this.values.keys()][i]??null} removeItem(k:string){this.values.delete(k)} setItem(k:string,v:string){this.values.set(k,v)}
}

describe('safe frontend upgrade state', () => {
  it('uses build identity and treats schema mismatch as critical', () => {
    expect(compareBuilds(client, server()).level).toBe('NONE');
    expect(compareBuilds(client, server('new')).level).toBe('REQUIRED');
    expect(compareBuilds(client, server('old', 2)).level).toBe('CRITICAL');
    expect(compareBuilds(server('new'), server('new')).level).toBe('NONE');
  });
  it('rollback and ordinary reconnect semantics follow actual builds', () => {
    expect(compareBuilds(client, server()).level).toBe('NONE');
    expect(compareBuilds(client, server('rollback-different')).level).toBe('REQUIRED');
  });
  it('tracks dirty state independently per tab and covers named admin forms', () => {
    const dashboard = new UnsavedChangesRegistry(); const admin = new UnsavedChangesRegistry();
    for (const key of ['trader-edit','deployment-edit','wallet-edit','manual-content-reference','alert-preferences']) { admin.set(key,true); expect(admin.hasChanges).toBe(true); admin.set(key,false); }
    dashboard.set('dashboard',false); admin.set('trader-edit',true);
    expect(shouldAutoRefresh(true,'/dashboard',dashboard.hasChanges,false)).toBe(true);
    expect(shouldAutoRefresh(true,'/admin',admin.hasChanges,false)).toBe(false);
    expect(dashboard.hasChanges).toBe(false);
  });
  it('supports cancellation and disabled auto refresh', () => {
    expect(shouldAutoRefresh(true,'/dashboard',false,true)).toBe(false);
    expect(shouldAutoRefresh(false,'/dashboard',false,false)).toBe(false);
    expect(shouldAutoRefresh(true,'/admin/updates',false,false)).toBe(false);
  });
  it('executes a clean-page countdown exactly once', () => {
    let callback: (() => void) | undefined; let reloaded = 0; const ticks:number[]=[];
    const clock = { setInterval: (fn: TimerHandler) => { callback = fn as () => void; return 1 as unknown as number; }, clearInterval: () => undefined };
    const countdown = new AutoRefreshCountdown(1, (value) => ticks.push(value), () => reloaded++, clock);
    countdown.start(); callback?.();
    expect(ticks).toEqual([1, 0]); expect(reloaded).toBe(1);
  });
  it('blocks mutations but permits GET until matching reload clears guard', () => {
    setMutationRefreshLevel('REQUIRED');
    expect(mutationBlocked('/admin/traders','POST')).toBe(true);
    expect(mutationBlocked('/admin/traders','GET')).toBe(false);
    setMutationRefreshLevel('NONE');
    expect(mutationBlocked('/admin/traders','POST')).toBe(false);
  });
  it('lets only the alert leader claim each upgraded build once', () => {
    const shared = new MemoryStorage();
    expect(claimUpgradeAnnouncement('new',false,shared)).toBe(false);
    expect(claimUpgradeAnnouncement('new',true,shared)).toBe(true);
    expect(claimUpgradeAnnouncement('new',true,shared)).toBe(false);
  });
});
