import {describe,expect,it} from 'vitest';
import {canInstallUpdate,rollbackWarning,updateProgressLabel} from './updates';
describe('secure updater controls',()=>{
 it('enables only fully verified compatible releases',()=>expect(canInstallUpdate({signature:'VALID',schema_compatible:true,rollback_compatible:true,install_allowed:true},false)).toBe(true));
 it('blocks invalid signature, unsafe rollback, and an active job',()=>{expect(canInstallUpdate({signature:'INVALID',schema_compatible:true,rollback_compatible:true,install_allowed:true},false)).toBe(false);expect(canInstallUpdate({signature:'VALID',schema_compatible:true,rollback_compatible:false,install_allowed:true},false)).toBe(false);expect(canInstallUpdate({signature:'VALID',schema_compatible:true,rollback_compatible:true,install_allowed:true},true)).toBe(false)});
 it('explains forward-only rollback risk',()=>expect(rollbackWarning({rollback_compatible:false})).toContain('database migration'));
 it('shows expected restart lifecycle instead of generic outage',()=>{expect(updateProgressLabel('DOWNLOADING')).toBe('Downloading');expect(updateProgressLabel('VERIFYING_HEALTH')).toBe('Health Check')});
});
