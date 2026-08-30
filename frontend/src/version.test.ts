import { describe, expect, it } from 'vitest';
import { CLIENT_API_SCHEMA_VERSION, CLIENT_APP_VERSION, CLIENT_BUILD_ID } from './version';

describe('embedded client identifiers', () => {
  it('always exposes non-empty build identifiers', () => {
    expect(CLIENT_APP_VERSION.length).toBeGreaterThan(0);
    expect(CLIENT_BUILD_ID.length).toBeGreaterThan(0);
    expect(Number.isInteger(CLIENT_API_SCHEMA_VERSION)).toBe(true);
  });
});
