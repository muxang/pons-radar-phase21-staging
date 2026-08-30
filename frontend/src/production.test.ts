import { describe, expect, it } from 'vitest';
import { displayDecimal, safeExternalUrl, safeImageUrl, short } from './api';

describe('production UI safety helpers', () => {
  it('allows only explicit HTTP(S) external links', () => {
    expect(safeExternalUrl('https://example.com/a')).toBe('https://example.com/a');
    expect(safeExternalUrl('javascript:alert(1)')).toBeNull();
    expect(safeExternalUrl('data:text/html,<script>alert(1)</script>')).toBeNull();
    expect(safeExternalUrl('not a url')).toBeNull();
  });

  it('normalizes IPFS token images without allowing active URL schemes', () => {
    expect(safeImageUrl('ipfs://bafybeigdyrzt/image.png')).toBe('https://ipfs.io/ipfs/bafybeigdyrzt/image.png');
    expect(safeImageUrl('ipfs://ipfs/QmHash/logo.webp')).toBe('https://ipfs.io/ipfs/QmHash/logo.webp');
    expect(safeImageUrl('javascript:alert(1)')).toBeNull();
  });

  it('renders untrusted text and exact decimal strings without numeric coercion', () => {
    const malicious = '<img src=x onerror=alert(1)>';
    expect(short(malicious, 100)).toBe(malicious);
    expect(displayDecimal('115792089237316195423570985008687907853269984665640564039457584007913129639935')).toBe('115792089237316195423570985008687907853269984665640564039457584007913129639935');
    expect(displayDecimal(null)).toBe('—');
  });
});
