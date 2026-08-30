import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const css = readFileSync(new URL('./style.css', import.meta.url), 'utf8');

describe('production design system', () => {
  it('defines the legible slate palette, typography, and responsive application shell', () => {
    expect(css).toContain('--canvas:#0b0f14');
    expect(css).toContain('--text:#f1f5f9');
    expect(css).toContain('--text2:#c1cad6');
    expect(css).toContain('--muted:#8b98a9');
    expect(css).toContain('--cyan:#79c7ee');
    expect(css).toContain('--mono:');
    expect(css).toContain('body{font-size:14px;line-height:1.5}');
    expect(css).toContain('table{font-size:12px}');
    expect(css).toContain('.app-shell{display:grid');
    expect(css).toContain('@media(max-width:720px)');
  });

  it('uses a consistent embedded icon set without external icon dependencies', () => {
    expect(css).toContain('.sidebar nav a:nth-child(1):before{mask-image:url("data:image/svg+xml');
    expect(css).toContain('.sidebar nav a:nth-child(8):before{mask-image:url("data:image/svg+xml');
  });

  it('keeps the embedded interface independent of external style and font providers', () => {
    expect(css).not.toContain('@import');
    expect(css).not.toMatch(/url\(["']?https?:\/\//);
  });
});
