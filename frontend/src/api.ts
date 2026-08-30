export class ApiError extends Error {
  constructor(public readonly status: number, message: string) { super(message); }
}

import { mutationBlocked } from './upgrade';

export function csrfToken() {
  return document.cookie.split('; ').find((value) => value.startsWith('pons_csrf='))?.split('=')[1];
}

export async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  if (mutationBlocked(path, init.method)) {
    window.dispatchEvent(new CustomEvent('pons:refresh-required'));
    throw new ApiError(409, 'Frontend refresh required before administrative mutations');
  }
  const headers = new Headers(init.headers);
  if (init.body && !headers.has('content-type')) headers.set('content-type', 'application/json');
  if (init.method && !['GET', 'HEAD'].includes(init.method.toUpperCase())) {
    const csrf = csrfToken();
    if (csrf) headers.set('x-csrf-token', decodeURIComponent(csrf));
  }
  const response = await fetch(`/api/v1${path}`, { ...init, headers, credentials: 'same-origin' });
  if (response.status === 401) window.dispatchEvent(new CustomEvent('pons:unauthorized'));
  if (!response.ok) {
    const body = await response.json().catch(() => ({ error: `HTTP ${response.status}` })) as { error?: string };
    throw new ApiError(response.status, body.error ?? `HTTP ${response.status}`);
  }
  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
}

export function safeExternalUrl(raw: unknown): string | null {
  if (typeof raw !== 'string' || raw.length > 2048) return null;
  try {
    const value = new URL(raw);
    return value.protocol === 'https:' || value.protocol === 'http:' ? value.href : null;
  } catch { return null; }
}

export function short(value: unknown, width = 10) {
  const text = String(value ?? '—');
  return text.length > width * 2 ? `${text.slice(0, width)}…${text.slice(-width)}` : text;
}

export function displayDecimal(value: unknown, fallback = '—') {
  if (value === null || value === undefined || value === '') return fallback;
  return String(value);
}
