import { environment } from '../../environments/environment';

declare global {
  interface Window {
    __UAI_API_BASE_URL__?: string;
  }
}

function normalizeBaseUrl(value: string) {
  return value.endsWith('/') ? value.slice(0, -1) : value;
}

function resolveApiBaseUrl() {
  const override =
    typeof window !== 'undefined' ? window.__UAI_API_BASE_URL__?.trim() ?? '' : '';
  if (override) {
    return normalizeBaseUrl(override);
  }
  const envBaseUrl = environment?.apiBaseUrl?.trim() ?? '';
  if (envBaseUrl) {
    return normalizeBaseUrl(envBaseUrl);
  }
  return '/api';
}

export const API_BASE_URL = resolveApiBaseUrl();
