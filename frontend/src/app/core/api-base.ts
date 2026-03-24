import { environment } from '../../environments/environment';

declare global {
  interface Window {
    __UAI_API_BASE_URL__?: string;
  }
}

function normalizeBaseUrl(value: string) {
  return value.endsWith('/') ? value.slice(0, -1) : value;
}

function isAbsoluteUrl(value: string) {
  return /^https?:\/\//i.test(value);
}

function isLocalhostUrl(value: string) {
  return /^https?:\/\/(localhost|127\.0\.0\.1|0\.0\.0\.0)(:\d+)?(\/|$)/i.test(value);
}

function resolveApiBaseUrl() {
  const override = typeof window !== 'undefined' ? window.__UAI_API_BASE_URL__?.trim() ?? '' : '';
  const envBaseUrl = environment?.apiBaseUrl?.trim() ?? '';

  if (environment?.production && envBaseUrl) {
    const normalizedEnv = normalizeBaseUrl(envBaseUrl);
    if (!override) {
      return normalizedEnv;
    }
    const normalizedOverride = normalizeBaseUrl(override);
    if (!isAbsoluteUrl(normalizedOverride) || isLocalhostUrl(normalizedOverride)) {
      return normalizedEnv;
    }
    return normalizedOverride;
  }

  if (override) {
    return normalizeBaseUrl(override);
  }
  if (envBaseUrl) {
    return normalizeBaseUrl(envBaseUrl);
  }
  return '/api';
}

export const API_BASE_URL = resolveApiBaseUrl();
