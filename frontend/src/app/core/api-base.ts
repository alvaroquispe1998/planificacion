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

  if (typeof window !== 'undefined' && window.location) {
    const protocol = window.location.protocol === 'https:' ? 'https:' : 'http:';
    const hostname =
      window.location.hostname && window.location.hostname !== 'localhost'
        ? window.location.hostname
        : '127.0.0.1';
    return `${protocol}//${hostname}:3000`;
  }

  return 'http://127.0.0.1:3000';
}

export const API_BASE_URL = resolveApiBaseUrl();
