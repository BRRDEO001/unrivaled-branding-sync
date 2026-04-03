import {
  AMROD_AUTH_DETAILS,
  AMROD_AUTH_ENDPOINT,
  AMROD_CATEGORIES_ENDPOINT,
  AMROD_CATEGORIES_UPDATED_ENDPOINT,
} from './config.js';

function categoriesUrlWithCode(baseUrl) {
  const code = String(AMROD_AUTH_DETAILS?.CustomerCode || '').trim();
  if (!code) {
    throw new Error('AMROD_CUSTOMER_CODE required for Amrod Categories API');
  }
  const sep = baseUrl.includes('?') ? '&' : '?';
  return `${baseUrl}${sep}CustomerCode=${encodeURIComponent(code)}`;
}

export const fetchAmrodToken = async () => {
  const res = await fetch(AMROD_AUTH_ENDPOINT, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(AMROD_AUTH_DETAILS)
  });
  if (!res.ok) throw new Error(`Amrod Auth failed: ${res.status}`);
  const data = await res.json();
  const tok = data?.token ?? data?.Token;
  if (!tok) throw new Error('No token returned from Amrod');
  return tok;
};

export const fetchAmrodCategories = async (token) => {
  const res = await fetch(AMROD_CATEGORIES_ENDPOINT, {
    headers: { Authorization: `Bearer ${token}` }
  });
  if (!res.ok) throw new Error(`Failed to fetch categories: ${res.status}`);
  return res.json();
};

/** Uses GetUpdated; if anything changed, caller should run full Categories sync. */
export const fetchUpdatedCategories = async (token) => {
  const url = categoriesUrlWithCode(AMROD_CATEGORIES_UPDATED_ENDPOINT);
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' }
  });
  if (res.status === 204) return [];
  if (!res.ok) throw new Error(`Categories GetUpdated failed: ${res.status}`);
  const text = await res.text();
  if (!text?.trim()) return [];
  const data = JSON.parse(text);
  if (Array.isArray(data)) return data;
  const inner = data?.Categories ?? data?.categories ?? data?.items;
  return Array.isArray(inner) ? inner : [];
};
