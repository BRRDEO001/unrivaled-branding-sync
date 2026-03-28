import { AMROD_AUTH_DETAILS, AMROD_AUTH_ENDPOINT, AMROD_CATEGORIES_ENDPOINT } from './config.js';

export const fetchAmrodToken = async () => {
  const res = await fetch(AMROD_AUTH_ENDPOINT, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(AMROD_AUTH_DETAILS)
  });
  if (!res.ok) throw new Error(`Amrod Auth failed: ${res.status}`);
  const data = await res.json();
  if (!data.token) throw new Error('No token returned from Amrod');
  return data.token;
};

export const fetchAmrodCategories = async (token) => {
  const res = await fetch(AMROD_CATEGORIES_ENDPOINT, {
    headers: { Authorization: `Bearer ${token}` }
  });
  if (!res.ok) throw new Error(`Failed to fetch categories: ${res.status}`);
  return res.json();
};
