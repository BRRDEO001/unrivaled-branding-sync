export const VAT_RATE = 0.15; // 15%

export const MARKUP_BRACKETS = [
  { min: 0, max: 50, pct: 130 },
  { min: 51, max: 100, pct: 100 },
  { min: 100.01, max: 200, pct: 95 },
  { min: 200.01, max: 500, pct: 90 },
  { min: 500.01, max: 1000, pct: 80 },
  { min: 1000.01, max: 3000, pct: 70 },
  { min: 3000.01, max: 5000, pct: 60 },
  { min: 5000.01, max: 10000, pct: 50 },
  { min: 10000.01, max: 20000, pct: 40 },
];

/**
 * What to do if price is above the highest bracket.
 * If you want a new bracket (e.g. 20000.01-50000 -> 30%), add it above instead.
 */
export const DEFAULT_MARKUP_PCT_ABOVE_MAX = 40;
