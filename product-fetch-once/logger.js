// logger.js
import fs from "fs";
import path from "path";

const LOG_DIR = path.resolve(process.cwd(), "logs");

function ensureDir() {
  if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });
}

function appendJsonl(filename, payload) {
  ensureDir();
  const filePath = path.join(LOG_DIR, filename);
  const line = JSON.stringify({ ts: new Date().toISOString(), ...payload }) + "\n";
  fs.appendFileSync(filePath, line, "utf8");
}

export function logProductFailure({ amrod, stage, error, extra = {} }) {
  appendJsonl("failed-products.jsonl", {
    stage,
    amrodCode: amrod?.fullCode || amrod?.simpleCode || null,
    productName: amrod?.productName || null,
    message: String(error?.message || error || "unknown error"),
    ...extra,
  });
}

export function logImageFailure({ amrod, error, extra = {} }) {
  appendJsonl("image-failed-products.jsonl", {
    amrodCode: amrod?.fullCode || amrod?.simpleCode || null,
    productName: amrod?.productName || null,
    message: String(error?.message || error || "unknown error"),
    ...extra,
  });
}
