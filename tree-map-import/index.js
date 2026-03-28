#!/usr/bin/env node
import { syncCategoryTree } from "./sync-tree.js";

(async () => {
  try {
    await syncCategoryTree();
    console.log("🎉 Category tree sync complete");
  } catch (err) {
    console.error("🔥 Category tree sync failed:");
    console.error(err?.message || err);
    process.exit(1);
  }
})();
