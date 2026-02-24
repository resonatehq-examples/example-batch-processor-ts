import { Resonate } from "@resonatehq/sdk";
import { importRecords } from "./workflow";
import { generateRecords } from "./processor";

// ---------------------------------------------------------------------------
// Resonate setup
// ---------------------------------------------------------------------------

const resonate = new Resonate();
resonate.register(importRecords);

// ---------------------------------------------------------------------------
// Run the batch processing demo
// ---------------------------------------------------------------------------

const crashMode = process.argv.includes("--crash");
const RECORD_COUNT = 50;
const BATCH_SIZE = 10;
const CRASH_AT_BATCH = crashMode ? 3 : -1; // crash at batch 3 (30-39% done)

const records = generateRecords(RECORD_COUNT);

const batchCount = Math.ceil(RECORD_COUNT / BATCH_SIZE);

console.log("=== Batch Processor Demo ===");
console.log(
  `Mode: ${crashMode ? `CRASH (process fails at batch ${CRASH_AT_BATCH}, resumes from checkpoint)` : "HAPPY PATH (process all records)"}`,
);
console.log(
  `\n[processor]  Starting: ${RECORD_COUNT} records → ${batchCount} batches of ${BATCH_SIZE}`,
);
if (crashMode) {
  console.log(`[processor]  Crash at batch ${CRASH_AT_BATCH}, then resume\n`);
} else {
  console.log();
}

const wallStart = Date.now();

const result = await resonate.run(
  `import/${Date.now()}`,
  importRecords,
  records,
  BATCH_SIZE,
  CRASH_AT_BATCH,
);

const wallMs = Date.now() - wallStart;

console.log("\n=== Result ===");
console.log(JSON.stringify({
  totalRecords: result.totalRecords,
  totalProcessed: result.totalProcessed,
  totalSkipped: result.totalSkipped,
  batchCount: result.batchCount,
  wallTimeMs: wallMs,
}, null, 2));

if (crashMode) {
  console.log(
    `\nNotice: batches 0-${CRASH_AT_BATCH - 1} each logged once (completed before crash).`,
    `\nBatch ${CRASH_AT_BATCH} failed → retried → succeeded.`,
    "\nBatches before the crash were NOT re-processed.",
  );
}
