import type { Context } from "@resonatehq/sdk";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface Record {
  id: string;
  name: string;
  email: string;
  value: number;
}

export interface BatchResult {
  batchIndex: number;
  processed: number;
  skipped: number;
  durationMs: number;
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Track batch attempts for crash demo
const batchAttempts = new Map<number, number>();

// ---------------------------------------------------------------------------
// Generate a deterministic set of fake import records
// ---------------------------------------------------------------------------

export function generateRecords(count: number): Record[] {
  return Array.from({ length: count }, (_, i) => ({
    id: `rec_${String(i + 1).padStart(4, "0")}`,
    name: `User ${i + 1}`,
    email: `user${i + 1}@example.com`,
    value: Math.floor(Math.random() * 1000) + 1,
  }));
}

// ---------------------------------------------------------------------------
// Process a single batch of records
// Each batch is an independent durable checkpoint.
// If we crash at batch 3, batches 0-2 are already checkpointed.
// On resume, Resonate replays them from storage — no DB re-writes.
// ---------------------------------------------------------------------------

export async function processBatchChunk(
  _ctx: Context,
  batchIndex: number,
  records: Record[],
  crashAtBatch: number,
): Promise<BatchResult> {
  const start = Date.now();
  const attempt = (batchAttempts.get(batchIndex) ?? 0) + 1;
  batchAttempts.set(batchIndex, attempt);

  console.log(
    `  [batch ${String(batchIndex).padStart(2, "0")}] Processing ${records.length} records...` +
      (attempt > 1 ? ` (retry ${attempt})` : ""),
  );

  // Simulate DB write latency
  await sleep(150);

  if (crashAtBatch === batchIndex && attempt === 1) {
    // Simulate a crash mid-batch (DB timeout, OOM, etc.)
    // Resonate retries this batch. Earlier batches are NOT re-processed.
    throw new Error(`Batch ${batchIndex} failed — simulated DB timeout`);
  }

  const skipped = records.filter((r) => r.value <= 0).length;
  console.log(
    `  [batch ${String(batchIndex).padStart(2, "0")}] Done — ${records.length - skipped} imported, ${skipped} skipped`,
  );

  return {
    batchIndex,
    processed: records.length - skipped,
    skipped,
    durationMs: Date.now() - start,
  };
}
