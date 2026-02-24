import type { Context } from "@resonatehq/sdk";
import { processBatchChunk, type Record, type BatchResult } from "./processor";

// ---------------------------------------------------------------------------
// Batch Processor Workflow
// ---------------------------------------------------------------------------
// Splits a large record set into chunks and processes them durably.
//
// Each batch is an independent ctx.run() checkpoint. If the process crashes
// at batch 4 of 10, batches 0-3 are already checkpointed in Resonate's
// promise store. On resume, those batches are returned from cache without
// hitting the database again. Processing resumes at batch 4.
//
// This is the key advantage over a simple for-loop with a DB cursor:
//   Regular loop:  crash at 40% → restart from 0%, re-process 40%
//   Resonate:      crash at 40% → resume at 40%, re-process 0%
//
// Real-world: importing 10K customer records, building search indexes,
// sending bulk emails, processing payment ledgers.

export interface ProcessingResult {
  totalRecords: number;
  totalProcessed: number;
  totalSkipped: number;
  batchCount: number;
  batches: BatchResult[];
}

function chunkArray<T>(arr: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

export function* importRecords(
  ctx: Context,
  records: Record[],
  batchSize: number,
  crashAtBatch: number,
): Generator<any, ProcessingResult, any> {
  const batches = chunkArray(records, batchSize);
  const batchResults: BatchResult[] = [];

  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    // Each yield* is a durable checkpoint. On crash+resume,
    // completed batches are returned from cache — not re-processed.
    const result = yield* ctx.run(processBatchChunk, i, batch, crashAtBatch);
    batchResults.push(result);
  }

  const totalProcessed = batchResults.reduce((s, b) => s + b.processed, 0);
  const totalSkipped = batchResults.reduce((s, b) => s + b.skipped, 0);

  return {
    totalRecords: records.length,
    totalProcessed,
    totalSkipped,
    batchCount: batches.length,
    batches: batchResults,
  };
}
