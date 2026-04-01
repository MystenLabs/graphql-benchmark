/**
 * Database queries for bloom filter analysis.
 */

import { query, queryScalar, hexToByteaParam } from "./connection";
import { getCheckpointRangesBatch } from "./queries";
import {
  computeBlockedPositions,
  buildBlockedBloomCondition,
} from "../bloom/positions";
import { CHECKPOINTS_PER_BLOCK } from "../bloom/constants";
import type { ScanTarget, TxKeyType } from "../types";
import { buildEventMatchSql } from "./event-queries";

// ── Transaction verification tables ──────────────────────────────────

/** Table and column mapping for tx key types */
const TX_TABLE_MAP: Record<TxKeyType, { table: string; column: string }> = {
  package: { table: "tx_calls", column: "package" },
  object: { table: "tx_affected_objects", column: "affected" },
  address: { table: "tx_affected_addresses", column: "affected" },
  sender: { table: "tx_affected_addresses", column: "sender" },
};

/** Tables that have sender column - used for sender-only queries */
const SENDER_TABLES = [
  { table: "tx_affected_addresses", column: "sender" },
  { table: "tx_affected_objects", column: "sender" },
  { table: "tx_calls", column: "sender" },
];

/**
 * Build SQL to find transactions matching a key, handling sender specially.
 */
function buildTxMatchSql(
  keyType: TxKeyType,
  keyClean: string,
  minTx: number,
  maxTx: number,
): { sql: string; params: unknown[] } {
  if (keyType === "sender") {
    // Sender requires checking all tables that track sender
    const unions = SENDER_TABLES.map(
      ({ table, column }) =>
        `SELECT tx_sequence_number FROM ${table} WHERE ${column} = decode($1, 'hex') AND tx_sequence_number >= $2 AND tx_sequence_number < $3`
    ).join(" UNION ");
    return {
      sql: `SELECT DISTINCT tx_sequence_number FROM (${unions}) t`,
      params: [keyClean, minTx, maxTx],
    };
  }

  const { table, column } = TX_TABLE_MAP[keyType];
  return {
    sql: `SELECT DISTINCT tx_sequence_number FROM ${table} WHERE ${column} = decode($1, 'hex') AND tx_sequence_number >= $2 AND tx_sequence_number < $3`,
    params: [keyClean, minTx, maxTx],
  };
}

// ── Unified match SQL dispatch ───────────────────────────────────────

/**
 * Parse a function filter value "0xpkg::mod::func" into components.
 */
function parseFunctionKey(value: string): {
  pkg: string;
  module?: string;
  func?: string;
} {
  const idx = value.indexOf("::");
  if (idx < 0) {
    return { pkg: hexToByteaParam(value) };
  }
  const pkg = hexToByteaParam(value.slice(0, idx));
  const rest = value.slice(idx + 2);

  const idx2 = rest.indexOf("::");
  if (idx2 < 0) {
    return { pkg, module: rest };
  }

  return { pkg, module: rest.slice(0, idx2), func: rest.slice(idx2 + 2) };
}

/**
 * Build SQL to find transactions matching a function filter.
 * Supports: "0xpkg", "0xpkg::module", "0xpkg::module::function"
 */
function buildFunctionMatchSql(
  value: string,
  minTx: number,
  maxTx: number,
): { sql: string; params: unknown[] } {
  const parts = parseFunctionKey(value);
  if (parts.func) {
    return {
      sql: `SELECT DISTINCT tx_sequence_number FROM tx_calls
            WHERE package = decode($1, 'hex') AND module = $2 AND function = $3
              AND tx_sequence_number >= $4 AND tx_sequence_number < $5`,
      params: [parts.pkg, parts.module, parts.func, minTx, maxTx],
    };
  }
  if (parts.module) {
    return {
      sql: `SELECT DISTINCT tx_sequence_number FROM tx_calls
            WHERE package = decode($1, 'hex') AND module = $2
              AND tx_sequence_number >= $3 AND tx_sequence_number < $4`,
      params: [parts.pkg, parts.module, minTx, maxTx],
    };
  }
  return {
    sql: `SELECT DISTINCT tx_sequence_number FROM tx_calls
          WHERE package = decode($1, 'hex')
            AND tx_sequence_number >= $2 AND tx_sequence_number < $3`,
    params: [parts.pkg, minTx, maxTx],
  };
}

/**
 * Build SQL to find transactions matching a filter field + value.
 * Dispatches to tx or event verification tables based on scan target.
 */
export function buildMatchSql(
  scan: ScanTarget,
  filterField: string,
  value: string,
  minTx: number,
  maxTx: number,
): { sql: string; params: unknown[] } {
  if (scan === "events") {
    return buildEventMatchSql(filterField, value, minTx, maxTx);
  }

  switch (filterField) {
    case "function":
      return buildFunctionMatchSql(value, minTx, maxTx);
    case "affectedObject": {
      const keyClean = hexToByteaParam(value);
      return buildTxMatchSql("object", keyClean, minTx, maxTx);
    }
    case "affectedAddress": {
      const keyClean = hexToByteaParam(value);
      return buildTxMatchSql("address", keyClean, minTx, maxTx);
    }
    case "sentAddress": {
      const keyClean = hexToByteaParam(value);
      return buildTxMatchSql("sender", keyClean, minTx, maxTx);
    }
    default:
      throw new Error(
        `Unknown TransactionFilter field: ${filterField}. ` +
          `Valid: function, affectedObject, affectedAddress, sentAddress`,
      );
  }
}

/**
 * Check if the cp_bloom_blocks table exists.
 */
export async function checkBlockedBloomTableExists(): Promise<boolean> {
  const sql = `
    SELECT EXISTS (
      SELECT FROM information_schema.tables
      WHERE table_name = 'cp_bloom_blocks'
    )
  `;
  return (await queryScalar<boolean>(sql)) ?? false;
}

/**
 * Get distribution of bloom filter sizes.
 */
export async function getBloomSizeDistribution(
  cpLo: number,
  cpHi: number,
): Promise<
  Array<{ bits: number; count: number; avgItems: number; pct: number }>
> {
  const sql = `
    SELECT
      length(bloom_filter) * 8 as bits,
      COUNT(*) as count,
      ROUND(AVG(bit_count(bloom_filter)::numeric), 1) as avg_items,
      ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct
    FROM cp_blooms
    WHERE cp_sequence_number >= $1 AND cp_sequence_number <= $2
    GROUP BY length(bloom_filter) * 8
    ORDER BY bits
  `;
  const rows = await query<{
    bits: string;
    count: string;
    avg_items: string;
    pct: string;
  }>(sql, [cpLo, cpHi]);
  return rows.map((r) => ({
    bits: Number(r.bits),
    count: Number(r.count),
    avgItems: Number(r.avg_items),
    pct: Number(r.pct),
  }));
}

/**
 * Get distribution of bloom filter densities by size.
 */
export async function getBloomDensityDistribution(
  cpLo: number,
  cpHi: number,
  sampleSize: number = 10000,
): Promise<
  Array<{
    bits: number;
    count: number;
    avgDensity: number;
    minDensity: number;
    maxDensity: number;
    p50Density: number;
    p90Density: number;
  }>
> {
  const sql = `
    WITH sampled AS (
      SELECT
        length(bloom_filter) * 8 as bits,
        bit_count(bloom_filter)::float / (length(bloom_filter) * 8) as density
      FROM cp_blooms
      WHERE cp_sequence_number >= $1 AND cp_sequence_number <= $2
      ORDER BY RANDOM()
      LIMIT $3
    )
    SELECT
      bits,
      COUNT(*) as count,
      ROUND(AVG(density)::numeric, 4) as avg_density,
      ROUND(MIN(density)::numeric, 4) as min_density,
      ROUND(MAX(density)::numeric, 4) as max_density,
      ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY density)::numeric, 4) as p50_density,
      ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY density)::numeric, 4) as p90_density
    FROM sampled
    GROUP BY bits
    ORDER BY bits
  `;
  const rows = await query<{
    bits: string;
    count: string;
    avg_density: string;
    min_density: string;
    max_density: string;
    p50_density: string;
    p90_density: string;
  }>(sql, [cpLo, cpHi, sampleSize]);

  return rows.map((r) => ({
    bits: Number(r.bits),
    count: Number(r.count),
    avgDensity: Number(r.avg_density),
    minDensity: Number(r.min_density),
    maxDensity: Number(r.max_density),
    p50Density: Number(r.p50_density),
    p90Density: Number(r.p90_density),
  }));
}

/**
 * Binary search to check if any transaction exists in [lo, hi) range.
 */
function hasTransactionInRange(
  sortedTxs: number[],
  lo: number,
  hi: number,
): boolean {
  if (sortedTxs.length === 0) return false;

  // Binary search for first tx >= lo
  let left = 0;
  let right = sortedTxs.length;
  while (left < right) {
    const mid = (left + right) >>> 1;
    if (sortedTxs[mid] < lo) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }

  // Check if the found transaction is within range
  return left < sortedTxs.length && sortedTxs[left] < hi;
}

/**
 * Count bloom matches and true positives for a filter field.
 *
 * @param scan - "transactions" or "events"
 * @param filterField - GraphQL filter field name (e.g. "function", "module")
 * @param value - The filter value
 * @param bloomConditions - SQL conditions for bloom filter bit checks
 * @param cpLo - Lower checkpoint bound
 * @param cpHi - Upper checkpoint bound
 * @param sampleSize - Max checkpoints to sample
 * @param chunkSize - Chunk size for batched verification
 */
export async function countTruePositives(
  scan: ScanTarget,
  filterField: string,
  value: string,
  bloomConditions: string,
  cpLo: number,
  cpHi: number,
  sampleSize: number = 10000,
  chunkSize: number = 1000,
): Promise<{ bloomMatches: number; truePositives: number }> {
  // Step 1: Get sampled checkpoints that match bloom filter
  const bloomSql = `
    SELECT cp_sequence_number
    FROM cp_blooms
    WHERE cp_sequence_number >= $1 AND cp_sequence_number <= $2
      AND ${bloomConditions}
    ORDER BY cp_sequence_number
    LIMIT $3
  `;
  const bloomMatchRows = await query<{ cp_sequence_number: string }>(bloomSql, [
    cpLo,
    cpHi,
    sampleSize,
  ]);
  const bloomMatchCps = bloomMatchRows.map((r) => Number(r.cp_sequence_number));

  if (bloomMatchCps.length === 0) {
    return { bloomMatches: 0, truePositives: 0 };
  }

  const bloomMatches = bloomMatchCps.length;

  // Step 2: Get checkpoint -> tx range mappings in chunks
  const truePositiveCps = new Set<number>();

  for (let i = 0; i < bloomMatchCps.length; i += chunkSize) {
    const chunkCps = bloomMatchCps.slice(i, i + chunkSize);
    const cpRanges = await getCheckpointRangesBatch(chunkCps);

    if (cpRanges.size === 0) continue;

    // Get overall tx range for this chunk
    let minTx = Infinity;
    let maxTx = -Infinity;
    for (const { lo, hi } of cpRanges.values()) {
      minTx = Math.min(minTx, lo);
      maxTx = Math.max(maxTx, hi);
    }

    // Find tx_sequence_numbers that match the key in this tx range
    const { sql: txSql, params: txParams } = buildMatchSql(
      scan,
      filterField,
      value,
      minTx,
      maxTx,
    );
    const matchingTxRows = await query<{ tx_sequence_number: string }>(
      txSql,
      txParams,
    );

    // Sort matching transactions for binary search
    const sortedTxs = matchingTxRows
      .map((r) => Number(r.tx_sequence_number))
      .sort((a, b) => a - b);

    // Use binary search to find true positives efficiently
    for (const [cp, { lo, hi }] of cpRanges) {
      if (hasTransactionInRange(sortedTxs, lo, hi)) {
        truePositiveCps.add(cp);
      }
    }
  }

  return { bloomMatches, truePositives: truePositiveCps.size };
}

/**
 * Count bloom matches and true positives for a key using blocked bloom filters.
 * Optimized with batched block queries and binary search for true positive matching.
 */
export async function countBlockedTruePositives(
  scan: ScanTarget,
  filterField: string,
  keyHex: string,
  cpLo: number,
  cpHi: number,
  sampleSize: number = 10000,
  chunkSize: number = 1000,
): Promise<{ bloomMatches: number; truePositives: number }> {

  // Group checkpoints by their cp_block_id
  const cpBlockLo = Math.floor(cpLo / CHECKPOINTS_PER_BLOCK);
  const cpBlockHi = Math.floor(cpHi / CHECKPOINTS_PER_BLOCK);

  // Batch blocks together for fewer queries
  const BLOCKS_PER_BATCH = 100;
  const bloomMatchBlockIds: number[] = [];
  let remainingSample = sampleSize;

  // Process blocks in batches
  for (
    let batchStart = cpBlockLo;
    batchStart <= cpBlockHi && remainingSample > 0;
    batchStart += BLOCKS_PER_BATCH
  ) {
    const batchEnd = Math.min(batchStart + BLOCKS_PER_BATCH - 1, cpBlockHi);

    // Build UNION ALL query for all blocks in this batch
    const unionParts: string[] = [];
    const params: unknown[] = [];
    let paramIdx = 1;

    for (let cpBlockId = batchStart; cpBlockId <= batchEnd; cpBlockId++) {
      const [blockIdx, positions] = computeBlockedPositions(keyHex, cpBlockId);
      const conditions = buildBlockedBloomCondition(blockIdx, positions);

      // Query by cp_block_id, return cp_block_id if bloom matches
      unionParts.push(`
        SELECT ${cpBlockId} as cp_block_id
        FROM cp_bloom_blocks
        WHERE cp_block_id = $${paramIdx}
          AND ${conditions}
      `);
      params.push(cpBlockId);
      paramIdx += 1;
    }

    // Combine with UNION ALL, order, and limit
    const batchSql = `
      WITH batch_matches AS (
        ${unionParts.join(" UNION ALL ")}
      )
      SELECT cp_block_id FROM batch_matches
      ORDER BY cp_block_id
      LIMIT $${paramIdx}
    `;
    params.push(remainingSample);

    const rows = await query<{ cp_block_id: string }>(batchSql, params);

    for (const row of rows) {
      bloomMatchBlockIds.push(Number(row.cp_block_id));
      remainingSample--;
      if (remainingSample <= 0) break;
    }
  }

  if (bloomMatchBlockIds.length === 0) {
    return { bloomMatches: 0, truePositives: 0 };
  }

  const bloomMatches = bloomMatchBlockIds.length;

  // Step 2: For each matching block, check if there are true positive transactions
  // A block is a true positive if ANY transaction in its checkpoint range matches
  const truePositiveBlocks = new Set<number>();

  for (let i = 0; i < bloomMatchBlockIds.length; i += chunkSize) {
    const chunkBlockIds = bloomMatchBlockIds.slice(i, i + chunkSize);

    // Get tx range for each block (block covers checkpoints blockId*1000 to blockId*1000+999)
    // We need to find the tx range for the first and last checkpoint in each block
    const blockRanges: Array<{ blockId: number; txLo: number; txHi: number }> = [];

    for (const blockId of chunkBlockIds) {
      const blockCpLo = Math.max(cpLo, blockId * CHECKPOINTS_PER_BLOCK);
      const blockCpHi = Math.min(cpHi, (blockId + 1) * CHECKPOINTS_PER_BLOCK - 1);

      // Get checkpoint ranges for this block's checkpoints
      const cpList = [];
      for (let cp = blockCpLo; cp <= blockCpHi; cp++) {
        cpList.push(cp);
      }

      const cpRanges = await getCheckpointRangesBatch(cpList);
      if (cpRanges.size === 0) continue;

      // Find overall tx range for this block
      let minTx = Infinity;
      let maxTx = -Infinity;
      for (const { lo, hi } of cpRanges.values()) {
        minTx = Math.min(minTx, lo);
        maxTx = Math.max(maxTx, hi);
      }

      if (minTx !== Infinity) {
        blockRanges.push({ blockId, txLo: minTx, txHi: maxTx });
      }
    }

    if (blockRanges.length === 0) continue;

    // Get overall tx range across all blocks in this chunk
    const overallTxLo = Math.min(...blockRanges.map((b) => b.txLo));
    const overallTxHi = Math.max(...blockRanges.map((b) => b.txHi));

    // Find tx_sequence_numbers that match the key in this tx range
    const { sql: txSql, params: txParams } = buildMatchSql(
      scan,
      filterField,
      keyHex,
      overallTxLo,
      overallTxHi,
    );
    const matchingTxRows = await query<{ tx_sequence_number: string }>(
      txSql,
      txParams,
    );

    // Sort matching transactions for binary search
    const sortedTxs = matchingTxRows
      .map((r) => Number(r.tx_sequence_number))
      .sort((a, b) => a - b);

    // Check each block for true positives
    for (const { blockId, txLo, txHi } of blockRanges) {
      if (hasTransactionInRange(sortedTxs, txLo, txHi)) {
        truePositiveBlocks.add(blockId);
      }
    }
  }

  return { bloomMatches, truePositives: truePositiveBlocks.size };
}

/**
 * Count bloom matches and categorize results for multiple filters (AND condition).
 *
 * @param scan - "transactions" or "events"
 * @param filters - Array of {field, value} pairs (GraphQL filter field names)
 *
 * Returns breakdown:
 * - truePositives: all filters match same transaction in checkpoint
 * - partialMatches: some filters match transactions, but not all (known pair issue)
 * - pureFalsePositives: no filter matches any transaction (true bloom collision)
 */
export async function countComboTruePositives(
  scan: ScanTarget,
  filters: Array<{ field: string; value: string }>,
  bloomConditions: string,
  cpLo: number,
  cpHi: number,
  sampleSize: number = 10000,
  chunkSize: number = 1000,
): Promise<{
  bloomMatches: number;
  truePositives: number;
  partialMatches: number;
  pureFalsePositives: number;
}> {
  if (filters.length === 0) {
    return { bloomMatches: 0, truePositives: 0, partialMatches: 0, pureFalsePositives: 0 };
  }

  // Step 1: Get sampled checkpoints that match bloom filter
  const bloomSql = `
    SELECT cp_sequence_number
    FROM cp_blooms
    WHERE cp_sequence_number >= $1 AND cp_sequence_number <= $2
      AND ${bloomConditions}
    ORDER BY cp_sequence_number
    LIMIT $3
  `;
  const bloomMatchRows = await query<{ cp_sequence_number: string }>(bloomSql, [
    cpLo,
    cpHi,
    sampleSize,
  ]);
  const bloomMatchCps = bloomMatchRows.map((r) => Number(r.cp_sequence_number));

  if (bloomMatchCps.length === 0) {
    return { bloomMatches: 0, truePositives: 0, partialMatches: 0, pureFalsePositives: 0 };
  }

  const bloomMatches = bloomMatchCps.length;

  // Step 2: Process checkpoints in chunks
  const truePositiveCps = new Set<number>();
  const partialMatchCps = new Set<number>();
  const pureFalsePositiveCps = new Set<number>();

  for (let i = 0; i < bloomMatchCps.length; i += chunkSize) {
    const chunkCps = bloomMatchCps.slice(i, i + chunkSize);
    const cpRanges = await getCheckpointRangesBatch(chunkCps);

    if (cpRanges.size === 0) continue;

    // Get overall tx range for this chunk
    let minTx = Infinity;
    let maxTx = -Infinity;
    for (const { lo, hi } of cpRanges.values()) {
      minTx = Math.min(minTx, lo);
      maxTx = Math.max(maxTx, hi);
    }

    // For each filter, get matching tx_sequence_numbers in this range
    const matchingTxSets: Set<number>[] = [];

    for (const filter of filters) {
      const { sql: txSql, params: txParams } = buildMatchSql(
        scan,
        filter.field,
        filter.value,
        minTx,
        maxTx,
      );
      const txRows = await query<{ tx_sequence_number: string }>(
        txSql,
        txParams,
      );
      matchingTxSets.push(
        new Set(txRows.map((r) => Number(r.tx_sequence_number))),
      );
    }

    // Intersect all filter results
    let intersectedTxs = matchingTxSets[0];
    for (let j = 1; j < matchingTxSets.length; j++) {
      const newSet = new Set<number>();
      for (const tx of intersectedTxs) {
        if (matchingTxSets[j].has(tx)) {
          newSet.add(tx);
        }
      }
      intersectedTxs = newSet;
    }

    // Sort intersected transactions for binary search
    const sortedIntersection = Array.from(intersectedTxs).sort((a, b) => a - b);

    // Sort each individual filter's transactions for binary search
    const sortedMatchingSets = matchingTxSets.map((set) =>
      Array.from(set).sort((a, b) => a - b)
    );

    // Categorize each checkpoint
    for (const [cp, { lo, hi }] of cpRanges) {
      // Check if intersection has tx in this cp's range (true positive)
      if (hasTransactionInRange(sortedIntersection, lo, hi)) {
        truePositiveCps.add(cp);
      } else {
        // Check if ANY individual filter has tx in this cp's range
        let anyFilterMatches = false;
        for (const sortedTxs of sortedMatchingSets) {
          if (hasTransactionInRange(sortedTxs, lo, hi)) {
            anyFilterMatches = true;
            break;
          }
        }
        if (anyFilterMatches) {
          partialMatchCps.add(cp); // Known pair - some filters match but not all
        } else {
          pureFalsePositiveCps.add(cp); // True bloom collision - no filter matches
        }
      }
    }
  }

  return {
    bloomMatches,
    truePositives: truePositiveCps.size,
    partialMatches: partialMatchCps.size,
    pureFalsePositives: pureFalsePositiveCps.size,
  };
}

