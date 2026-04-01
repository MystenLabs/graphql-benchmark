/**
 * Database queries for discovering test parameters.
 */

import { query, queryScalar } from "./connection";

interface AddressCount {
  address: string;
  count: number;
}

/**
 * Get the available checkpoint range from the database.
 */
export async function getAvailableCheckpointRange(): Promise<{
  lo: number;
  hi: number;
}> {
  const sql = `
    SELECT MIN(cp_sequence_number) as lo, MAX(cp_sequence_number) as hi
    FROM cp_sequence_numbers
  `;
  const rows = await query<{ lo: string; hi: string }>(sql);
  if (rows.length === 0 || rows[0].lo === null) {
    return { lo: 0, hi: 0 };
  }
  return { lo: Number(rows[0].lo), hi: Number(rows[0].hi) };
}

/**
 * Get transaction sequence number range for a checkpoint range.
 */
export async function getCheckpointTxRange(
  cpLo: number,
  cpHi: number,
): Promise<{ lo: number; hi: number }> {
  const sql = `
    SELECT
      MIN(tx_lo) as lo,
      COALESCE(
        (SELECT tx_lo FROM cp_sequence_numbers WHERE cp_sequence_number = $1 + 1),
        MAX(tx_lo) + 10000
      ) as hi
    FROM cp_sequence_numbers
    WHERE cp_sequence_number BETWEEN $2 AND $1
  `;
  const rows = await query<{ lo: string; hi: string }>(sql, [cpHi, cpLo]);
  if (rows.length === 0 || rows[0].lo === null) {
    return { lo: 0, hi: 0 };
  }
  return { lo: Number(rows[0].lo), hi: Number(rows[0].hi) };
}

/**
 * Get tx_sequence_number ranges for multiple checkpoints in a single query.
 */
export async function getCheckpointRangesBatch(
  checkpoints: number[],
): Promise<Map<number, { lo: number; hi: number }>> {
  if (checkpoints.length === 0) {
    return new Map();
  }

  const minCp = Math.min(...checkpoints);
  const maxCp = Math.max(...checkpoints);

  const sql = `
    WITH cp_ranges AS (
      SELECT
        cp_sequence_number,
        tx_lo,
        LEAD(tx_lo) OVER (ORDER BY cp_sequence_number) as tx_hi
      FROM cp_sequence_numbers
      WHERE cp_sequence_number >= $1 AND cp_sequence_number <= $2 + 1
    )
    SELECT cp_sequence_number, tx_lo, COALESCE(tx_hi, tx_lo + 10000) as tx_hi
    FROM cp_ranges
    WHERE cp_sequence_number = ANY($3)
  `;

  const rows = await query<{
    cp_sequence_number: string;
    tx_lo: string;
    tx_hi: string;
  }>(sql, [minCp, maxCp, checkpoints]);

  const result = new Map<number, { lo: number; hi: number }>();
  for (const row of rows) {
    result.set(Number(row.cp_sequence_number), {
      lo: Number(row.tx_lo),
      hi: Number(row.tx_hi),
    });
  }
  return result;
}

/**
 * Find highest-frequency packages by call count.
 */
export async function queryTopPackages(
  txLo: number,
  txHi: number,
  limit: number = 10,
  sampleRate: number = 1,
): Promise<AddressCount[]> {
  let sql: string;
  let params: unknown[];

  if (sampleRate > 1) {
    sql = `
      SELECT '0x' || encode(package, 'hex') as address, COUNT(*) * $1 as count
      FROM tx_calls
      WHERE tx_sequence_number BETWEEN $2 AND $3
        AND tx_sequence_number % $1 = 0
      GROUP BY package
      ORDER BY COUNT(*) DESC
      LIMIT $4
    `;
    params = [sampleRate, txLo, txHi, limit];
  } else {
    sql = `
      SELECT '0x' || encode(package, 'hex') as address, COUNT(*) as count
      FROM tx_calls
      WHERE tx_sequence_number BETWEEN $1 AND $2
      GROUP BY package
      ORDER BY COUNT(*) DESC
      LIMIT $3
    `;
    params = [txLo, txHi, limit];
  }

  const rows = await query<{ address: string; count: string }>(sql, params);
  return rows.map((r) => ({ address: r.address, count: Number(r.count) }));
}

/**
 * Find highest-frequency senders.
 */
export async function queryTopSenders(
  txLo: number,
  txHi: number,
  limit: number = 10,
  sampleRate: number = 1,
): Promise<AddressCount[]> {
  let sql: string;
  let params: unknown[];

  if (sampleRate > 1) {
    sql = `
      SELECT '0x' || encode(sender, 'hex') as address, COUNT(*) * $1 as count
      FROM tx_affected_addresses
      WHERE tx_sequence_number BETWEEN $2 AND $3
        AND tx_sequence_number % $1 = 0
      GROUP BY sender
      ORDER BY COUNT(*) DESC
      LIMIT $4
    `;
    params = [sampleRate, txLo, txHi, limit];
  } else {
    sql = `
      SELECT '0x' || encode(sender, 'hex') as address, COUNT(*) as count
      FROM tx_affected_addresses
      WHERE tx_sequence_number BETWEEN $1 AND $2
      GROUP BY sender
      ORDER BY COUNT(*) DESC
      LIMIT $3
    `;
    params = [txLo, txHi, limit];
  }

  const rows = await query<{ address: string; count: string }>(sql, params);
  return rows.map((r) => ({ address: r.address, count: Number(r.count) }));
}

/**
 * Find highest-frequency affected addresses.
 */
export async function queryTopAffectedAddresses(
  txLo: number,
  txHi: number,
  limit: number = 10,
  sampleRate: number = 1,
): Promise<AddressCount[]> {
  let sql: string;
  let params: unknown[];

  if (sampleRate > 1) {
    sql = `
      SELECT '0x' || encode(affected, 'hex') as address, COUNT(*) * $1 as count
      FROM tx_affected_addresses
      WHERE tx_sequence_number BETWEEN $2 AND $3
        AND tx_sequence_number % $1 = 0
      GROUP BY affected
      ORDER BY COUNT(*) DESC
      LIMIT $4
    `;
    params = [sampleRate, txLo, txHi, limit];
  } else {
    sql = `
      SELECT '0x' || encode(affected, 'hex') as address, COUNT(*) as count
      FROM tx_affected_addresses
      WHERE tx_sequence_number BETWEEN $1 AND $2
      GROUP BY affected
      ORDER BY COUNT(*) DESC
      LIMIT $3
    `;
    params = [txLo, txHi, limit];
  }

  const rows = await query<{ address: string; count: string }>(sql, params);
  return rows.map((r) => ({ address: r.address, count: Number(r.count) }));
}

/**
 * Find highest-frequency affected objects.
 */
export async function queryTopObjects(
  txLo: number,
  txHi: number,
  limit: number = 10,
  sampleRate: number = 1,
): Promise<AddressCount[]> {
  let sql: string;
  let params: unknown[];

  if (sampleRate > 1) {
    sql = `
      SELECT '0x' || encode(affected, 'hex') as address, COUNT(*) * $1 as count
      FROM tx_affected_objects
      WHERE tx_sequence_number BETWEEN $2 AND $3
        AND tx_sequence_number % $1 = 0
      GROUP BY affected
      ORDER BY COUNT(*) DESC
      LIMIT $4
    `;
    params = [sampleRate, txLo, txHi, limit];
  } else {
    sql = `
      SELECT '0x' || encode(affected, 'hex') as address, COUNT(*) as count
      FROM tx_affected_objects
      WHERE tx_sequence_number BETWEEN $1 AND $2
      GROUP BY affected
      ORDER BY COUNT(*) DESC
      LIMIT $3
    `;
    params = [txLo, txHi, limit];
  }

  const rows = await query<{ address: string; count: string }>(sql, params);
  return rows.map((r) => ({ address: r.address, count: Number(r.count) }));
}

/**
 * Find items with moderate frequency (useful for FPR testing).
 * Uses sampling to avoid full table scans.
 */
export async function queryLowFreqItems(
  table: string,
  column: string,
  txLo: number,
  txHi: number,
  minCount: number = 100,
  maxCount: number = 1000,
  limit: number = 5,
  sampleSize: number = 100000,
): Promise<AddressCount[]> {
  // Validate table and column names to prevent SQL injection
  const validTables = ["tx_calls", "tx_affected_addresses", "tx_affected_objects"];
  const validColumns = ["package", "sender", "affected"];

  if (!validTables.includes(table)) {
    throw new Error(`Invalid table name: ${table}`);
  }
  if (!validColumns.includes(column)) {
    throw new Error(`Invalid column name: ${column}`);
  }

  // Sample a subset of the range to estimate frequencies quickly
  const sql = `
    WITH sampled AS (
      SELECT ${column}
      FROM ${table}
      WHERE tx_sequence_number BETWEEN $1 AND $2
      ORDER BY tx_sequence_number
      LIMIT $6
    )
    SELECT '0x' || encode(${column}, 'hex') as address, COUNT(*) as count
    FROM sampled
    GROUP BY ${column}
    HAVING COUNT(*) BETWEEN $3 AND $4
    ORDER BY COUNT(*) DESC
    LIMIT $5
  `;

  const rows = await query<{ address: string; count: string }>(sql, [
    txLo,
    txHi,
    minCount,
    maxCount,
    limit,
    sampleSize,
  ]);
  return rows.map((r) => ({ address: r.address, count: Number(r.count) }));
}

/**
 * Find package + sender pairs that co-occur in transactions.
 * Uses a database join with sampling for efficiency.
 */
export async function queryCooccurringPkgSender(
  txLo: number,
  txHi: number,
  minCount: number = 10000,
  maxCount: number = 100000,
  limit: number = 5,
  sampleSize: number = 100000,
): Promise<Array<{ package: string; sender: string; count: number }>> {
  // Use database join on a sampled subset for efficiency
  const sql = `
    WITH sampled_txs AS (
      SELECT DISTINCT tx_sequence_number
      FROM tx_calls
      WHERE tx_sequence_number BETWEEN $1 AND $2
      ORDER BY tx_sequence_number
      LIMIT $5
    ),
    pkg_sender AS (
      SELECT
        c.package,
        a.sender,
        c.tx_sequence_number
      FROM tx_calls c
      JOIN tx_affected_addresses a ON c.tx_sequence_number = a.tx_sequence_number
      WHERE c.tx_sequence_number IN (SELECT tx_sequence_number FROM sampled_txs)
    )
    SELECT
      '0x' || encode(package, 'hex') as package,
      '0x' || encode(sender, 'hex') as sender,
      COUNT(DISTINCT tx_sequence_number) as count
    FROM pkg_sender
    GROUP BY package, sender
    HAVING COUNT(DISTINCT tx_sequence_number) BETWEEN $3 AND $4
    ORDER BY count DESC
    LIMIT $6
  `;

  const rows = await query<{ package: string; sender: string; count: string }>(
    sql,
    [txLo, txHi, minCount, maxCount, sampleSize, limit],
  );
  return rows.map((r) => ({
    package: r.package,
    sender: r.sender,
    count: Number(r.count),
  }));
}

/**
 * Find package + object pairs that co-occur in transactions.
 * Uses a database join with sampling for efficiency.
 */
export async function queryCooccurringPkgObject(
  txLo: number,
  txHi: number,
  minCount: number = 1000,
  maxCount: number = 100000,
  limit: number = 5,
  sampleSize: number = 100000,
): Promise<Array<{ package: string; object: string; count: number }>> {
  const sql = `
    WITH sampled_txs AS (
      SELECT DISTINCT tx_sequence_number
      FROM tx_calls
      WHERE tx_sequence_number BETWEEN $1 AND $2
      ORDER BY tx_sequence_number
      LIMIT $5
    ),
    pkg_obj AS (
      SELECT
        c.package,
        o.affected as object,
        c.tx_sequence_number
      FROM tx_calls c
      JOIN tx_affected_objects o ON c.tx_sequence_number = o.tx_sequence_number
      WHERE c.tx_sequence_number IN (SELECT tx_sequence_number FROM sampled_txs)
    )
    SELECT
      '0x' || encode(package, 'hex') as package,
      '0x' || encode(object, 'hex') as object,
      COUNT(DISTINCT tx_sequence_number) as count
    FROM pkg_obj
    GROUP BY package, object
    HAVING COUNT(DISTINCT tx_sequence_number) BETWEEN $3 AND $4
    ORDER BY count DESC
    LIMIT $6
  `;

  const rows = await query<{ package: string; object: string; count: string }>(
    sql,
    [txLo, txHi, minCount, maxCount, sampleSize, limit],
  );
  return rows.map((r) => ({
    package: r.package,
    object: r.object,
    count: Number(r.count),
  }));
}

/**
 * Find sender + object pairs that co-occur in transactions.
 * Uses a database join with sampling for efficiency.
 */
export async function queryCooccurringSenderObject(
  txLo: number,
  txHi: number,
  minCount: number = 1000,
  maxCount: number = 100000,
  limit: number = 5,
  sampleSize: number = 100000,
): Promise<Array<{ sender: string; object: string; count: number }>> {
  const sql = `
    WITH sampled_txs AS (
      SELECT DISTINCT tx_sequence_number
      FROM tx_affected_addresses
      WHERE tx_sequence_number BETWEEN $1 AND $2
      ORDER BY tx_sequence_number
      LIMIT $5
    ),
    sender_obj AS (
      SELECT
        a.sender,
        o.affected as object,
        a.tx_sequence_number
      FROM tx_affected_addresses a
      JOIN tx_affected_objects o ON a.tx_sequence_number = o.tx_sequence_number
      WHERE a.tx_sequence_number IN (SELECT tx_sequence_number FROM sampled_txs)
    )
    SELECT
      '0x' || encode(sender, 'hex') as sender,
      '0x' || encode(object, 'hex') as object,
      COUNT(DISTINCT tx_sequence_number) as count
    FROM sender_obj
    GROUP BY sender, object
    HAVING COUNT(DISTINCT tx_sequence_number) BETWEEN $3 AND $4
    ORDER BY count DESC
    LIMIT $6
  `;

  const rows = await query<{ sender: string; object: string; count: string }>(
    sql,
    [txLo, txHi, minCount, maxCount, sampleSize, limit],
  );
  return rows.map((r) => ({
    sender: r.sender,
    object: r.object,
    count: Number(r.count),
  }));
}
