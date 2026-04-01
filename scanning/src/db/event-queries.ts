/**
 * Database queries for event scanning analysis.
 *
 * Uses ev_emit_mod and ev_struct_inst tables for:
 * - Discovery: finding top event emitting modules, types, and senders
 * - Verification: confirming bloom filter true positives against actual event data
 */

import { query, hexToByteaParam } from "./connection";

// ── Types ────────────────────────────────────────────────────────────

interface ModuleCount {
  package: string;
  module: string;
  count: number;
}

interface TypeCount {
  package: string;
  module: string;
  name: string;
  count: number;
}

interface AddressCount {
  address: string;
  count: number;
}

// ── Discovery queries ────────────────────────────────────────────────

/**
 * Find highest-frequency event emitting modules.
 */
export async function queryTopEmitModules(
  txLo: number,
  txHi: number,
  limit: number = 10,
): Promise<ModuleCount[]> {
  const sql = `
    SELECT
      '0x' || encode(package, 'hex') as package,
      module,
      COUNT(*) as count
    FROM ev_emit_mod
    WHERE tx_sequence_number BETWEEN $1 AND $2
    GROUP BY package, module
    ORDER BY COUNT(*) DESC
    LIMIT $3
  `;
  const rows = await query<{ package: string; module: string; count: string }>(
    sql,
    [txLo, txHi, limit],
  );
  return rows.map((r) => ({
    package: r.package,
    module: r.module,
    count: Number(r.count),
  }));
}

/**
 * Find highest-frequency event types.
 */
export async function queryTopEventTypes(
  txLo: number,
  txHi: number,
  limit: number = 10,
): Promise<TypeCount[]> {
  const sql = `
    SELECT
      '0x' || encode(package, 'hex') as package,
      module,
      name,
      COUNT(*) as count
    FROM ev_struct_inst
    WHERE tx_sequence_number BETWEEN $1 AND $2
    GROUP BY package, module, name
    ORDER BY COUNT(*) DESC
    LIMIT $3
  `;
  const rows = await query<{
    package: string;
    module: string;
    name: string;
    count: string;
  }>(sql, [txLo, txHi, limit]);
  return rows.map((r) => ({
    package: r.package,
    module: r.module,
    name: r.name,
    count: Number(r.count),
  }));
}

/**
 * Find highest-frequency event senders.
 */
export async function queryTopEventSenders(
  txLo: number,
  txHi: number,
  limit: number = 10,
): Promise<AddressCount[]> {
  const sql = `
    SELECT
      '0x' || encode(sender, 'hex') as address,
      COUNT(*) as count
    FROM ev_emit_mod
    WHERE tx_sequence_number BETWEEN $1 AND $2
    GROUP BY sender
    ORDER BY COUNT(*) DESC
    LIMIT $3
  `;
  const rows = await query<{ address: string; count: string }>(sql, [
    txLo,
    txHi,
    limit,
  ]);
  return rows.map((r) => ({ address: r.address, count: Number(r.count) }));
}

/**
 * Find moderate-frequency event emitting modules (for FPR testing).
 */
export async function queryLowFreqEmitModules(
  txLo: number,
  txHi: number,
  minCount: number = 100,
  maxCount: number = 1000,
  limit: number = 5,
  sampleSize: number = 100000,
): Promise<ModuleCount[]> {
  const sql = `
    WITH sampled AS (
      SELECT package, module
      FROM ev_emit_mod
      WHERE tx_sequence_number BETWEEN $1 AND $2
      ORDER BY tx_sequence_number
      LIMIT $6
    )
    SELECT
      '0x' || encode(package, 'hex') as package,
      module,
      COUNT(*) as count
    FROM sampled
    GROUP BY package, module
    HAVING COUNT(*) BETWEEN $3 AND $4
    ORDER BY COUNT(*) DESC
    LIMIT $5
  `;
  const rows = await query<{ package: string; module: string; count: string }>(
    sql,
    [txLo, txHi, minCount, maxCount, limit, sampleSize],
  );
  return rows.map((r) => ({
    package: r.package,
    module: r.module,
    count: Number(r.count),
  }));
}

/**
 * Find moderate-frequency event types (for FPR testing).
 */
export async function queryLowFreqEventTypes(
  txLo: number,
  txHi: number,
  minCount: number = 100,
  maxCount: number = 1000,
  limit: number = 5,
  sampleSize: number = 100000,
): Promise<TypeCount[]> {
  const sql = `
    WITH sampled AS (
      SELECT package, module, name
      FROM ev_struct_inst
      WHERE tx_sequence_number BETWEEN $1 AND $2
      ORDER BY tx_sequence_number
      LIMIT $6
    )
    SELECT
      '0x' || encode(package, 'hex') as package,
      module,
      name,
      COUNT(*) as count
    FROM sampled
    GROUP BY package, module, name
    HAVING COUNT(*) BETWEEN $3 AND $4
    ORDER BY COUNT(*) DESC
    LIMIT $5
  `;
  const rows = await query<{
    package: string;
    module: string;
    name: string;
    count: string;
  }>(sql, [txLo, txHi, minCount, maxCount, limit, sampleSize]);
  return rows.map((r) => ({
    package: r.package,
    module: r.module,
    name: r.name,
    count: Number(r.count),
  }));
}

// ── Verification queries (true positive matching) ────────────────────

/**
 * Parse a module filter value "0xpkg::mod" into components.
 */
function parseModuleKey(value: string): { pkg: string; module?: string } {
  const idx = value.indexOf("::");
  if (idx < 0) {
    return { pkg: hexToByteaParam(value) };
  }
  return {
    pkg: hexToByteaParam(value.slice(0, idx)),
    module: value.slice(idx + 2),
  };
}

/**
 * Parse a type filter value "0xpkg::mod::Name" into components.
 */
function parseTypeKey(value: string): {
  pkg: string;
  module?: string;
  name?: string;
} {
  // Strip type params
  const angleIdx = value.indexOf("<");
  const base = angleIdx >= 0 ? value.slice(0, angleIdx) : value;

  const idx = base.indexOf("::");
  if (idx < 0) {
    return { pkg: hexToByteaParam(base) };
  }
  const pkg = hexToByteaParam(base.slice(0, idx));
  const rest = base.slice(idx + 2);

  const idx2 = rest.indexOf("::");
  if (idx2 < 0) {
    return { pkg, module: rest };
  }

  return { pkg, module: rest.slice(0, idx2), name: rest.slice(idx2 + 2) };
}

/**
 * Build SQL to find transactions matching an event filter field.
 *
 * EventFilter fields:
 * - sender: query ev_emit_mod by sender
 * - module: query ev_emit_mod by package (+ module)
 * - type: query ev_struct_inst by package (+ module + name)
 */
export function buildEventMatchSql(
  field: string,
  value: string,
  minTx: number,
  maxTx: number,
): { sql: string; params: unknown[] } {
  switch (field) {
    case "sender": {
      const keyClean = hexToByteaParam(value);
      return {
        sql: `SELECT DISTINCT tx_sequence_number FROM ev_emit_mod
              WHERE sender = decode($1, 'hex')
                AND tx_sequence_number >= $2 AND tx_sequence_number < $3`,
        params: [keyClean, minTx, maxTx],
      };
    }

    case "module": {
      const parts = parseModuleKey(value);
      if (parts.module) {
        return {
          sql: `SELECT DISTINCT tx_sequence_number FROM ev_emit_mod
                WHERE package = decode($1, 'hex') AND module = $2
                  AND tx_sequence_number >= $3 AND tx_sequence_number < $4`,
          params: [parts.pkg, parts.module, minTx, maxTx],
        };
      }
      return {
        sql: `SELECT DISTINCT tx_sequence_number FROM ev_emit_mod
              WHERE package = decode($1, 'hex')
                AND tx_sequence_number >= $2 AND tx_sequence_number < $3`,
        params: [parts.pkg, minTx, maxTx],
      };
    }

    case "type": {
      const parts = parseTypeKey(value);
      if (parts.name) {
        return {
          sql: `SELECT DISTINCT tx_sequence_number FROM ev_struct_inst
                WHERE package = decode($1, 'hex') AND module = $2 AND name = $3
                  AND tx_sequence_number >= $4 AND tx_sequence_number < $5`,
          params: [parts.pkg, parts.module, parts.name, minTx, maxTx],
        };
      }
      if (parts.module) {
        return {
          sql: `SELECT DISTINCT tx_sequence_number FROM ev_struct_inst
                WHERE package = decode($1, 'hex') AND module = $2
                  AND tx_sequence_number >= $3 AND tx_sequence_number < $4`,
          params: [parts.pkg, parts.module, minTx, maxTx],
        };
      }
      return {
        sql: `SELECT DISTINCT tx_sequence_number FROM ev_struct_inst
              WHERE package = decode($1, 'hex')
                AND tx_sequence_number >= $2 AND tx_sequence_number < $3`,
        params: [parts.pkg, minTx, maxTx],
      };
    }

    default:
      throw new Error(`Unknown event filter field: ${field}`);
  }
}
