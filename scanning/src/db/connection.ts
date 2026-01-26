/**
 * PostgreSQL connection management.
 */

import { Pool, PoolClient, QueryResult } from "pg";

let pool: Pool | null = null;

/**
 * Initialize the connection pool.
 * @param databaseUrl - PostgreSQL connection URL
 */
export function initPool(databaseUrl: string): Pool {
  if (pool) {
    return pool;
  }

  pool = new Pool({
    connectionString: databaseUrl,
    max: 10, // max connections
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
  });

  return pool;
}

/**
 * Get the connection pool, initializing if needed.
 * @param databaseUrl - PostgreSQL connection URL (required on first call)
 */
export function getPool(databaseUrl?: string): Pool {
  if (!pool && databaseUrl) {
    return initPool(databaseUrl);
  }
  if (!pool) {
    throw new Error("Pool not initialized. Call initPool first.");
  }
  return pool;
}

/**
 * Close the connection pool.
 */
export async function closePool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
  }
}

/**
 * Execute a query and return results.
 */
export async function query<T extends Record<string, unknown>>(
  sql: string,
  params?: unknown[],
): Promise<T[]> {
  const p = getPool();
  const result: QueryResult<T> = await p.query(sql, params);
  return result.rows;
}

/**
 * Execute a query and return a single scalar value.
 */
export async function queryScalar<T>(
  sql: string,
  params?: unknown[],
): Promise<T | null> {
  const p = getPool();
  const result = await p.query(sql, params);
  if (result.rows.length === 0) {
    return null;
  }
  const row = result.rows[0] as Record<string, T>;
  const firstKey = Object.keys(row)[0];
  return row[firstKey] ?? null;
}

/**
 * Get a client for transaction use.
 */
export async function getClient(): Promise<PoolClient> {
  const p = getPool();
  return p.connect();
}

/**
 * Convert hex string to PostgreSQL bytea parameter format.
 * @param hexStr - Hex string (with or without 0x prefix)
 * @returns Hex string without 0x prefix (for use with decode(..., 'hex'))
 */
export function hexToByteaParam(hexStr: string): string {
  return hexStr.startsWith("0x") ? hexStr.slice(2) : hexStr;
}
