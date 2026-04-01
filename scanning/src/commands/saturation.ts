/**
 * Bloom filter saturation analysis.
 *
 * Reports bloom filter size distribution and bit saturation for a database,
 * using PostgreSQL's bit_count() for accurate popcount measurement.
 */

import { printHeader } from "../utils";
import { initPool, closePool, query } from "../db/connection";

export interface SaturationOptions {
  databaseUrl: string;
  cpLo: number;
  cpHi: number;
}

interface SizeRow {
  filter_bytes: number;
  n: number;
  avg_saturation: number;
  min_saturation: number;
  max_saturation: number;
  p50_saturation: number;
  p90_saturation: number;
  avg_bits_set: number;
  total_bits: number;
}

export async function runSaturation(options: SaturationOptions): Promise<void> {
  initPool(options.databaseUrl);

  try {
    printHeader("BLOOM FILTER SATURATION ANALYSIS");

    // Get effective checkpoint range
    const rangeRows = await query<{
      min: string;
      max: string;
      count: string;
    }>(
      "SELECT min(cp_sequence_number)::bigint as min, max(cp_sequence_number)::bigint as max, count(*)::bigint as count FROM cp_blooms",
    );
    const dbMin = Number(rangeRows[0].min);
    const dbMax = Number(rangeRows[0].max);
    const dbCount = Number(rangeRows[0].count);

    const cpLo = Math.max(options.cpLo, dbMin);
    const cpHi = Math.min(options.cpHi, dbMax);

    console.log(
      `Database: ${dbCount.toLocaleString()} filters, CP ${dbMin.toLocaleString()} - ${dbMax.toLocaleString()}`,
    );
    console.log(
      `Analysis range: CP ${cpLo.toLocaleString()} - ${cpHi.toLocaleString()}`,
    );
    console.log();

    const rows = await query<Record<string, string>>(
      `
      SELECT
        length(bloom_filter) as filter_bytes,
        count(*)::bigint as n,
        round(avg(bit_count(bloom_filter)::numeric / (length(bloom_filter) * 8)), 4) as avg_saturation,
        round(min(bit_count(bloom_filter)::numeric / (length(bloom_filter) * 8)), 4) as min_saturation,
        round(max(bit_count(bloom_filter)::numeric / (length(bloom_filter) * 8)), 4) as max_saturation,
        round(percentile_cont(0.5) within group (order by bit_count(bloom_filter)::numeric / (length(bloom_filter) * 8))::numeric, 4) as p50_saturation,
        round(percentile_cont(0.9) within group (order by bit_count(bloom_filter)::numeric / (length(bloom_filter) * 8))::numeric, 4) as p90_saturation,
        round(avg(bit_count(bloom_filter)), 0) as avg_bits_set,
        length(bloom_filter) * 8 as total_bits
      FROM cp_blooms
      WHERE cp_sequence_number >= $1 AND cp_sequence_number <= $2
      GROUP BY length(bloom_filter)
      ORDER BY length(bloom_filter)
      `,
      [cpLo, cpHi],
    );

    const stats: SizeRow[] = rows.map((r) => ({
      filter_bytes: Number(r.filter_bytes),
      n: Number(r.n),
      avg_saturation: Number(r.avg_saturation),
      min_saturation: Number(r.min_saturation),
      max_saturation: Number(r.max_saturation),
      p50_saturation: Number(r.p50_saturation),
      p90_saturation: Number(r.p90_saturation),
      avg_bits_set: Number(r.avg_bits_set),
      total_bits: Number(r.total_bits),
    }));

    const totalFilters = stats.reduce((sum, r) => sum + r.n, 0);

    // Size distribution
    console.log("Size distribution:");
    console.log(
      `  ${"Bytes".padStart(8)} ${"Count".padStart(10)} ${"%".padStart(8)} ${"Total Bits".padStart(12)}`,
    );
    console.log(`  ${"-".repeat(42)}`);
    for (const row of stats) {
      const pct = ((row.n / totalFilters) * 100).toFixed(2);
      console.log(
        `  ${row.filter_bytes.toString().padStart(8)} ${row.n.toString().padStart(10)} ${(pct + "%").padStart(8)} ${row.total_bits.toString().padStart(12)}`,
      );
    }
    console.log();

    // Saturation details
    console.log("Saturation (fraction of bits set):");
    console.log(
      `  ${"Bytes".padStart(8)} ${"Avg".padStart(10)} ${"Min".padStart(10)} ${"P50".padStart(10)} ${"P90".padStart(10)} ${"Max".padStart(10)} ${"Avg Bits".padStart(12)}`,
    );
    console.log(`  ${"-".repeat(64)}`);
    for (const row of stats) {
      console.log(
        `  ${row.filter_bytes.toString().padStart(8)} ${(row.avg_saturation * 100).toFixed(2).padStart(9)}% ${(row.min_saturation * 100).toFixed(2).padStart(9)}% ${(row.p50_saturation * 100).toFixed(2).padStart(9)}% ${(row.p90_saturation * 100).toFixed(2).padStart(9)}% ${(row.max_saturation * 100).toFixed(2).padStart(9)}% ${row.avg_bits_set.toString().padStart(12)}`,
      );
    }

    // Summary
    printHeader("SUMMARY");
    const main = stats.find((r) => r.filter_bytes === 1024);
    if (main) {
      console.log(`Primary filter size: 1024 bytes (${main.n.toLocaleString()} filters)`);
      console.log(`  Average saturation: ${(main.avg_saturation * 100).toFixed(2)}%`);
      console.log(`  P90 saturation:     ${(main.p90_saturation * 100).toFixed(2)}%`);
      console.log(`  Average bits set:   ${main.avg_bits_set} / ${main.total_bits}`);

      if (main.p90_saturation < 0.2) {
        console.log("\n  Filters are sparse. Could fold more aggressively.");
      } else if (main.p90_saturation < 0.4) {
        console.log("\n  Saturation is healthy. Well below fold threshold.");
      } else {
        console.log("\n  Saturation is high. Consider larger minimum filter size.");
      }
    }

    const foldedCount = stats
      .filter((r) => r.filter_bytes > 1024)
      .reduce((sum, r) => sum + r.n, 0);
    console.log(
      `\nFolded filters (>1024 bytes): ${foldedCount.toLocaleString()} (${((foldedCount / totalFilters) * 100).toFixed(2)}%)`,
    );

    // ── cp_bloom_blocks analysis ─────────────────────────────────────
    await analyzeBloomBlocks(cpLo, cpHi);
  } finally {
    await closePool();
  }
}

/**
 * Analyze cp_bloom_blocks saturation if the table exists.
 * Each checkpoint block spans 1000 checkpoints and has up to 128 bloom blocks
 * of 2048 bytes (16384 bits) each.
 */
async function analyzeBloomBlocks(cpLo: number, cpHi: number): Promise<void> {
  // Check if table exists
  const existsRows = await query<{ exists: boolean }>(
    `SELECT EXISTS (
      SELECT FROM information_schema.tables WHERE table_name = 'cp_bloom_blocks'
    ) as exists`,
  );
  if (!existsRows[0]?.exists) {
    console.log("\ncp_bloom_blocks table not found, skipping block bloom analysis.");
    return;
  }

  const cpBlockLo = Math.floor(cpLo / 1000);
  const cpBlockHi = Math.floor(cpHi / 1000);

  printHeader("BLOCK BLOOM FILTER SATURATION (cp_bloom_blocks)");

  // Overview
  const overviewRows = await query<Record<string, string>>(
    `SELECT
      count(DISTINCT cp_block_index)::bigint as num_blocks,
      count(*)::bigint as num_bloom_blocks,
      round(avg(length(bloom_filter)))::bigint as avg_filter_bytes
    FROM cp_bloom_blocks
    WHERE cp_block_index >= $1 AND cp_block_index <= $2`,
    [cpBlockLo, cpBlockHi],
  );
  const overview = overviewRows[0];
  console.log(`Checkpoint blocks: ${Number(overview.num_blocks).toLocaleString()} (CP blocks ${cpBlockLo.toLocaleString()} - ${cpBlockHi.toLocaleString()})`);
  console.log(`Total bloom blocks: ${Number(overview.num_bloom_blocks).toLocaleString()}`);
  console.log(`Avg bloom blocks per CP block: ${(Number(overview.num_bloom_blocks) / Math.max(1, Number(overview.num_blocks))).toFixed(1)} / 128`);
  console.log();

  // Saturation per bloom block
  const satRows = await query<Record<string, string>>(
    `SELECT
      round(avg(bit_count(bloom_filter)::numeric / (length(bloom_filter) * 8)), 4) as avg_saturation,
      round(min(bit_count(bloom_filter)::numeric / (length(bloom_filter) * 8)), 4) as min_saturation,
      round(percentile_cont(0.5) within group (order by bit_count(bloom_filter)::numeric / (length(bloom_filter) * 8))::numeric, 4) as p50_saturation,
      round(percentile_cont(0.9) within group (order by bit_count(bloom_filter)::numeric / (length(bloom_filter) * 8))::numeric, 4) as p90_saturation,
      round(max(bit_count(bloom_filter)::numeric / (length(bloom_filter) * 8)), 4) as max_saturation,
      round(avg(bit_count(bloom_filter)), 0) as avg_bits_set,
      length(bloom_filter) * 8 as total_bits
    FROM cp_bloom_blocks
    WHERE cp_block_index >= $1 AND cp_block_index <= $2
    GROUP BY length(bloom_filter)`,
    [cpBlockLo, cpBlockHi],
  );

  if (satRows.length > 0) {
    console.log("Bloom block saturation (fraction of bits set):");
    console.log(
      `  ${"Bytes".padStart(8)} ${"Avg".padStart(10)} ${"Min".padStart(10)} ${"P50".padStart(10)} ${"P90".padStart(10)} ${"Max".padStart(10)} ${"Avg Bits".padStart(12)}`,
    );
    console.log(`  ${"-".repeat(64)}`);
    for (const row of satRows) {
      const bytes = Number(row.total_bits) / 8;
      console.log(
        `  ${bytes.toString().padStart(8)} ${(Number(row.avg_saturation) * 100).toFixed(2).padStart(9)}% ${(Number(row.min_saturation) * 100).toFixed(2).padStart(9)}% ${(Number(row.p50_saturation) * 100).toFixed(2).padStart(9)}% ${(Number(row.p90_saturation) * 100).toFixed(2).padStart(9)}% ${(Number(row.max_saturation) * 100).toFixed(2).padStart(9)}% ${row.avg_bits_set.padStart(12)}`,
      );
    }
  }

  // Distribution of non-zero blocks per checkpoint block
  const distRows = await query<Record<string, string>>(
    `WITH block_counts AS (
      SELECT cp_block_index, count(*) as n_blocks
      FROM cp_bloom_blocks
      WHERE cp_block_index >= $1 AND cp_block_index <= $2
      GROUP BY cp_block_index
    )
    SELECT
      round(avg(n_blocks), 1) as avg_blocks,
      min(n_blocks)::bigint as min_blocks,
      round(percentile_cont(0.5) within group (order by n_blocks))::bigint as p50_blocks,
      round(percentile_cont(0.9) within group (order by n_blocks))::bigint as p90_blocks,
      max(n_blocks)::bigint as max_blocks
    FROM block_counts`,
    [cpBlockLo, cpBlockHi],
  );

  if (distRows.length > 0) {
    const dist = distRows[0];
    console.log(`\nNon-zero blocks per checkpoint block (out of 128):`);
    console.log(`  Avg: ${dist.avg_blocks}  Min: ${dist.min_blocks}  P50: ${dist.p50_blocks}  P90: ${dist.p90_blocks}  Max: ${dist.max_blocks}`);

    const avgBlocks = Number(dist.avg_blocks);
    if (avgBlocks < 30) {
      console.log("  Sparse block usage - most bloom blocks are empty (good for storage).");
    } else if (avgBlocks < 80) {
      console.log("  Moderate block usage.");
    } else {
      console.log("  Dense block usage - most bloom blocks are populated.");
    }
  }
}
