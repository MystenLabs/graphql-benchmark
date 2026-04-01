/**
 * FPR Analysis command - measures false positive rates and overfetch factors.
 *
 * Supports both transaction and event scanning via the unified --filter interface
 * and correct bloom value tagging.
 */

import * as fs from "fs";
import * as path from "path";
import type {
  FPRResult,
  ScanTarget,
} from "../types";
import { printHeader } from "../utils";
import { initPool, closePool, query } from "../db/connection";
import {
  getAvailableCheckpointRange,
  getCheckpointTxRange,
  queryTopPackages,
  queryTopSenders,
  queryTopObjects,
  queryLowFreqItems,
} from "../db/queries";
import {
  countTruePositives,
  countBlockedTruePositives,
  countComboTruePositives,
  getBloomSizeDistribution,
  getBloomDensityDistribution,
  checkBlockedBloomTableExists,
} from "../db/bloom-queries";
import {
  computePositionsForValues,
  buildBloomCondition,
  theoreticalFpr,
  overfetchFactor,
} from "../bloom/positions";
import { filterFieldToBloomValues } from "../bloom/values";
import {
  DEEPBOOK_PACKAGE,
  SUI_FRAMEWORK,
  NONEXISTENT_ADDRESS,
  CP_BLOOM_NUM_HASHES,
} from "../bloom/constants";

export interface AnalyzeOptions {
  scan: ScanTarget;
  databaseUrl: string;
  cpLo: number;
  cpHi: number;
  sampleSize: number;
  pageSize: number;
  lowMin: number;
  lowMax: number;
  overfetch: boolean;
  blocked: boolean;
  sizeDistribution: boolean;
  density: boolean;
  theoretical: boolean;
  /** Unified filters as JSON objects matching GraphQL filter shape */
  filters: Array<Record<string, string>>;
  paramsFile?: string;
  outputDir?: string;
}


/**
 * Run a single FPR test for a filter field.
 * Uses correct bloom value tagging via filterFieldToBloomValues().
 */
async function runSingleFprTest(
  scan: ScanTarget,
  label: string,
  filterField: string,
  value: string,
  cpLo: number,
  cpHi: number,
  sampleSize: number,
): Promise<FPRResult> {
  const bloomValues = filterFieldToBloomValues(scan, filterField, value);
  const positions = computePositionsForValues(bloomValues);
  const conditions = buildBloomCondition(positions);

  const { bloomMatches, truePositives } = await countTruePositives(
    scan,
    filterField,
    value,
    conditions,
    cpLo,
    cpHi,
    sampleSize,
  );

  const falsePositives = bloomMatches - truePositives;
  let fpr: number | "inf" = 0;
  let of: number | "inf" = 1.0;

  if (bloomMatches > 0) {
    fpr = falsePositives / bloomMatches;
    of = overfetchFactor(fpr);
    if (of === Infinity) of = "inf";
  }

  return {
    label,
    bloom_matches: bloomMatches,
    true_positives: truePositives,
    false_positives: falsePositives,
    fpr,
    overfetch: of,
    no_matching_txs: false,
  };
}

/**
 * Run FPR test for a filter with multiple fields combined with AND.
 * Uses correct bloom value tagging via filterFieldToBloomValues().
 */
async function runComboFprTest(
  scan: ScanTarget,
  label: string,
  filter: Record<string, string>,
  cpLo: number,
  cpHi: number,
  sampleSize: number,
): Promise<FPRResult> {
  // Compute bloom positions for ALL filter fields
  const allPositions: number[] = [];
  for (const [field, value] of Object.entries(filter)) {
    const bloomValues = filterFieldToBloomValues(scan, field, value);
    allPositions.push(...computePositionsForValues(bloomValues));
  }

  const conditions = buildBloomCondition(allPositions);

  const comboFilters = Object.entries(filter).map(([field, value]) => ({
    field,
    value,
  }));

  const { bloomMatches, truePositives, partialMatches, pureFalsePositives } =
    await countComboTruePositives(scan, comboFilters, conditions, cpLo, cpHi, sampleSize);

  const falsePositives = bloomMatches - truePositives;
  const noMatchingTxs = truePositives === 0 && bloomMatches > 0;

  let fpr: number | "inf" = 0;
  let of: number | "inf" = 1.0;

  if (bloomMatches > 0 && !noMatchingTxs) {
    fpr = falsePositives / bloomMatches;
    of = overfetchFactor(fpr);
    if (of === Infinity) of = "inf";
  }

  return {
    label,
    bloom_matches: bloomMatches,
    true_positives: truePositives,
    false_positives: falsePositives,
    fpr,
    overfetch: of,
    no_matching_txs: noMatchingTxs,
    partial_matches: partialMatches,
    pure_false_positives: pureFalsePositives,
  };
}

/**
 * Run FPR test using blocked bloom filters.
 * Uses correct bloom value tagging.
 */
async function runBlockedFprTest(
  scan: ScanTarget,
  label: string,
  filterField: string,
  value: string,
  cpLo: number,
  cpHi: number,
  sampleSize: number,
): Promise<FPRResult> {
  const { bloomMatches, truePositives } = await countBlockedTruePositives(
    scan,
    filterField,
    value,
    cpLo,
    cpHi,
    sampleSize,
  );

  const falsePositives = bloomMatches - truePositives;
  let fpr: number | "inf" = 0;
  let of: number | "inf" = 1.0;

  if (bloomMatches > 0) {
    fpr = falsePositives / bloomMatches;
    of = overfetchFactor(fpr);
    if (of === Infinity) of = "inf";
  }

  return {
    label,
    bloom_matches: bloomMatches,
    true_positives: truePositives,
    false_positives: falsePositives,
    fpr,
    overfetch: of,
    no_matching_txs: false,
  };
}

function printFprTable(results: FPRResult[], pageSize: number): void {
  const header =
    `${"Filter Type".padEnd(30)} ${"Bloom Match".padStart(12)} ${"True Pos".padStart(10)} ` +
    `${"False Pos".padStart(10)} ${"FPR %".padStart(8)} ${"Overfetch".padStart(10)} ${`Fetch for ${pageSize}`.padStart(14)}`;
  console.log(header);
  console.log("=".repeat(110));

  for (const r of results) {
    if (r.bloom_matches === 0) {
      console.log(
        `${r.label.padEnd(30)} ${"0".padStart(12)} ${"0".padStart(10)} ${"0".padStart(10)} ` +
          `${"N/A".padStart(8)} ${"N/A".padStart(10)} ${"N/A".padStart(14)}`,
      );
    } else if (r.no_matching_txs) {
      console.log(
        `${r.label.padEnd(30)} ${r.bloom_matches.toString().padStart(12)} ${r.true_positives.toString().padStart(10)} ` +
          `${"-".padStart(10)} ${"N/A*".padStart(8)} ${"N/A".padStart(10)} ${"N/A".padStart(14)}`,
      );
    } else {
      const fprNum = typeof r.fpr === "number" ? r.fpr : 1;
      const fprPct = `${(fprNum * 100).toFixed(2)}%`;
      const ofNum = typeof r.overfetch === "number" ? r.overfetch : Infinity;
      const overfetchStr = ofNum === Infinity ? "inf" : ofNum.toFixed(2);
      const fetchNeeded =
        ofNum === Infinity ? "N/A" : Math.round(pageSize * ofNum).toString();
      console.log(
        `${r.label.padEnd(30)} ${r.bloom_matches.toString().padStart(12)} ${r.true_positives.toString().padStart(10)} ` +
          `${r.false_positives.toString().padStart(10)} ${fprPct.padStart(8)} ${overfetchStr.padStart(10)} ${fetchNeeded.padStart(14)}`,
      );
    }
  }
}

/**
 * Print FPR table for combo filters with partial match breakdown.
 * Shows: Partial (some filters match) vs PureFP (no filter matches = true bloom collision)
 */
function printComboFprTable(results: FPRResult[], pageSize: number): void {
  const header =
    `${"Filter Type".padEnd(35)} ${"Bloom".padStart(7)} ${"TruePos".padStart(8)} ` +
    `${"Partial".padStart(8)} ${"PureFP".padStart(8)} ${"FPR %".padStart(8)} ` +
    `${"EffFPR %".padStart(9)} ${"Overfetch".padStart(10)}`;
  console.log(header);
  console.log("=".repeat(110));

  for (const r of results) {
    const partial = r.partial_matches ?? 0;
    const pureFp = r.pure_false_positives ?? r.false_positives;

    if (r.bloom_matches === 0) {
      console.log(
        `${r.label.padEnd(35)} ${"0".padStart(7)} ${"0".padStart(8)} ` +
          `${"0".padStart(8)} ${"0".padStart(8)} ${"N/A".padStart(8)} ` +
          `${"N/A".padStart(9)} ${"N/A".padStart(10)}`,
      );
    } else if (r.no_matching_txs) {
      const effFprPct =
        r.bloom_matches > 0 ? `${((pureFp / r.bloom_matches) * 100).toFixed(2)}%` : "N/A";
      console.log(
        `${r.label.padEnd(35)} ${r.bloom_matches.toString().padStart(7)} ${r.true_positives.toString().padStart(8)} ` +
          `${partial.toString().padStart(8)} ${pureFp.toString().padStart(8)} ${"N/A*".padStart(8)} ` +
          `${effFprPct.padStart(9)} ${"N/A".padStart(10)}`,
      );
    } else {
      const fprNum = typeof r.fpr === "number" ? r.fpr : 1;
      const fprPct = `${(fprNum * 100).toFixed(2)}%`;
      const effFprPct = `${((pureFp / r.bloom_matches) * 100).toFixed(2)}%`;
      const ofNum = typeof r.overfetch === "number" ? r.overfetch : Infinity;
      const overfetchStr = ofNum === Infinity ? "inf" : ofNum.toFixed(2);
      console.log(
        `${r.label.padEnd(35)} ${r.bloom_matches.toString().padStart(7)} ${r.true_positives.toString().padStart(8)} ` +
          `${partial.toString().padStart(8)} ${pureFp.toString().padStart(8)} ${fprPct.padStart(8)} ` +
          `${effFprPct.padStart(9)} ${overfetchStr.padStart(10)}`,
      );
    }
  }
}

/**
 * Analyze bloom filter size distribution.
 */
async function analyzeSizeDistribution(cpLo: number, cpHi: number): Promise<void> {
  printHeader("BLOOM FILTER SIZE DISTRIBUTION");

  const results = await getBloomSizeDistribution(cpLo, cpHi);

  console.log(
    `${"Bits".padStart(10)} ${"Count".padStart(12)} ${"Avg Items".padStart(12)} ${"Percentage".padStart(12)}`,
  );
  console.log("-".repeat(50));
  for (const { bits, count, avgItems, pct } of results) {
    console.log(
      `${bits.toString().padStart(10)} ${count.toString().padStart(12)} ${avgItems.toFixed(1).padStart(12)} ${(pct + "%").padStart(11)}`,
    );
  }
}

/**
 * Analyze bloom filter density distribution.
 */
async function analyzeDensityDistribution(
  cpLo: number,
  cpHi: number,
  sampleSize: number,
): Promise<void> {
  printHeader("BLOOM FILTER DENSITY DISTRIBUTION");
  console.log(`Sample size: ${sampleSize.toLocaleString()} filters`);
  console.log();
  console.log("Density = fraction of bits set (0.0 = empty, 1.0 = full)");
  console.log("For optimal FPR, target density ~0.5 (ln(2) for k hash functions)");
  console.log();

  const results = await getBloomDensityDistribution(cpLo, cpHi, sampleSize);

  console.log(
    `${"Bits".padStart(8)} ${"Count".padStart(8)} ${"Avg".padStart(8)} ${"Min".padStart(8)} ${"Max".padStart(8)} ${"P50".padStart(8)} ${"P90".padStart(8)} ${"Est FPR".padStart(10)}`,
  );
  console.log("-".repeat(80));

  for (const { bits, count, avgDensity, minDensity, maxDensity, p50Density, p90Density } of results) {
    const estFpr = theoreticalFpr(p90Density, CP_BLOOM_NUM_HASHES);
    console.log(
      `${bits.toString().padStart(8)} ${count.toString().padStart(8)} ${(avgDensity * 100).toFixed(0).padStart(6)}% ${(minDensity * 100).toFixed(0).padStart(6)}% ` +
        `${(maxDensity * 100).toFixed(0).padStart(6)}% ${(p50Density * 100).toFixed(0).padStart(6)}% ${(p90Density * 100).toFixed(0).padStart(6)}% ${(estFpr * 100).toFixed(2).padStart(8)}%`,
    );
  }

  console.log();
  console.log("Folding recommendations:");
  console.log("- If P90 density > 50%, consider larger min filter size");
  console.log("- If P90 density < 20%, filters could be folded more aggressively");
}

/**
 * Print theoretical FPR table.
 */
function analyzeTheoreticalFpr(): void {
  printHeader("THEORETICAL FPR BY FILTER SIZE");
  console.log("(Assuming uniform bit distribution)");
  console.log();

  const sizes = [8192, 16384, 32768, 65536, 131072];
  const densities = [0.1, 0.2, 0.4, 0.6];

  let header = `${"Bits".padStart(8)}`;
  for (const d of densities) {
    header += ` ${`Density ${(d * 100).toFixed(0)}%`.padStart(12)}`;
  }
  console.log(header);
  console.log("-".repeat(60));

  for (const bits of sizes) {
    let row = `${bits.toString().padStart(8)}`;
    for (const density of densities) {
      const fpr = theoreticalFpr(density, CP_BLOOM_NUM_HASHES);
      row += ` ${(fpr * 100).toFixed(2).padStart(10)}%`;
    }
    console.log(row);
  }
}

/**
 * Run filter analysis using the unified --filter interface.
 * Each filter is a JSON object matching the GraphQL filter shape.
 */
async function checkVerificationTables(scan: "transactions" | "events"): Promise<string[]> {
  const tables =
    scan === "transactions"
      ? ["tx_calls", "tx_affected_objects", "tx_affected_addresses"]
      : ["ev_emit_mod", "ev_struct_inst"];
  const empty: string[] = [];
  for (const table of tables) {
    const rows = await query<{ n: string }>(
      `SELECT count(*)::bigint AS n FROM ${table} LIMIT 1`,
    );
    if (rows.length === 0 || Number(rows[0].n) === 0) {
      empty.push(table);
    }
  }
  return empty;
}

async function runFilterAnalysis(
  options: AnalyzeOptions,
  cpLo: number,
  cpHi: number,
): Promise<void> {
  const scan = options.scan;

  printHeader(`FILTER ANALYSIS (scan${scan === "transactions" ? "Transactions" : "Events"})`);
  console.log(`Checkpoint range: ${cpLo} - ${cpHi}`);
  console.log(`Sample size: ${options.sampleSize}`);
  console.log();

  // Check that verification tables have data
  const emptyTables = await checkVerificationTables(scan);
  if (emptyTables.length > 0) {
    console.log(`WARNING: Verification tables are empty: ${emptyTables.join(", ")}`);
    console.log(
      "FPR results will show 100% false positives because true-positive verification cannot find matching rows.",
    );
    console.log(
      "This does NOT mean bloom filters are broken — it means the indexer pipelines for these tables are not enabled.\n",
    );
  }

  const singleFilters: Array<{ label: string; field: string; value: string }> = [];
  const comboFilters: Array<{ label: string; filter: Record<string, string> }> = [];

  for (const filter of options.filters) {
    const fields = Object.keys(filter);
    if (fields.length === 1) {
      const field = fields[0];
      const value = filter[field];
      const shortVal = value.length > 20 ? value.slice(0, 20) + "..." : value;
      singleFilters.push({ label: `${field}: ${shortVal}`, field, value });
    } else {
      const label = fields.map((f) => `${f}:${filter[f].slice(0, 10)}...`).join(" + ");
      comboFilters.push({ label, filter });
    }
  }

  // Single-field filter tests
  if (singleFilters.length > 0) {
    console.log(`Running ${singleFilters.length} single-filter tests...`);
    console.log();
    const results = await Promise.all(
      singleFilters.map(({ label, field, value }) =>
        runSingleFprTest(scan, label, field, value, cpLo, cpHi, options.sampleSize),
      ),
    );
    printFprTable(results, options.pageSize);
  }

  // Multi-field (combo) filter tests
  if (comboFilters.length > 0) {
    console.log();
    printHeader("COMBO FILTER ANALYSIS (AND conditions)");
    console.log(`Running ${comboFilters.length} combo filter tests...`);
    console.log();
    const results = await Promise.all(
      comboFilters.map(({ label, filter }) =>
        runComboFprTest(scan, label, filter, cpLo, cpHi, options.sampleSize),
      ),
    );
    printComboFprTable(results, options.pageSize);
  }
}

/**
 * Run overfetch factor analysis with auto-discovered parameters (transactions only).
 */
async function runOverfetchAnalysis(
  options: AnalyzeOptions,
  cpLo: number,
  cpHi: number,
  txLo: number,
  txHi: number,
): Promise<void> {
  printHeader("OVERFETCH FACTOR ANALYSIS");
  console.log(`Page size: ${options.pageSize}`);
  console.log(`Checkpoint range: ${cpLo} - ${cpHi}`);
  console.log(`Sample size: ${options.sampleSize}`);
  console.log();
  console.log(`Transaction range: ${txLo} - ${txHi}`);
  console.log();

  const scan = options.scan;

  // Discover test parameters from database
  console.log("Discovering test parameters from database...");
  console.log(
    `Low-freq range: ${options.lowMin.toLocaleString()} - ${options.lowMax.toLocaleString()} occurrences`,
  );

  // High-frequency items
  const [highPkgs, highSenders, highObjs] = await Promise.all([
    queryTopPackages(txLo, txHi, 1),
    queryTopSenders(txLo, txHi, 1),
    queryTopObjects(txLo, txHi, 1),
  ]);

  // Low-frequency items
  const [lowPkgs, lowSenders, lowObjs] = await Promise.all([
    queryLowFreqItems("tx_calls", "package", txLo, txHi, options.lowMin, options.lowMax, 1),
    queryLowFreqItems("tx_affected_addresses", "sender", txLo, txHi, options.lowMin, options.lowMax, 1),
    queryLowFreqItems("tx_affected_objects", "affected", txLo, txHi, options.lowMin, options.lowMax, 1),
  ]);

  console.log();

  const tests: Array<{ label: string; field: string; value: string }> = [];

  if (highPkgs.length > 0) {
    const { address, count } = highPkgs[0];
    tests.push({ label: `High-freq function (${count.toLocaleString()})`, field: "function", value: address });
  }
  if (highSenders.length > 0) {
    const { address, count } = highSenders[0];
    tests.push({ label: `High-freq sentAddress (${count.toLocaleString()})`, field: "sentAddress", value: address });
  }
  if (highObjs.length > 0) {
    const { address, count } = highObjs[0];
    tests.push({ label: `High-freq affectedObject (${count.toLocaleString()})`, field: "affectedObject", value: address });
  }
  if (lowPkgs.length > 0) {
    const { address, count } = lowPkgs[0];
    tests.push({ label: `Low-freq function (${count.toLocaleString()})`, field: "function", value: address });
  }
  if (lowSenders.length > 0) {
    const { address, count } = lowSenders[0];
    tests.push({ label: `Low-freq sentAddress (${count.toLocaleString()})`, field: "sentAddress", value: address });
  }
  if (lowObjs.length > 0) {
    const { address, count } = lowObjs[0];
    tests.push({ label: `Low-freq affectedObject (${count.toLocaleString()})`, field: "affectedObject", value: address });
  }
  tests.push({ label: "DeepBook (dee9)", field: "function", value: DEEPBOOK_PACKAGE });
  tests.push({ label: "Non-existent (baseline)", field: "affectedAddress", value: NONEXISTENT_ADDRESS });

  console.log(`Running FPR tests on ${tests.length} single-filter items...`);
  console.log();

  const results = await Promise.all(
    tests.map(({ label, field, value }) =>
      runSingleFprTest(scan, label, field, value, cpLo, cpHi, options.sampleSize),
    ),
  );

  printFprTable(results, options.pageSize);

  console.log();
  console.log("Interpretation:");
  console.log("- Overfetch factor = 1 / (1 - FPR)");
  console.log(`- To get ${options.pageSize} true results, fetch 'Fetch for ${options.pageSize}' bloom matches`);
}

/**
 * Run blocked bloom filter analysis (transactions only).
 */
async function runBlockedAnalysis(
  options: AnalyzeOptions,
  cpLo: number,
  cpHi: number,
  txLo: number,
  txHi: number,
): Promise<void> {
  const scan = options.scan;

  printHeader("BLOCKED BLOOM FPR SUMMARY ANALYSIS");
  console.log(`Checkpoint range: ${cpLo} - ${cpHi}`);
  console.log(`Checkpoint blocks: ${Math.floor(cpLo / 1000)} - ${Math.floor(cpHi / 1000)}`);
  console.log();

  if (!(await checkBlockedBloomTableExists())) {
    console.log("ERROR: cp_bloom_blocks table does not exist in the database.");
    return;
  }

  console.log(`Transaction range: ${txLo} - ${txHi}`);
  console.log();

  console.log("Discovering active items...");
  const [topPackages, highFreqObj, lowFreqPkg, lowFreqObj, lowFreqSender] =
    await Promise.all([
      queryTopPackages(txLo, txHi, 3),
      queryTopObjects(txLo, txHi, 1),
      queryLowFreqItems("tx_calls", "package", txLo, txHi, options.lowMin, options.lowMax, 1),
      queryLowFreqItems("tx_affected_objects", "affected", txLo, txHi, options.lowMin, options.lowMax, 1),
      queryLowFreqItems("tx_affected_addresses", "sender", txLo, txHi, options.lowMin, options.lowMax, 1),
    ]);

  const tests: Array<{ label: string; field: string; value: string }> = [
    { label: "Package: dee9 (Deepbook)", field: "function", value: DEEPBOOK_PACKAGE },
    { label: "Package: 0x2 (Sui Framework)", field: "function", value: SUI_FRAMEWORK },
  ];

  if (topPackages.length > 0) {
    const { address, count } = topPackages[0];
    tests.push({ label: `Package: ${address.slice(0, 10)}... (${count} txs)`, field: "function", value: address });
  }
  if (highFreqObj.length > 0) {
    const { address, count } = highFreqObj[0];
    tests.push({ label: `Object (high): ${address.slice(0, 10)}... (${count})`, field: "affectedObject", value: address });
  }
  if (lowFreqPkg.length > 0) {
    const { address, count } = lowFreqPkg[0];
    tests.push({ label: `Package (low): ${address.slice(0, 10)}... (${count})`, field: "function", value: address });
  }
  if (lowFreqObj.length > 0) {
    const { address, count } = lowFreqObj[0];
    tests.push({ label: `Object (low): ${address.slice(0, 10)}... (${count})`, field: "affectedObject", value: address });
  }
  if (lowFreqSender.length > 0) {
    const { address, count } = lowFreqSender[0];
    tests.push({ label: `Sender (low): ${address.slice(0, 10)}... (${count})`, field: "sentAddress", value: address });
  }
  tests.push({ label: "Non-existent (baseline)", field: "affectedAddress", value: NONEXISTENT_ADDRESS });

  console.log(`Running ${tests.length} FPR tests...`);
  const results = await Promise.all(
    tests.map(({ label, field, value }) =>
      runBlockedFprTest(scan, label, field, value, cpLo, cpHi, options.sampleSize),
    ),
  );

  console.log();
  printFprTable(results, 50);
}

/**
 * Run the analyze command.
 */
export async function runAnalyze(options: AnalyzeOptions): Promise<void> {
  // Check if we only need to show theoretical table (no database required)
  const onlyTheoretical =
    options.theoretical &&
    !options.sizeDistribution &&
    !options.density &&
    !options.overfetch &&
    !options.blocked &&
    options.filters.length === 0;

  if (onlyTheoretical) {
    analyzeTheoreticalFpr();
    return;
  }

  initPool(options.databaseUrl);

  try {
    console.log("=".repeat(60));
    console.log(`Bloom Filter Analysis (scan${options.scan === "transactions" ? "Transactions" : "Events"})`);
    console.log("=".repeat(60));
    console.log(`Checkpoint range: ${options.cpLo} - ${options.cpHi}`);
    console.log(`Sample size: ${options.sampleSize}`);

    // Get effective checkpoint range
    const dbRange = await getAvailableCheckpointRange();
    const cpLo = Math.max(options.cpLo, dbRange.lo);
    const cpHi = Math.min(options.cpHi, dbRange.hi);
    const txRange = await getCheckpointTxRange(cpLo, cpHi);

    if (options.sizeDistribution) {
      await analyzeSizeDistribution(cpLo, cpHi);
    }

    if (options.density) {
      await analyzeDensityDistribution(cpLo, cpHi, options.sampleSize);
    }

    if (options.theoretical) {
      analyzeTheoreticalFpr();
    }

    if (options.filters.length > 0) {
      await runFilterAnalysis(options, cpLo, cpHi);
    } else if (options.overfetch) {
      await runOverfetchAnalysis(options, cpLo, cpHi, txRange.lo, txRange.hi);
    } else if (options.blocked) {
      await runBlockedAnalysis(options, cpLo, cpHi, txRange.lo, txRange.hi);
    } else if (!options.sizeDistribution && !options.theoretical && !options.density) {
      // Default: show size distribution and theoretical FPR
      await analyzeSizeDistribution(cpLo, cpHi);
      analyzeTheoreticalFpr();
    }
  } finally {
    await closePool();
  }
}
