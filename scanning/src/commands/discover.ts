/**
 * Parameter discovery command - finds high/low frequency items from database.
 *
 * Supports both transaction and event scanning.
 */

import * as fs from "fs";
import * as path from "path";
import type {
  DiscoveryResults,
  DiscoveryItem,
  BenchmarkParams,
  ScanTarget,
} from "../types";
import { printHeader } from "../utils";
import { initPool, closePool } from "../db/connection";
import {
  getAvailableCheckpointRange,
  getCheckpointTxRange,
  queryTopPackages,
  queryTopSenders,
  queryTopAffectedAddresses,
  queryTopObjects,
  queryLowFreqItems,
} from "../db/queries";
import {
  queryTopEmitModules,
  queryTopEventTypes,
  queryTopEventSenders,
  queryLowFreqEmitModules,
  queryLowFreqEventTypes,
} from "../db/event-queries";

export interface DiscoverOptions {
  scan: ScanTarget;
  databaseUrl: string;
  cpLo: number;
  cpHi: number;
  topN: number;
  lowFreq: boolean;
  lowFreqMin: number;
  lowFreqMax: number;
  outputDir?: string;
  json: boolean;
  generateParams: boolean;
}


function printResults(
  title: string,
  results: Array<{ address: string; count: number }>,
): void {
  console.log(`\n${title}`);
  console.log("-".repeat(80));
  console.log(`${"Address".padEnd(68)} ${"Count".padStart(10)}`);
  console.log("-".repeat(80));
  for (const { address, count } of results) {
    console.log(`${address.padEnd(68)} ${count.toLocaleString().padStart(10)}`);
  }
  console.log();
}

/**
 * Find high-frequency items for FPR testing.
 */
async function findHighFreqItems(
  txLo: number,
  txHi: number,
  limit: number,
): Promise<{
  packages: DiscoveryItem[];
  senders: DiscoveryItem[];
  affected_addresses: DiscoveryItem[];
  objects: DiscoveryItem[];
}> {
  const [packages, senders, affectedAddresses, objects] = await Promise.all([
    queryTopPackages(txLo, txHi, limit),
    queryTopSenders(txLo, txHi, limit),
    queryTopAffectedAddresses(txLo, txHi, limit),
    queryTopObjects(txLo, txHi, limit),
  ]);

  return {
    packages: packages.map((p) => ({ address: p.address, count: p.count })),
    senders: senders.map((s) => ({ address: s.address, count: s.count })),
    affected_addresses: affectedAddresses.map((a) => ({
      address: a.address,
      count: a.count,
    })),
    objects: objects.map((o) => ({ address: o.address, count: o.count })),
  };
}

/**
 * Find low-frequency items for FPR testing.
 */
async function findLowFreqItems(
  txLo: number,
  txHi: number,
  minCount: number,
  maxCount: number,
  limit: number,
): Promise<{
  packages: DiscoveryItem[];
  senders: DiscoveryItem[];
  affected_addresses: DiscoveryItem[];
  objects: DiscoveryItem[];
}> {
  const [packages, senders, affectedAddresses, objects] = await Promise.all([
    queryLowFreqItems("tx_calls", "package", txLo, txHi, minCount, maxCount, limit),
    queryLowFreqItems(
      "tx_affected_addresses",
      "sender",
      txLo,
      txHi,
      minCount,
      maxCount,
      limit,
    ),
    queryLowFreqItems(
      "tx_affected_addresses",
      "affected",
      txLo,
      txHi,
      minCount,
      maxCount,
      limit,
    ),
    queryLowFreqItems(
      "tx_affected_objects",
      "affected",
      txLo,
      txHi,
      minCount,
      maxCount,
      limit,
    ),
  ]);

  return {
    packages: packages.map((p) => ({ address: p.address, count: p.count })),
    senders: senders.map((s) => ({ address: s.address, count: s.count })),
    affected_addresses: affectedAddresses.map((a) => ({
      address: a.address,
      count: a.count,
    })),
    objects: objects.map((o) => ({ address: o.address, count: o.count })),
  };
}

/**
 * Generate benchmark parameters from discovery results.
 */
export function generateBenchmarkParams(
  results: DiscoveryResults,
  useLowFreqOnly: boolean = false,
): BenchmarkParams {
  const params: BenchmarkParams = {
    filter: {
      function: [],
      sentAddress: [],
      affectedAddress: [],
      affectedObject: [],
      afterCheckpoint: [],
      beforeCheckpoint: [],
    },
  };

  const highFreq = results.high_freq;
  const lowFreq = results.low_freq;

  function getMixedItems(
    highItems: DiscoveryItem[],
    lowItems: DiscoveryItem[],
    count: number = 3,
  ): string[] {
    if (useLowFreqOnly && lowItems.length > 0) {
      return lowItems.slice(0, count).map((i) => i.address);
    } else if (lowItems.length > 0 && highItems.length > 0) {
      // Mix: take some high-freq and some low-freq
      const nHigh = Math.ceil(count / 2);
      const nLow = count - nHigh;
      return [
        ...highItems.slice(0, nHigh).map((i) => i.address),
        ...lowItems.slice(0, nLow).map((i) => i.address),
      ];
    } else if (highItems.length > 0) {
      return highItems.slice(0, count).map((i) => i.address);
    }
    return [];
  }

  params.filter.function = getMixedItems(highFreq.packages, lowFreq.packages);
  params.filter.sentAddress = getMixedItems(highFreq.senders, lowFreq.senders);
  params.filter.affectedAddress = getMixedItems(
    highFreq.affected_addresses,
    lowFreq.affected_addresses,
  );
  params.filter.affectedObject = getMixedItems(highFreq.objects, lowFreq.objects);

  // Add checkpoint ranges
  const { lo, hi } = results.checkpoint_range;
  const step = Math.floor((hi - lo) / 4);
  if (step > 0) {
    params.filter.afterCheckpoint = [lo, lo + step, lo + 2 * step];
    params.filter.beforeCheckpoint = [lo + step, lo + 2 * step, lo + 3 * step];
  }

  return params;
}

/**
 * Run event-specific discovery.
 */
async function runEventDiscover(options: DiscoverOptions): Promise<void> {
  initPool(options.databaseUrl);

  try {
    printHeader("FINDING EVENT TEST PARAMETERS");

    const dbRange = await getAvailableCheckpointRange();
    console.log(`Available checkpoint range: ${dbRange.lo} - ${dbRange.hi}`);

    const effectiveCpLo = Math.max(options.cpLo, dbRange.lo);
    const effectiveCpHi = Math.min(options.cpHi, dbRange.hi);
    console.log(`Effective checkpoint range: ${effectiveCpLo} - ${effectiveCpHi}`);

    const txRange = await getCheckpointTxRange(effectiveCpLo, effectiveCpHi);
    console.log(`Transaction range: ${txRange.lo} - ${txRange.hi}`);

    if (txRange.lo === 0 && txRange.hi === 0) {
      console.error("Error: No transactions found in checkpoint range");
      return;
    }

    // Discover high-frequency event items
    console.log("\nFinding high-frequency event items...");
    const [modules, types, senders] = await Promise.all([
      queryTopEmitModules(txRange.lo, txRange.hi, options.topN),
      queryTopEventTypes(txRange.lo, txRange.hi, options.topN),
      queryTopEventSenders(txRange.lo, txRange.hi, options.topN),
    ]);

    console.log(`\nTop ${options.topN} Emitting Modules:`);
    console.log("-".repeat(80));
    console.log(`${"Module".padEnd(68)} ${"Count".padStart(10)}`);
    console.log("-".repeat(80));
    for (const m of modules) {
      const label = `${m.package}::${m.module}`;
      console.log(`${label.padEnd(68)} ${m.count.toLocaleString().padStart(10)}`);
    }

    console.log(`\nTop ${options.topN} Event Types:`);
    console.log("-".repeat(80));
    console.log(`${"Type".padEnd(68)} ${"Count".padStart(10)}`);
    console.log("-".repeat(80));
    for (const t of types) {
      const label = `${t.package}::${t.module}::${t.name}`;
      console.log(`${label.padEnd(68)} ${t.count.toLocaleString().padStart(10)}`);
    }

    console.log(`\nTop ${options.topN} Event Senders:`);
    console.log("-".repeat(80));
    console.log(`${"Address".padEnd(68)} ${"Count".padStart(10)}`);
    console.log("-".repeat(80));
    for (const s of senders) {
      console.log(`${s.address.padEnd(68)} ${s.count.toLocaleString().padStart(10)}`);
    }

    // Low-frequency items
    if (options.lowFreq) {
      console.log(`\nFinding low-frequency event items (${options.lowFreqMin}-${options.lowFreqMax})...`);
      const [lowModules, lowTypes] = await Promise.all([
        queryLowFreqEmitModules(txRange.lo, txRange.hi, options.lowFreqMin, options.lowFreqMax, 5),
        queryLowFreqEventTypes(txRange.lo, txRange.hi, options.lowFreqMin, options.lowFreqMax, 5),
      ]);

      if (lowModules.length > 0) {
        console.log(`\nLow-frequency Emitting Modules:`);
        for (const m of lowModules) {
          console.log(`  ${m.package}::${m.module}  (${m.count.toLocaleString()})`);
        }
      }
      if (lowTypes.length > 0) {
        console.log(`\nLow-frequency Event Types:`);
        for (const t of lowTypes) {
          console.log(`  ${t.package}::${t.module}::${t.name}  (${t.count.toLocaleString()})`);
        }
      }
    }

    // Generate event parameters JSON
    if (options.json || options.generateParams) {
      const params = {
        filter: {
          sender: senders.slice(0, 3).map((s) => s.address),
          module: modules.slice(0, 5).map((m) => `${m.package}::${m.module}`),
          type: types.slice(0, 5).map((t) => `${t.package}::${t.module}::${t.name}`),
          afterCheckpoint: [effectiveCpLo],
          beforeCheckpoint: [effectiveCpHi],
        },
      };

      if (options.json) {
        printHeader("JSON OUTPUT");
        console.log(JSON.stringify({ modules, types, senders }, null, 2));
      }
      if (options.generateParams) {
        printHeader("BENCHMARK PARAMETERS (events-scanning)");
        console.log(JSON.stringify(params, null, 2));
      }

      if (options.outputDir) {
        const dir = options.outputDir;
        if (!fs.existsSync(dir)) {
          fs.mkdirSync(dir, { recursive: true });
        }
        const timestamp = new Date()
          .toISOString()
          .replace(/[-:]/g, "")
          .slice(0, 15)
          .replace("T", "_");
        const paramsPath = path.join(dir, `event_parameters_${timestamp}.json`);
        fs.writeFileSync(paramsPath, JSON.stringify(params, null, 2));
        console.log(`\nEvent parameters written to: ${paramsPath}`);
      }
    }
  } finally {
    await closePool();
  }
}

/**
 * Run the discover command.
 */
export async function runDiscover(options: DiscoverOptions): Promise<void> {
  if (options.scan === "events") {
    return runEventDiscover(options);
  }

  initPool(options.databaseUrl);

  try {
    printHeader("FINDING TRANSACTION TEST PARAMETERS");

    // Get the actual available checkpoint range from the database
    const dbRange = await getAvailableCheckpointRange();
    console.log(
      `Available checkpoint range in DB: ${dbRange.lo} - ${dbRange.hi}`,
    );

    // Use intersection of user-provided range and available range
    const effectiveCpLo = Math.max(options.cpLo, dbRange.lo);
    const effectiveCpHi = Math.min(options.cpHi, dbRange.hi);
    console.log(`Effective checkpoint range: ${effectiveCpLo} - ${effectiveCpHi}`);

    // Get transaction range
    const txRange = await getCheckpointTxRange(effectiveCpLo, effectiveCpHi);
    console.log(`Transaction range: ${txRange.lo} - ${txRange.hi}`);

    if (txRange.lo === 0 && txRange.hi === 0) {
      console.error("Error: No transactions found in checkpoint range");
      return;
    }

    const results: DiscoveryResults = {
      checkpoint_range: { lo: effectiveCpLo, hi: effectiveCpHi },
      available_checkpoint_range: dbRange,
      transaction_range: txRange,
      high_freq: {
        packages: [],
        senders: [],
        affected_addresses: [],
        objects: [],
      },
      low_freq: {
        packages: [],
        senders: [],
        affected_addresses: [],
        objects: [],
      },
    };

    // Find high-frequency items
    console.log("\nFinding high-frequency items...");
    const highFreq = await findHighFreqItems(
      txRange.lo,
      txRange.hi,
      options.topN,
    );
    results.high_freq = highFreq;

    printResults(`Top ${options.topN} Packages (by call count)`, highFreq.packages);
    printResults(`Top ${options.topN} Senders (by tx count)`, highFreq.senders);
    printResults(
      `Top ${options.topN} Affected Addresses (by tx count)`,
      highFreq.affected_addresses,
    );
    printResults(`Top ${options.topN} Objects (by tx count)`, highFreq.objects);

    // Find low-frequency items if requested
    if (options.lowFreq) {
      console.log(
        `\nFinding low-frequency items (${options.lowFreqMin}-${options.lowFreqMax} occurrences)...`,
      );
      const lowFreq = await findLowFreqItems(
        txRange.lo,
        txRange.hi,
        options.lowFreqMin,
        options.lowFreqMax,
        5,
      );
      results.low_freq = lowFreq;

      printResults(
        `Low-frequency Packages (~${options.lowFreqMin}-${options.lowFreqMax} calls)`,
        lowFreq.packages,
      );
      printResults(
        `Low-frequency Senders (~${options.lowFreqMin}-${options.lowFreqMax} txs)`,
        lowFreq.senders,
      );
      printResults(
        `Low-frequency Affected Addresses (~${options.lowFreqMin}-${options.lowFreqMax} txs)`,
        lowFreq.affected_addresses,
      );
      printResults(
        `Low-frequency Objects (~${options.lowFreqMin}-${options.lowFreqMax} txs)`,
        lowFreq.objects,
      );
    }

    // Print summary
    printHeader("SUMMARY");

    if (highFreq.packages.length > 0) {
      const { address, count } = highFreq.packages[0];
      console.log(`Highest-activity package: ${address}`);
      console.log(`  Call count: ${count.toLocaleString()}`);
    }

    if (highFreq.senders.length > 0) {
      const { address, count } = highFreq.senders[0];
      console.log(`\nHighest-activity sender: ${address}`);
      console.log(`  Transaction count: ${count.toLocaleString()}`);
    }

    if (highFreq.objects.length > 0) {
      const { address, count } = highFreq.objects[0];
      console.log(`\nHighest-activity object: ${address}`);
      console.log(`  Transaction count: ${count.toLocaleString()}`);
    }

    // Generate benchmark parameters
    const params = generateBenchmarkParams(results, false);

    if (options.json) {
      printHeader("JSON OUTPUT");
      console.log(JSON.stringify(results, null, 2));
    }

    if (options.generateParams) {
      printHeader("BENCHMARK PARAMETERS");
      console.log(JSON.stringify(params, null, 2));
    }

    // Write output files
    if (options.outputDir) {
      const dir = options.outputDir;
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }

      const timestamp = new Date()
        .toISOString()
        .replace(/[-:]/g, "")
        .slice(0, 15)
        .replace("T", "_");
      const analysisPath = path.join(dir, `analysis_${timestamp}.json`);
      const paramsPath = path.join(dir, `parameters_${timestamp}.json`);

      fs.writeFileSync(analysisPath, JSON.stringify(results, null, 2));
      fs.writeFileSync(paramsPath, JSON.stringify(params, null, 2));

      printHeader("OUTPUT FILES");
      console.log(`Analysis results: ${analysisPath}`);
      console.log(`Benchmark params: ${paramsPath}`);
      console.log();
      console.log("To use parameters for benchmarking:");
      console.log(`  cp ${paramsPath} parameters.json`);
    }
  } finally {
    await closePool();
  }
}
