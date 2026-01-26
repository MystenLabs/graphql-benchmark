#!/usr/bin/env ts-node
/**
 * Scanning CLI for bloom filter analysis and parameter discovery.
 *
 * Supports both transaction and event scanning, aligned with the
 * GraphQL scanTransactions / scanEvents API.
 *
 * Usage:
 *   pnpm scan transactions discover --low-freq
 *   pnpm scan transactions analyze --filter '{"sentAddress": "0x..."}'
 *   pnpm scan events discover
 *   pnpm scan events analyze --filter '{"module": "0xpkg::mod"}'
 *   pnpm scan saturation
 */

import yargs from "yargs";
import { hideBin } from "yargs/helpers";
import { loadConfig, getDatabaseUrl } from "./src/config";
import { runDiscover } from "./src/commands/discover";
import { runAnalyze } from "./src/commands/analyze";
import { runSaturation } from "./src/commands/saturation";
import type { ScanTarget } from "./src/types";

const config = loadConfig();

/** Parse --filter JSON strings into objects */
function parseFilters(raw: string[]): Array<Record<string, string>> {
  return raw.map((s) => {
    try {
      const obj = JSON.parse(s);
      if (typeof obj !== "object" || obj === null || Array.isArray(obj)) {
        throw new Error("must be a JSON object");
      }
      return obj as Record<string, string>;
    } catch (e) {
      throw new Error(
        `Invalid --filter value: ${s}\n` +
          `Expected a JSON object, e.g. '{"sentAddress": "0x..."}'`,
      );
    }
  });
}

/** Shared analyze options for both transactions and events */
function analyzeOptions(y: yargs.Argv) {
  return y
    .option("filter", {
      describe:
        'JSON filter object (repeatable). E.g. \'{"sentAddress": "0x..."}\'',
      type: "string",
      array: true,
      default: [] as string[],
    })
    .option("overfetch", {
      describe: "Run auto-discovered overfetch factor analysis",
      type: "boolean",
      default: false,
    })
    .option("blocked", {
      describe: "Run blocked bloom filter analysis",
      type: "boolean",
      default: false,
    })
    .option("size-distribution", {
      describe: "Show bloom filter size distribution",
      type: "boolean",
      default: false,
    })
    .option("density", {
      describe: "Show bloom filter density distribution",
      type: "boolean",
      default: false,
    })
    .option("theoretical", {
      describe: "Show theoretical FPR table",
      type: "boolean",
      default: false,
    })
    .option("cp-lo", {
      describe: "Lower checkpoint bound",
      type: "number",
      default: config.discovery.checkpoint_lo ?? 0,
    })
    .option("cp-hi", {
      describe: "Upper checkpoint bound",
      type: "number",
      default: config.discovery.checkpoint_hi ?? 999999999,
    })
    .option("sample", {
      describe: "Sample size for analysis",
      type: "number",
      default: 10000,
    })
    .option("page-size", {
      describe: "Page size for overfetch calculation",
      type: "number",
      default: config.benchmark.limit,
    })
    .option("low-min", {
      describe: "Min count for low-freq items",
      type: "number",
      default: 10000,
    })
    .option("low-max", {
      describe: "Max count for low-freq items",
      type: "number",
      default: 100000,
    })
    .option("params-file", {
      alias: "p",
      describe: "Load test parameters from JSON file",
      type: "string",
    })
    .option("output", {
      alias: "o",
      describe: "Output directory for JSON results",
      type: "string",
      default: config.benchmark.output_dir,
    });
}

/** Shared discover options */
function discoverOptions(y: yargs.Argv) {
  return y
    .option("low-freq", {
      describe: "Also find low-frequency items",
      type: "boolean",
      default: false,
    })
    .option("low-freq-min", {
      describe: "Minimum count for low-frequency items",
      type: "number",
      default: config.discovery.low_freq_min,
    })
    .option("low-freq-max", {
      describe: "Maximum count for low-frequency items",
      type: "number",
      default: config.discovery.low_freq_max,
    })
    .option("cp-lo", {
      describe: "Lower checkpoint bound",
      type: "number",
      default: config.discovery.checkpoint_lo ?? 0,
    })
    .option("cp-hi", {
      describe: "Upper checkpoint bound",
      type: "number",
      default: config.discovery.checkpoint_hi ?? 999999999,
    })
    .option("top", {
      describe: "Number of top items to find",
      type: "number",
      default: config.discovery.top_n,
    })
    .option("json", {
      describe: "Output results as JSON to stdout",
      type: "boolean",
      default: false,
    })
    .option("generate-params", {
      describe: "Generate benchmark parameters.json format",
      type: "boolean",
      default: false,
    })
    .option("output", {
      alias: "o",
      describe: "Output directory for results",
      type: "string",
    });
}

/** Build analyze handler for a given scan target */
function analyzeHandler(scan: ScanTarget) {
  return async (argv: any) => {
    try {
      const onlyTheoretical =
        argv.theoretical &&
        !argv["size-distribution"] &&
        !argv.density &&
        !argv.overfetch &&
        !argv.blocked &&
        argv.filter.length === 0;

      const databaseUrl = onlyTheoretical ? "" : getDatabaseUrl(argv.db, config);
      const filters = parseFilters(argv.filter);

      await runAnalyze({
        scan,
        databaseUrl,
        cpLo: argv["cp-lo"],
        cpHi: argv["cp-hi"],
        sampleSize: argv.sample,
        pageSize: argv["page-size"],
        lowMin: argv["low-min"],
        lowMax: argv["low-max"],
        overfetch: argv.overfetch,
        blocked: argv.blocked,
        sizeDistribution: argv["size-distribution"],
        density: argv.density,
        theoretical: argv.theoretical,
        filters,
        paramsFile: argv["params-file"],
        outputDir: argv.output,
      });
    } catch (error) {
      console.error("Error:", (error as Error).message);
      process.exit(1);
    }
  };
}

/** Build discover handler for a given scan target */
function discoverHandler(scan: ScanTarget) {
  return async (argv: any) => {
    try {
      const databaseUrl = getDatabaseUrl(argv.db, config);
      await runDiscover({
        scan,
        databaseUrl,
        cpLo: argv["cp-lo"],
        cpHi: argv["cp-hi"],
        topN: argv.top,
        lowFreq: argv["low-freq"],
        lowFreqMin: argv["low-freq-min"],
        lowFreqMax: argv["low-freq-max"],
        outputDir: argv.output,
        json: argv.json,
        generateParams: argv["generate-params"],
      });
    } catch (error) {
      console.error("Error:", (error as Error).message);
      process.exit(1);
    }
  };
}

yargs(hideBin(process.argv))
  .scriptName("scan")
  .usage("$0 <command> [options]")
  .option("db", {
    describe: "Database URL (overrides config/env)",
    type: "string",
  })
  // ── transactions ─────────────────────────────────────────────────
  .command(
    "transactions",
    "Analyze scanTransactions bloom filters",
    (y) =>
      y
        .command(
          "discover",
          "Discover transaction filter test parameters",
          discoverOptions,
          discoverHandler("transactions"),
        )
        .command(
          "analyze",
          "Analyze transaction bloom filter FPR",
          analyzeOptions,
          analyzeHandler("transactions"),
        )
        .demandCommand(1, "Specify: discover or analyze"),
  )
  // ── events ───────────────────────────────────────────────────────
  .command(
    "events",
    "Analyze scanEvents bloom filters",
    (y) =>
      y
        .command(
          "discover",
          "Discover event filter test parameters",
          discoverOptions,
          discoverHandler("events"),
        )
        .command(
          "analyze",
          "Analyze event bloom filter FPR",
          analyzeOptions,
          analyzeHandler("events"),
        )
        .demandCommand(1, "Specify: discover or analyze"),
  )
  // ── saturation (shared, no scan target needed) ───────────────────
  .command(
    "saturation",
    "Analyze bloom filter saturation (cp_blooms + cp_bloom_blocks)",
    (y) =>
      y
        .option("cp-lo", {
          describe: "Lower checkpoint bound",
          type: "number",
          default: config.discovery.checkpoint_lo ?? 0,
        })
        .option("cp-hi", {
          describe: "Upper checkpoint bound",
          type: "number",
          default: config.discovery.checkpoint_hi ?? 999999999,
        }),
    async (argv) => {
      try {
        const databaseUrl = getDatabaseUrl(argv.db, config);
        await runSaturation({
          databaseUrl,
          cpLo: argv["cp-lo"],
          cpHi: argv["cp-hi"],
        });
      } catch (error) {
        console.error("Error:", (error as Error).message);
        process.exit(1);
      }
    },
  )
  .demandCommand(1, "Specify: transactions, events, or saturation")
  .strict()
  .help()
  .alias("h", "help")
  .parse();
