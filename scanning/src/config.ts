/**
 * Configuration loader with precedence: CLI > env > file > defaults.
 */

import * as fs from "fs";
import * as path from "path";
import type { Config } from "./types";

const DEFAULT_CONFIG: Config = {
  connection: {
    graphql_url: "http://127.0.0.1:7001/graphql",
    database_url: null,
  },
  benchmark: {
    limit: 50,
    num_pages: 3,
    min_filters: 2,
    require_checkpoint_bounds: true,
    output_dir: "experiments",
  },
  discovery: {
    checkpoint_lo: null,
    checkpoint_hi: null,
    top_n: 10,
    low_freq_min: 100,
    low_freq_max: 1000,
  },
  bloom: {
    seed: 67,
    num_bits: 131072,
    num_hashes: 6,
    min_fold_bits: 8192,
    max_fold_density: 0.4,
  },
};

/**
 * Load configuration from file.
 * @param configPath - Path to config file (optional)
 * @returns Loaded config merged with defaults
 */
export function loadConfig(configPath?: string): Config {
  const defaultConfigPaths = [
    path.join(process.cwd(), "benchmark.config.json"),
    path.join(__dirname, "..", "benchmark.config.json"),
  ];

  const pathsToTry = configPath ? [configPath] : defaultConfigPaths;

  let fileConfig: Partial<Config> = {};

  for (const p of pathsToTry) {
    try {
      if (fs.existsSync(p)) {
        const content = fs.readFileSync(p, "utf-8");
        fileConfig = JSON.parse(content);
        break;
      }
    } catch {
      // Continue to next path
    }
  }

  // Deep merge with defaults
  return mergeConfig(DEFAULT_CONFIG, fileConfig);
}

/**
 * Deep merge two config objects.
 */
function mergeConfig(defaults: Config, overrides: Partial<Config>): Config {
  return {
    connection: { ...defaults.connection, ...overrides.connection },
    benchmark: { ...defaults.benchmark, ...overrides.benchmark },
    discovery: { ...defaults.discovery, ...overrides.discovery },
    bloom: { ...defaults.bloom, ...overrides.bloom },
  };
}

/**
 * Get database URL from CLI arg, env var, or config.
 * @param cliValue - Value from CLI argument
 * @param config - Loaded config
 * @returns Database URL or throws if not found
 */
export function getDatabaseUrl(
  cliValue: string | undefined,
  config: Config,
): string {
  const url =
    cliValue || process.env.DATABASE_URL || config.connection.database_url;

  if (!url) {
    throw new Error(
      "Database URL required. Use --db, set DATABASE_URL env var, or configure in benchmark.config.json",
    );
  }

  return url;
}

/**
 * Get GraphQL URL from CLI arg, env var, or config.
 * @param cliValue - Value from CLI argument
 * @param config - Loaded config
 * @returns GraphQL URL
 */
export function getGraphqlUrl(
  cliValue: string | undefined,
  config: Config,
): string {
  return (
    cliValue || process.env.GRAPHQL_URL || config.connection.graphql_url
  );
}
