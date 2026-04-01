/**
 * Shared types for scanning tools.
 */

/** Configuration file structure */
export interface Config {
  connection: ConnectionConfig;
  benchmark: BenchmarkConfig;
  discovery: DiscoveryConfig;
  bloom: BloomConfig;
}

export interface ConnectionConfig {
  graphql_url: string;
  database_url: string | null;
}

export interface BenchmarkConfig {
  limit: number;
  num_pages: number;
  min_filters: number;
  require_checkpoint_bounds: boolean;
  output_dir: string;
}

export interface DiscoveryConfig {
  checkpoint_lo: number | null;
  checkpoint_hi: number | null;
  top_n: number;
  low_freq_min: number;
  low_freq_max: number;
}

export interface BloomConfig {
  seed: number;
  num_bits: number;
  num_hashes: number;
  min_fold_bits: number;
  max_fold_density: number;
}

/** Discovery result item */
export interface DiscoveryItem {
  address: string;
  count: number;
}

/** Discovery results */
export interface DiscoveryResults {
  checkpoint_range: { lo: number; hi: number };
  available_checkpoint_range: { lo: number; hi: number };
  transaction_range: { lo: number; hi: number };
  high_freq: {
    packages: DiscoveryItem[];
    senders: DiscoveryItem[];
    affected_addresses: DiscoveryItem[];
    objects: DiscoveryItem[];
  };
  low_freq: {
    packages: DiscoveryItem[];
    senders: DiscoveryItem[];
    affected_addresses: DiscoveryItem[];
    objects: DiscoveryItem[];
  };
}

/** Scan target - aligns with GraphQL scanTransactions / scanEvents */
export type ScanTarget = "transactions" | "events";

/** Key type for bloom filter lookups (transaction filters) */
export type TxKeyType = "package" | "object" | "address" | "sender";

/** Key type for bloom filter lookups (event filters) */
export type EventKeyType = "module" | "type" | "ev_sender";

/** Combined key type */
export type KeyType = TxKeyType | EventKeyType;

/** Single filter test key */
export interface TestKey {
  type: KeyType;
  key: string;
  frequency: "high" | "low" | "known" | "nonexistent" | "unknown";
  count: number | null;
}

/** Combo filter test key */
export interface ComboTestKey {
  label?: string;
  filters: Array<{ type: KeyType; key: string }>;
  count: number | null;
}

/** FPR test result */
export interface FPRResult {
  label: string;
  bloom_matches: number;
  true_positives: number;
  false_positives: number;
  fpr: number | "inf";
  overfetch: number | "inf";
  no_matching_txs: boolean;
  // Optional fields for combo filter breakdown
  partial_matches?: number; // Some filters match, not all (known pair issue)
  pure_false_positives?: number; // No filter matches any tx (true bloom collision)
}

/** Overfetch analysis results */
export interface OverfetchAnalysisResults {
  config: {
    page_size: number;
    checkpoint_range: { lo: number; hi: number };
    transaction_range: { lo: number; hi: number };
    sample_size: number;
    low_freq_range: { min: number; max: number };
  };
  single_filter_tests: {
    keys: TestKey[];
    results: FPRResult[];
  };
  combo_filter_tests: {
    keys: ComboTestKey[];
    results: FPRResult[];
  };
}

/** Benchmark parameters format */
export interface BenchmarkParams {
  filter: {
    function: string[];
    sentAddress: string[];
    affectedAddress: string[];
    affectedObject: string[];
    afterCheckpoint: number[];
    beforeCheckpoint: number[];
  };
}
