/**
 * Bloom filter constants matching Rust implementation.
 */

/** Bloom filter seed (matches Rust BLOOM_SEED in cp_blooms.rs) */
export const BLOOM_SEED = 67n;

/** Per-checkpoint bloom filter settings */
export const CP_BLOOM_NUM_BITS = 131072; // 16KB before folding
export const CP_BLOOM_NUM_HASHES = 6;

/** Double hashing multiplier (from hash.rs) - 2^64 / π */
export const H2_MULTIPLIER = 0x517cc1b727220a95n;

/** Folding settings */
export const MIN_FOLD_BITS = 8192; // 1KB minimum
export const MAX_FOLD_DENSITY = 0.4;

/** Blocked bloom filter settings (for cp_bloom_blocks) */
export const BLOCKED_BLOOM_NUM_BLOCKS = 128;
export const BLOCKED_BLOOM_BITS_PER_BLOCK = 16384; // 2KB per block
export const BLOCKED_BLOOM_NUM_HASHES = 5;

/** Checkpoints per block for blocked bloom filters */
export const CHECKPOINTS_PER_BLOCK = 1000;

/** Well-known packages */
export const DEEPBOOK_PACKAGE =
  "0x000000000000000000000000000000000000000000000000000000000000dee9";
export const SUI_FRAMEWORK =
  "0x0000000000000000000000000000000000000000000000000000000000000002";

/** Non-existent address for baseline FPR testing */
export const NONEXISTENT_ADDRESS =
  "0x0000000000000000000000000000000000000000000000000000000000000001";
