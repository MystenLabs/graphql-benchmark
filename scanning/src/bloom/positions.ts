/**
 * Bloom filter position calculation and SQL condition building.
 */

import { parseHex } from "./siphash";
import { computeHashPositions } from "./hash";
import {
  BLOOM_SEED,
  CP_BLOOM_NUM_BITS,
  CP_BLOOM_NUM_HASHES,
  BLOCKED_BLOOM_NUM_BLOCKS,
  BLOCKED_BLOOM_BITS_PER_BLOCK,
  BLOCKED_BLOOM_NUM_HASHES,
} from "./constants";
import { type BloomValue, bloomValueToBytes } from "./values";

/**
 * Compute bit positions for a key in a per-checkpoint bloom filter.
 *
 * DEPRECATED: Use computePositionsForValues() instead, which applies correct
 * bloom value tagging. This function passes raw hex bytes without tags, which
 * is only correct for SenderOrRecipient (untagged) bloom values.
 *
 * @param keyHex - Hex string of the key (with or without 0x prefix)
 * @param numBits - Total bits in the bloom filter (default: 131072)
 * @param numHashes - Number of hash positions (default: 6)
 * @param seed - Seed for the hash function (default: 67)
 * @returns List of bit positions
 */
export function computePositions(
  keyHex: string,
  numBits: number = CP_BLOOM_NUM_BITS,
  numHashes: number = CP_BLOOM_NUM_HASHES,
  seed: bigint = BLOOM_SEED,
): number[] {
  const keyBytes = parseHex(keyHex);
  return computeHashPositions(keyBytes, numBits, numHashes, seed);
}

/**
 * Compute bit positions for a set of bloom values in a per-checkpoint bloom filter.
 * All positions from all values are combined (ANDed when checking the bloom filter).
 *
 * This correctly applies bloom value tagging matching the Rust indexer.
 */
export function computePositionsForValues(
  values: BloomValue[],
  numBits: number = CP_BLOOM_NUM_BITS,
  numHashes: number = CP_BLOOM_NUM_HASHES,
  seed: bigint = BLOOM_SEED,
): number[] {
  const allPositions: number[] = [];
  for (const value of values) {
    const bytes = bloomValueToBytes(value);
    allPositions.push(...computeHashPositions(bytes, numBits, numHashes, seed));
  }
  return allPositions;
}

/**
 * Compute block index and bit positions for a blocked bloom filter.
 *
 * The blocked bloom filter uses:
 * - seed = cp_block_id for block selection (via DoubleHasher.nextHash)
 * - seed = cp_block_id + 1 for bit positions within the block
 *
 * @param keyHex - Hex string of the key (raw, no tagging applied)
 * @param cpBlockId - The checkpoint block ID (used as seed)
 * @returns Tuple of (block_index, list of bit positions within that block)
 */
export function computeBlockedPositions(
  keyHex: string,
  cpBlockId: number,
): [number, number[]] {
  const keyBytes = parseHex(keyHex);
  return computeBlockedPositionsFromBytes(keyBytes, cpBlockId);
}

/**
 * Compute block index and bit positions for a bloom value in a blocked bloom filter.
 */
export function computeBlockedPositionsForValue(
  value: BloomValue,
  cpBlockId: number,
): [number, number[]] {
  const keyBytes = bloomValueToBytes(value);
  return computeBlockedPositionsFromBytes(keyBytes, cpBlockId);
}

/**
 * Compute block index and bit positions from raw bytes.
 */
function computeBlockedPositionsFromBytes(
  keyBytes: Uint8Array,
  cpBlockId: number,
): [number, number[]] {
  const seed = BigInt(cpBlockId);

  // Block selection: use DoubleHasher with base seed, take first hash
  // This matches Rust: hasher.next_hash() % BLOCKS
  const blockPositions = computeHashPositions(keyBytes, BLOCKED_BLOOM_NUM_BLOCKS, 1, seed);
  const blockIdx = blockPositions[0];

  // Bit positions with seed+1 using DoubleHasher
  const positions = computeHashPositions(
    keyBytes,
    BLOCKED_BLOOM_BITS_PER_BLOCK,
    BLOCKED_BLOOM_NUM_HASHES,
    seed + 1n,
  );

  return [blockIdx, positions];
}

/**
 * Build SQL condition for checking bloom filter bits.
 *
 * The condition handles folding by using modulo with the actual filter size.
 *
 * @param positions - List of bit positions to check
 * @param filterSize - If provided, uses fixed size; otherwise uses dynamic folding
 * @returns SQL condition string
 */
export function buildBloomCondition(
  positions: number[],
  filterSize?: number,
): string {
  const conditions: string[] = [];

  for (const pos of positions) {
    if (filterSize !== undefined) {
      // Fixed size - compute byte and bit directly
      const foldedPos = pos % filterSize;
      const byteIdx = Math.floor(foldedPos / 8);
      const bitIdx = foldedPos % 8;
      const mask = 1 << bitIdx;
      conditions.push(`(get_byte(bloom_filter, ${byteIdx}) & ${mask}) != 0`);
    } else {
      // Dynamic size - use SQL to compute with folding
      conditions.push(
        `(get_byte(bloom_filter, ((${pos} % (length(bloom_filter)*8)) / 8)) ` +
          `& (1 << ((${pos} % (length(bloom_filter)*8)) % 8))) != 0`,
      );
    }
  }

  return conditions.join(" AND ");
}

/**
 * Build SQL condition for checking blocked bloom filter bits.
 *
 * @param blockIdx - The block index within the blocked bloom filter
 * @param positions - List of bit positions within the block
 * @returns SQL condition string for cp_bloom_blocks table
 */
export function buildBlockedBloomCondition(
  blockIdx: number,
  positions: number[],
): string {
  const bitConditions: string[] = [];

  for (const pos of positions) {
    const byteIdx = Math.floor(pos / 8);
    const bitMask = 1 << (pos % 8);
    bitConditions.push(
      `(get_byte(bloom_filter, ${byteIdx}) & ${bitMask}) != 0`,
    );
  }

  return `bloom_block_index = ${blockIdx} AND ${bitConditions.join(" AND ")}`;
}

/**
 * Calculate theoretical false positive rate.
 *
 * FPR = density ^ num_hashes (probability all bits are set by chance)
 *
 * @param density - Fraction of bits set in the filter (0.0 to 1.0)
 * @param numHashes - Number of hash functions
 * @returns False positive rate (0.0 to 1.0)
 */
export function theoreticalFpr(density: number, numHashes: number): number {
  return Math.pow(density, numHashes);
}

/**
 * Calculate overfetch factor given FPR.
 *
 * To get N true results, need to fetch N * overfetch_factor bloom matches.
 * overfetch_factor = 1 / (1 - FPR)
 *
 * @param fpr - False positive rate (0.0 to 1.0)
 * @returns Overfetch factor (1.0 or higher, infinity if FPR >= 1.0)
 */
export function overfetchFactor(fpr: number): number {
  if (fpr >= 1.0) {
    return Infinity;
  }
  return 1.0 / (1.0 - fpr);
}
