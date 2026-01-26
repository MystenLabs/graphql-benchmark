/**
 * DoubleHasher implementation matching Rust's DoubleHasher in hash.rs.
 *
 * Uses a single SipHash call to derive h1, then derives h2 from the upper
 * bits of h1 multiplied by a constant. Each subsequent hash is computed
 * by adding h2 to h1 and rotating.
 */

import { siphash13, parseHex } from "./siphash";
import { H2_MULTIPLIER } from "./constants";

const MASK64 = 0xffffffffffffffffn;

/**
 * 64-bit rotate left.
 */
function rotl64(x: bigint, b: number): bigint {
  return ((x << BigInt(b)) | (x >> BigInt(64 - b))) & MASK64;
}

/**
 * Double hashing implementation matching Rust's DoubleHasher.
 */
export class DoubleHasher {
  private h1: bigint;
  private h2: bigint;

  /**
   * Initialize with the first hash value.
   */
  constructor(h1: bigint) {
    this.h1 = h1;
    this.h2 = ((h1 >> 32n) * H2_MULTIPLIER) & MASK64;
  }

  /**
   * Create a DoubleHasher from key bytes and seed.
   * Matches Rust's DoubleHasher::with_value().
   *
   * @param keyBytes - The key to hash
   * @param seed - The seed (up to 128 bits)
   */
  static withValue(keyBytes: Uint8Array, seed: bigint): DoubleHasher {
    const k0 = seed & MASK64;
    const k1 = (seed >> 64n) & MASK64;
    const h1 = siphash13(keyBytes, k0, k1);
    return new DoubleHasher(h1);
  }

  /**
   * Generate the next hash value.
   * Matches Rust's DoubleHasher::next_hash().
   */
  nextHash(): bigint {
    this.h1 = (this.h1 + this.h2) & MASK64;
    this.h1 = rotl64(this.h1, 5); // rotate_left(5)
    return this.h1;
  }
}

/**
 * Compute bit positions for a key in a bloom filter.
 *
 * @param keyBytes - The key to hash
 * @param numBits - Total bits in the bloom filter
 * @param numHashes - Number of hash positions to generate
 * @param seed - Seed for the hash function
 * @returns List of bit positions
 */
export function computeHashPositions(
  keyBytes: Uint8Array,
  numBits: number,
  numHashes: number,
  seed: bigint,
): number[] {
  const hasher = DoubleHasher.withValue(keyBytes, seed);
  const positions: number[] = [];
  for (let i = 0; i < numHashes; i++) {
    const h = hasher.nextHash();
    positions.push(Number(h % BigInt(numBits)));
  }
  return positions;
}

/**
 * Convenience function to compute positions from hex string.
 *
 * @param keyHex - Hex string of the key (with or without 0x prefix)
 * @param numBits - Total bits in the bloom filter
 * @param numHashes - Number of hash positions to generate
 * @param seed - Seed for the hash function
 * @returns List of bit positions
 */
export function computePositionsFromHex(
  keyHex: string,
  numBits: number,
  numHashes: number,
  seed: bigint,
): number[] {
  const keyBytes = parseHex(keyHex);
  return computeHashPositions(keyBytes, numBits, numHashes, seed);
}
