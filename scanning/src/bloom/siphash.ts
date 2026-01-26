/**
 * SipHash-1-3 implementation matching Rust's siphasher crate.
 *
 * Uses 1 compression round per block and 3 finalization rounds.
 */

const MASK64 = 0xffffffffffffffffn;

/**
 * 64-bit rotate left.
 */
function rotl64(x: bigint, b: number): bigint {
  return ((x << BigInt(b)) | (x >> BigInt(64 - b))) & MASK64;
}

/**
 * SipHash-1-3 implementation.
 *
 * @param keyBytes - The key to hash (Uint8Array)
 * @param k0 - First 64 bits of the seed
 * @param k1 - Second 64 bits of the seed
 * @returns 64-bit hash value
 */
export function siphash13(keyBytes: Uint8Array, k0: bigint, k1: bigint): bigint {
  // Initialize state
  let v0 = k0 ^ 0x736f6d6570736575n;
  let v1 = k1 ^ 0x646f72616e646f6dn;
  let v2 = k0 ^ 0x6c7967656e657261n;
  let v3 = k1 ^ 0x7465646279746573n;

  function sipround(): void {
    v0 = (v0 + v1) & MASK64;
    v1 = rotl64(v1, 13);
    v1 ^= v0;
    v0 = rotl64(v0, 32);
    v2 = (v2 + v3) & MASK64;
    v3 = rotl64(v3, 16);
    v3 ^= v2;
    v0 = (v0 + v3) & MASK64;
    v3 = rotl64(v3, 21);
    v3 ^= v0;
    v2 = (v2 + v1) & MASK64;
    v1 = rotl64(v1, 17);
    v1 ^= v2;
    v2 = rotl64(v2, 32);
  }

  // Process full 8-byte blocks
  const numBlocks = Math.floor(keyBytes.length / 8);
  for (let i = 0; i < numBlocks; i++) {
    // Read 8 bytes as little-endian u64
    let m = 0n;
    for (let j = 0; j < 8; j++) {
      m |= BigInt(keyBytes[i * 8 + j]) << BigInt(8 * j);
    }
    v3 ^= m;
    sipround(); // 1 round for SipHash-1-3
    v0 ^= m;
  }

  // Process remaining bytes with length byte
  const remaining = keyBytes.subarray(numBlocks * 8);
  let b = BigInt(keyBytes.length % 256) << 56n;
  for (let i = 0; i < remaining.length; i++) {
    b |= BigInt(remaining[i]) << BigInt(8 * i);
  }

  v3 ^= b;
  sipround(); // 1 round
  v0 ^= b;

  // Finalize
  v2 ^= 0xffn;
  for (let i = 0; i < 3; i++) {
    // 3 rounds for SipHash-1-3
    sipround();
  }

  return v0 ^ v1 ^ v2 ^ v3;
}

/**
 * Parse hex string to Uint8Array.
 */
export function parseHex(hexStr: string): Uint8Array {
  const clean = hexStr.startsWith("0x") ? hexStr.slice(2) : hexStr;
  const bytes = new Uint8Array(clean.length / 2);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(clean.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}
