/**
 * Tests for SipHash-1-3 and bloom filter position computation.
 *
 * These test vectors should match the Rust implementation in
 * sui-indexer-alt-schema/src/blooms/hash.rs
 */

import { siphash13, parseHex } from "./siphash";
import { computePositions, computeBlockedPositions } from "./positions";
import { BLOOM_SEED, CP_BLOOM_NUM_BITS, CP_BLOOM_NUM_HASHES } from "./constants";

describe("SipHash-1-3", () => {
  it("should parse hex strings correctly", () => {
    const bytes = parseHex("0xdee9");
    expect(bytes).toEqual(new Uint8Array([0xde, 0xe9]));

    const bytes2 = parseHex("dee9");
    expect(bytes2).toEqual(new Uint8Array([0xde, 0xe9]));
  });

  it("should compute siphash for empty input", () => {
    // Empty input with zero seed
    const hash = siphash13(new Uint8Array([]), 0n, 0n);
    // This should produce a deterministic value
    expect(typeof hash).toBe("bigint");
    expect(hash >= 0n).toBe(true);
  });

  it("should compute siphash for known input", () => {
    // Test with a known address-like input
    const keyHex = "0x000000000000000000000000000000000000000000000000000000000000dee9";
    const keyBytes = parseHex(keyHex);
    const hash = siphash13(keyBytes, BLOOM_SEED, 0n);

    // The hash should be a valid 64-bit value
    expect(hash >= 0n).toBe(true);
    expect(hash < (1n << 64n)).toBe(true);
  });
});

describe("Bloom filter positions", () => {
  it("should compute consistent positions for same input", () => {
    const key = "0x000000000000000000000000000000000000000000000000000000000000dee9";

    const pos1 = computePositions(key);
    const pos2 = computePositions(key);

    expect(pos1).toEqual(pos2);
    expect(pos1.length).toBe(CP_BLOOM_NUM_HASHES);
  });

  it("should compute positions within valid range", () => {
    const key = "0x02a212de6a9dfa3a69e22387acfbafbb1a9e591bd9d636e7895dcfc8de05f331";

    const positions = computePositions(key);

    expect(positions.length).toBe(CP_BLOOM_NUM_HASHES);
    for (const pos of positions) {
      expect(pos >= 0).toBe(true);
      expect(pos < CP_BLOOM_NUM_BITS).toBe(true);
    }
  });

  it("should compute different positions for different keys", () => {
    const key1 = "0x000000000000000000000000000000000000000000000000000000000000dee9";
    const key2 = "0x0000000000000000000000000000000000000000000000000000000000000002";

    const pos1 = computePositions(key1);
    const pos2 = computePositions(key2);

    // Positions should be different for different keys
    // (highly likely, not guaranteed)
    const set1 = new Set(pos1);
    const set2 = new Set(pos2);
    const intersection = [...set1].filter((x) => set2.has(x));
    expect(intersection.length).toBeLessThan(pos1.length);
  });
});

describe("Blocked bloom filter positions", () => {
  it("should compute block index and positions", () => {
    const key = "0x000000000000000000000000000000000000000000000000000000000000dee9";
    const cpBlockId = 1000;

    const [blockIdx, positions] = computeBlockedPositions(key, cpBlockId);

    // Block index should be in valid range (0-127)
    expect(blockIdx >= 0).toBe(true);
    expect(blockIdx < 128).toBe(true);

    // Should have 5 positions (BLOCKED_BLOOM_NUM_HASHES)
    expect(positions.length).toBe(5);

    // Positions should be in valid range (0-16383)
    for (const pos of positions) {
      expect(pos >= 0).toBe(true);
      expect(pos < 16384).toBe(true);
    }
  });

  it("should compute consistent blocked positions", () => {
    const key = "0x02a212de6a9dfa3a69e22387acfbafbb1a9e591bd9d636e7895dcfc8de05f331";
    const cpBlockId = 12345;

    const [blockIdx1, pos1] = computeBlockedPositions(key, cpBlockId);
    const [blockIdx2, pos2] = computeBlockedPositions(key, cpBlockId);

    expect(blockIdx1).toBe(blockIdx2);
    expect(pos1).toEqual(pos2);
  });

  it("should compute different positions for different block IDs", () => {
    const key = "0x000000000000000000000000000000000000000000000000000000000000dee9";

    const [blockIdx1, pos1] = computeBlockedPositions(key, 1000);
    const [blockIdx2, pos2] = computeBlockedPositions(key, 1001);

    // Different block IDs should likely produce different results
    // (not guaranteed but highly probable)
    const different =
      blockIdx1 !== blockIdx2 ||
      pos1.some((p, i) => p !== pos2[i]);
    expect(different).toBe(true);
  });
});
