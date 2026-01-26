/**
 * BloomValue type system matching the Rust BloomValue enum.
 *
 * Each variant corresponds to a distinct dimension that can be inserted into
 * or probed against a checkpoint bloom filter. Tagged variants use a single-byte
 * prefix to prevent cross-dimension collisions.
 *
 * Reference: sui/crates/sui-indexer-alt-schema/src/blooms/mod.rs
 */

import { parseHex } from "./siphash";

// ── High-frequency addresses excluded from bloom filters ─────────────
// These appear in most checkpoints, causing queries to match nearly all blocks.
// Matches BLOOM_SKIP_ADDRESSES in sui-indexer-alt-schema/src/blooms/mod.rs.

const ZERO_ADDRESS =
  "0x0000000000000000000000000000000000000000000000000000000000000000";
const SUI_CLOCK_ADDRESS =
  "0x0000000000000000000000000000000000000000000000000000000000000006";
const BLOOM_SKIP_ADDRESSES = [ZERO_ADDRESS, SUI_CLOCK_ADDRESS];

/**
 * Returns true if this bloom value should be excluded from bloom filter operations
 * because it matches a high-frequency address (0x0 or SUI_CLOCK_ADDRESS).
 */
function shouldSkipBloomValue(value: BloomValue): boolean {
  let address: string | null = null;
  switch (value.kind) {
    case "SenderOrRecipient":
    case "AffectedObject":
    case "MoveCallPackage":
    case "EvAddress":
      address = value.address;
      break;
    default:
      return false;
  }
  // Normalize to full-length 0x-prefixed for comparison
  const clean = address.startsWith("0x") ? address : "0x" + address;
  const padded = "0x" + clean.slice(2).padStart(64, "0");
  return BLOOM_SKIP_ADDRESSES.includes(padded);
}

// ── Tag bytes matching Rust BloomTag enum ────────────────────────────

const TAG_MOVE_CALL_PACKAGE = 0x50; // 'P'
const TAG_MOVE_CALL_MODULE = 0x4d; // 'M'
const TAG_EV_EMIT_MODULE = 0x45; // 'E'
const TAG_EV_ADDRESS = 0x41; // 'A'
const TAG_AFFECTED_OBJECT = 0x4f; // 'O'
const TAG_EV_TYPE_MODULE = 0x54; // 'T'

// ── BloomValue type ──────────────────────────────────────────────────

export type BloomValue =
  | { kind: "SenderOrRecipient"; address: string }
  | { kind: "AffectedObject"; address: string }
  | { kind: "MoveCallPackage"; address: string }
  | { kind: "MoveCallModule"; module: string }
  | { kind: "EvAddress"; address: string }
  | { kind: "EvEmitModule"; module: string }
  | { kind: "EvTypeModule"; module: string }
  | { kind: "Name"; name: string }
  | { kind: "TypeParam"; param: string };

/**
 * Convert a BloomValue to its byte representation for hashing.
 * Matches the Rust BloomValue::to_bytes() implementation.
 */
export function bloomValueToBytes(value: BloomValue): Uint8Array {
  switch (value.kind) {
    case "SenderOrRecipient":
      return parseHex(value.address);

    case "AffectedObject":
      return prefixTag(TAG_AFFECTED_OBJECT, parseHex(value.address));

    case "MoveCallPackage":
      return prefixTag(TAG_MOVE_CALL_PACKAGE, parseHex(value.address));

    case "MoveCallModule":
      return prefixTag(TAG_MOVE_CALL_MODULE, encodeUtf8(value.module));

    case "EvAddress":
      return prefixTag(TAG_EV_ADDRESS, parseHex(value.address));

    case "EvEmitModule":
      return prefixTag(TAG_EV_EMIT_MODULE, encodeUtf8(value.module));

    case "EvTypeModule":
      return prefixTag(TAG_EV_TYPE_MODULE, encodeUtf8(value.module));

    case "Name":
      return encodeUtf8(value.name);

    case "TypeParam":
      return encodeUtf8(value.param);
  }
}

// ── Filter-to-BloomValue mapping ─────────────────────────────────────

export type ScanTarget = "transactions" | "events";

/**
 * Convert a GraphQL filter field + value into the bloom values that should
 * be probed. Returns multiple values for compound keys (ANDed in the bloom).
 *
 * TransactionFilter fields: function, affectedObject, affectedAddress, sentAddress
 * EventFilter fields: sender, module, type
 */
export function filterFieldToBloomValues(
  scan: ScanTarget,
  field: string,
  value: string,
): BloomValue[] {
  const raw =
    scan === "transactions"
      ? txFilterFieldToBloomValues(field, value)
      : eventFilterFieldToBloomValues(field, value);

  // Filter out high-frequency addresses, matching Rust bloom_probe_values()
  return raw.filter((v) => !shouldSkipBloomValue(v));
}

/**
 * Parse transaction filter fields into bloom values.
 *
 * function: "0xpkg" -> MoveCallPackage(pkg)
 * function: "0xpkg::mod" -> MoveCallPackage(pkg) + MoveCallModule(mod)
 * function: "0xpkg::mod::name" -> MoveCallPackage(pkg) + MoveCallModule(mod) + Name(name)
 */
function txFilterFieldToBloomValues(
  field: string,
  value: string,
): BloomValue[] {
  switch (field) {
    case "function": {
      return parseFunctionFilter(value);
    }
    case "affectedObject":
      return [{ kind: "AffectedObject", address: value }];
    case "affectedAddress":
      return [{ kind: "SenderOrRecipient", address: value }];
    case "sentAddress":
      return [{ kind: "SenderOrRecipient", address: value }];
    default:
      throw new Error(
        `Unknown TransactionFilter field: ${field}. ` +
          `Valid fields: function, affectedObject, affectedAddress, sentAddress`,
      );
  }
}

/**
 * Parse a function filter string: "0xpkg", "0xpkg::module", or "0xpkg::module::function"
 */
function parseFunctionFilter(value: string): BloomValue[] {
  const parts = splitModulePath(value);
  const values: BloomValue[] = [
    { kind: "MoveCallPackage", address: parts.package },
  ];
  if (parts.module) {
    values.push({ kind: "MoveCallModule", module: parts.module });
  }
  if (parts.name) {
    values.push({ kind: "Name", name: parts.name });
  }
  return values;
}

/**
 * Parse event filter values into bloom values.
 *
 * module: "0xpkg" -> EvAddress(pkg)
 * module: "0xpkg::mod" -> EvAddress(pkg) + EvEmitModule(mod)
 *
 * type: "0xpkg" -> EvAddress(pkg)
 * type: "0xpkg::mod" -> EvAddress(pkg) + EvTypeModule(mod)
 * type: "0xpkg::mod::Name" -> EvAddress(pkg) + EvTypeModule(mod) + Name(name)
 * type: "0xpkg::mod::Name<params>" -> above + TypeParam for each param
 */
function eventFilterFieldToBloomValues(
  field: string,
  value: string,
): BloomValue[] {
  switch (field) {
    case "sender":
      return [{ kind: "SenderOrRecipient", address: value }];

    case "module": {
      return parseModuleFilter(value);
    }

    case "type": {
      return parseTypeFilter(value);
    }

    default:
      throw new Error(
        `Unknown EventFilter field: ${field}. ` +
          `Valid fields: sender, module, type`,
      );
  }
}

/**
 * Parse a module filter string: "0xpkg" or "0xpkg::module_name"
 */
function parseModuleFilter(value: string): BloomValue[] {
  const parts = splitModulePath(value);
  const values: BloomValue[] = [{ kind: "EvAddress", address: parts.package }];
  if (parts.module) {
    values.push({ kind: "EvEmitModule", module: parts.module });
  }
  return values;
}

/**
 * Parse a type filter string: "0xpkg", "0xpkg::mod", "0xpkg::mod::Name",
 * or "0xpkg::mod::Name<T1, T2>"
 */
function parseTypeFilter(value: string): BloomValue[] {
  // Strip type params for initial parsing
  const angleIdx = value.indexOf("<");
  const basePart = angleIdx >= 0 ? value.slice(0, angleIdx) : value;
  const typeParams =
    angleIdx >= 0 ? parseTypeParams(value.slice(angleIdx)) : [];

  const parts = splitModulePath(basePart);
  const values: BloomValue[] = [{ kind: "EvAddress", address: parts.package }];

  if (parts.module) {
    values.push({ kind: "EvTypeModule", module: parts.module });
  }
  if (parts.name) {
    values.push({ kind: "Name", name: parts.name });
  }
  for (const tp of typeParams) {
    values.push({ kind: "TypeParam", param: tp });
  }

  return values;
}

/**
 * Split "0xpkg::module::Name" into components.
 */
function splitModulePath(value: string): {
  package: string;
  module?: string;
  name?: string;
} {
  const idx = value.indexOf("::");
  if (idx < 0) {
    return { package: value };
  }
  const pkg = value.slice(0, idx);
  const rest = value.slice(idx + 2);

  const idx2 = rest.indexOf("::");
  if (idx2 < 0) {
    return { package: pkg, module: rest };
  }

  return {
    package: pkg,
    module: rest.slice(0, idx2),
    name: rest.slice(idx2 + 2),
  };
}

/**
 * Parse type parameters from a string like "<T1, T2>" into individual canonical strings.
 * Handles nested generics by tracking angle bracket depth.
 */
function parseTypeParams(paramStr: string): string[] {
  if (!paramStr.startsWith("<") || !paramStr.endsWith(">")) return [];
  const inner = paramStr.slice(1, -1);
  const params: string[] = [];
  let depth = 0;
  let start = 0;

  for (let i = 0; i < inner.length; i++) {
    if (inner[i] === "<") depth++;
    else if (inner[i] === ">") depth--;
    else if (inner[i] === "," && depth === 0) {
      params.push(inner.slice(start, i).trim());
      start = i + 1;
    }
  }

  const last = inner.slice(start).trim();
  if (last) params.push(last);
  return params;
}

// ── Helpers ──────────────────────────────────────────────────────────

function prefixTag(tag: number, data: Uint8Array): Uint8Array {
  const out = new Uint8Array(1 + data.length);
  out[0] = tag;
  out.set(data, 1);
  return out;
}

function encodeUtf8(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}
