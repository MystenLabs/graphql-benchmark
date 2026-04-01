# Scanning CLI

Bloom filter analysis tool for the `scanTransactions` and `scanEvents` GraphQL APIs. Measures bloom filter saturation, false positive rates (FPR), and overfetch factors to help tune bloom filter parameters for production.

## Prerequisites

```bash
brew install pnpm
pnpm install

# Database access (indexer database with cp_blooms/cp_bloom_blocks table)
export DATABASE_URL="postgres://postgres:postgrespw@localhost:5432/sui_indexer_alt"
```

## CLI Structure

```
pnpm scan <target> <command> [options]

Targets:
  transactions   Analyze scanTransactions bloom filters
  events         Analyze scanEvents bloom filters
  saturation     Analyze bloom filter saturation (no target needed)
```

## Saturation Analysis

Reports bloom filter size distribution and bit saturation for both `cp_blooms` (per-checkpoint) and `cp_bloom_blocks` (1000-checkpoint blocks).

```bash
pnpm scan saturation --db $DATABASE_URL --cp-lo 12000000 --cp-hi 12050000
```

Output includes:

- Size distribution (how many filters at each byte size)
- Bit saturation percentiles (avg, min, P50, P90, max)
- Folding recommendations based on density thresholds
- Block bloom filter saturation and non-zero block distribution

## Parameter Discovery

Find high-frequency filter values from the database for testing.

```bash
# Discover top event emitting modules, types, and senders
pnpm scan events discover --db $DATABASE_URL --cp-lo 12000000 --cp-hi 12050000 --top 10

# Discover top transaction packages, senders, and objects
pnpm scan transactions discover --db $DATABASE_URL --cp-lo 12000000 --cp-hi 12050000

# Also find low-frequency items (useful for FPR measurement)
pnpm scan events discover --db $DATABASE_URL --low-freq --low-freq-min 100 --low-freq-max 1000
```

### Options

| Flag                | Default | Description                        |
| ------------------- | ------- | ---------------------------------- |
| `--top`             | 10      | Number of top items per category   |
| `--low-freq`        | false   | Also find moderate-frequency items |
| `--low-freq-min`    | 100     | Minimum count for low-freq items   |
| `--low-freq-max`    | 1000    | Maximum count for low-freq items   |
| `--json`            | false   | Output as JSON                     |
| `--generate-params` | false   | Generate parameters.json format    |
| `-o, --output`      | -       | Write results to directory         |

## FPR Analysis

Measure false positive rates for specific filters. The `--filter` flag takes a JSON object whose keys match the GraphQL filter field names.

### Single filter

```bash
# Event module filter
pnpm scan events analyze --db $DATABASE_URL \
  --filter '{"module": "0xa0eba1...::spot_dex"}' \
  --cp-lo 12000000 --cp-hi 12050000

# Event type filter (fully qualified)
pnpm scan events analyze --db $DATABASE_URL \
  --filter '{"type": "0xa0eba1...::spot_dex::SwapEvent"}'

# Event sender filter
pnpm scan events analyze --db $DATABASE_URL \
  --filter '{"sender": "0x02a212..."}'

# Transaction function filter (package, package::module, or package::module::function)
pnpm scan transactions analyze --db $DATABASE_URL \
  --filter '{"function": "0xa0eba1..."}'

# Transaction sentAddress filter
pnpm scan transactions analyze --db $DATABASE_URL \
  --filter '{"sentAddress": "0x02a212..."}'
```

### Combo filter (multiple fields ANDed)

Multiple fields in one JSON object are ANDed, matching GraphQL semantics:

```bash
# Events: module + sender (AND)
pnpm scan events analyze --db $DATABASE_URL \
  --filter '{"module": "0x00b53b...::pyth", "sender": "0x02a212..."}'
```

### Multiple filters in one run

Repeat `--filter` to test several filters in a single invocation:

```bash
pnpm scan events analyze --db $DATABASE_URL \
  --filter '{"module": "0xa0eba1...::spot_dex"}' \
  --filter '{"sender": "0x6fcce6..."}' \
  --filter '{"type": "0x91bfbc...::pool::SwapEvent"}'
```

### Filter field reference

**TransactionFilter** (for `pnpm scan transactions analyze`):

| Field             | Bloom values                                           | Format                                       |
| ----------------- | ------------------------------------------------------ | -------------------------------------------- |
| `function`        | `MoveCallPackage` + optional `MoveCallModule` + `Name` | `0xpkg`, `0xpkg::mod`, or `0xpkg::mod::func` |
| `affectedObject`  | `AffectedObject`                                       | `0x...` (hex address)                        |
| `affectedAddress` | `SenderOrRecipient`                                    | `0x...` (hex address)                        |
| `sentAddress`     | `SenderOrRecipient`                                    | `0x...` (hex address)                        |

**EventFilter** (for `pnpm scan events analyze`):

| Field    | Bloom values                                         | Format                                       |
| -------- | ---------------------------------------------------- | -------------------------------------------- |
| `sender` | `SenderOrRecipient`                                  | `0x...` (hex address)                        |
| `module` | `EventAddress` + optional `EventEmitModule`          | `0xpkg` or `0xpkg::mod`                      |
| `type`   | `EventAddress` + optional `EventTypeModule` + `Name` | `0xpkg`, `0xpkg::mod`, or `0xpkg::mod::Name` |

### Size distribution, density, and theoretical FPR

These modes don't require a `--filter` and are the same for both scan targets:

```bash
# Bloom filter size distribution
pnpm scan transactions analyze --db $DATABASE_URL --size-distribution

# Density distribution (for folding tuning)
pnpm scan transactions analyze --db $DATABASE_URL --density --sample 5000

# Theoretical FPR table by density
pnpm scan transactions analyze --db $DATABASE_URL --theoretical

# All three together
pnpm scan transactions analyze --db $DATABASE_URL --size-distribution --density --theoretical
```

### Analyze options

| Flag                  | Default | Description                                |
| --------------------- | ------- | ------------------------------------------ |
| `--filter`            | -       | JSON filter object (repeatable)            |
| `--cp-lo`             | 0       | Lower checkpoint bound                     |
| `--cp-hi`             | max     | Upper checkpoint bound                     |
| `--sample`            | 10000   | Max bloom-matching checkpoints to test     |
| `--page-size`         | 50      | Page size for overfetch calculation        |
| `--size-distribution` | false   | Show bloom filter size distribution        |
| `--density`           | false   | Show density distribution                  |
| `--theoretical`       | false   | Show theoretical FPR table                 |
| `--overfetch`         | false   | Auto-discover params and measure overfetch |
| `--blocked`           | false   | Analyze blocked bloom filters              |

## Output Interpretation

### FPR table columns

| Column      | Meaning                                                        |
| ----------- | -------------------------------------------------------------- |
| Bloom Match | Checkpoints where bloom filter bits matched                    |
| True Pos    | Checkpoints that actually contain matching transactions/events |
| False Pos   | Bloom matches that were false positives                        |
| FPR %       | `False Pos / Bloom Match * 100`                                |
| Overfetch   | `1 / (1 - FPR)` — multiply page size by this to compensate     |
| Fetch/N     | Bloom matches needed to get N true results                     |

### Combo filter columns

| Column   | Meaning                                                                      |
| -------- | ---------------------------------------------------------------------------- |
| Partial  | Bloom matched, some filters have real matches but not all (known pair issue) |
| PureFP   | Bloom matched, no filter has any real match (true bloom collision)           |
| EffFPR % | `PureFP / Bloom` — effective FPR from bloom collisions only                  |

### Healthy ranges

| Metric              | Target | Concern                                |
| ------------------- | ------ | -------------------------------------- |
| P90 saturation      | < 40%  | > 40% means filters should be larger   |
| FPR (single filter) | < 5%   | > 10% indicates poor bloom selectivity |
| Overfetch           | < 1.5x | > 2x makes scanning expensive          |

## Database Requirements

The CLI queries these tables:

| Table                   | Used by                       | Required for                       |
| ----------------------- | ----------------------------- | ---------------------------------- |
| `cp_blooms`             | saturation, analyze           | Bloom filter bit checking          |
| `cp_bloom_blocks`       | saturation                    | Block bloom analysis               |
| `cp_sequence_numbers`   | discover, analyze             | Checkpoint-to-tx mapping           |
| `tx_calls`              | transactions discover/analyze | Function filter verification       |
| `tx_affected_objects`   | transactions discover/analyze | Object filter verification         |
| `tx_affected_addresses` | transactions discover/analyze | Address/sender filter verification |
| `ev_emit_mod`           | events discover/analyze       | Module/sender filter verification  |
| `ev_struct_inst`        | events discover/analyze       | Type filter verification           |

If verification tables are empty, the tool will warn and FPR results will show 100% false positives. This means the indexer pipelines for those tables are not enabled, not that bloom filters are broken.

## Configuration

Settings can be configured in `benchmark.config.json`:

```json
{
  "connection": {
    "graphql_url": "http://127.0.0.1:7001/graphql",
    "database_url": null
  },
  "discovery": {
    "checkpoint_lo": null,
    "checkpoint_hi": null,
    "top_n": 10,
    "low_freq_min": 100,
    "low_freq_max": 1000
  }
}
```

The `--db` flag overrides `connection.database_url`. The `DATABASE_URL` environment variable is also checked.

## Bloom Filter Constants

| Setting                        | Value   | Description                                |
| ------------------------------ | ------- | ------------------------------------------ |
| `CP_BLOOM_NUM_BITS`            | 131,072 | 16KB per-checkpoint bloom (before folding) |
| `CP_BLOOM_NUM_HASHES`          | 6       | Hash functions per key                     |
| `MIN_FOLD_BITS`                | 8,192   | 1KB minimum after folding                  |
| `MAX_FOLD_DENSITY`             | 0.40    | Stop folding above 40% density             |
| `BLOCKED_BLOOM_NUM_BLOCKS`     | 128     | Blocks per blocked bloom                   |
| `BLOCKED_BLOOM_BITS_PER_BLOCK` | 16,384  | 2KB per block                              |
| `BLOCKED_BLOOM_NUM_HASHES`     | 5       | Hash functions for blocked blooms          |
