# Quick Start

`pnpm install`
`pnpm ts-node cli.ts --suite transaction-block --params-file-path transaction-block/parameters.json`

# Adding more to the benchmark

1. Create new dir, such as [transaction-block](transaction-block/) to represent a new collection to test.
2. Add new queries under the dir in a new file, such as [queries.ts](transaction-block/queries.ts).
3. Add a `parameters.json` that specifies combinations to benchmark.
4. Set the defaults in [config.ts](config.ts)

# Walkthrough

- set up a local graphql service connected to some replica. I set the default request timeout to 5 seconds
  - general tolerance is 4 seconds for a website to load
- each query is run 10 times forwards, 10 times backwards. The first 3 runs of each set are considered warmup runs, and not included in `metrics` statistics.
- filter combinations are generated from the input json file. All combinations are run by default.
  - special "type" filters like `0x2::coin::CoinMetadata<0x2::sui::SUI>` will yield up to 4 variants, by `package`, `p::m`, `p::m::t`, and the fully-qualified type
- each parameter combination is run sequentially

There are some utility python files such as `review.py` to merge multiple runs, select combinations that timed out, or repro arbitrary runs. `utils.py` has some tools such as converting from base58 to hex, which can then be directly queried against db (such as `transaction_digest` on the `transactions` table.)
