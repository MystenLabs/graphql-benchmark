# Benchmark setup

`pnpm install`
`pnpm ts-node cli.ts --suite transaction-block --params-file-path transaction-block/parameters.json`

# Adding more to the benchmark

1. Create new dir, such as [transaction-block](transaction-block/) to represent a new collection to test.
2. Add new queries under the dir in a new file, such as [queries.ts](transaction-block/queries.ts).
3. Add a `parameters.json` that specifies combinations to benchmark.
4. Set the defaults in [config.ts](config.ts)
