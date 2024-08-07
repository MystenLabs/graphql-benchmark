# Quick Start
`brew install pnpm`

`pnpm install`

`pyenv local 3.11`

`pip3 install -r requirements.txt`

`pnpm ts-node cli.ts --suite transaction-block --params-file-path transaction-block/parameters.json --url your-url`

`python3 analysis.py --file experiments/nameOfOutputFile.json`

Recommend setting `\pset pager off` in your psql session to show results of `explain [analyze]` at once.

To run the Python scripts that interact with postgres, you will need to install libpq and the psycopg2 library.

To replay a benchmark suite, add `--replay` as a flag. To run a benchmark experiment from manually picked variables, pass `--manual` as a flag.
```
pnpm ts-node cli.ts --suite transaction-block --params-file-path experiments/queryTransactionBlocks-manual-2024-05-14T19:21:26.278Z.json --url https://sui-mainnet.mystenlabs.com/ --limit 10 --replay

pnpm ts-node cli.ts --suite transaction-block --params-file-path manual.json --url https://sui-mainnet.mystenlabs.com/ --limit 10 --manual
```

# Defaults

- each query is run 10 times forwards, 10 times backwards. The first 3 runs of each set are considered warmup runs, and not included in `metrics` statistics.
- filter combinations are generated from the input json file. All combinations are run by default.
  - special "type" filters like `0x2::coin::CoinMetadata<0x2::sui::SUI>` will yield up to 4 variants, by `package`, `p::m`, `p::m::t`, and the fully-qualified type
- each parameter combination is run sequentially
- set up a local graphql service connected to some replica. I set the default request timeout to 5 seconds
  - general tolerance is 4 seconds for a website to load

# Adding more to the benchmark

1. Create new dir, such as [transaction-block](transaction-block/) to represent a new collection to test.
2. Add new queries under the dir in a new file, such as [queries.ts](transaction-block/queries.ts).
3. Add a `parameters.json` that specifies combinations to benchmark.
4. Set the defaults in [config.ts](config.ts)

# Graphql setup

- Modify `pg.rs` with the following to print db queries

```rust
    fn result<Q, U>(&mut self, query: impl Fn() -> Q) -> QueryResult<U>
    where
        Q: diesel::query_builder::Query,
        Q: LoadQuery<'static, Self::Connection, U>,
        Q: QueryId + QueryFragment<Self::Backend>,
    {
        query_cost::log(self.conn, self.max_cost, query());
        let binding = query();
        let debugged = debug_query(&binding);
        println!("Query: {}", debugged);
        query().get_result(self.conn)
    }
```

- Note that the checkpoint watermark task is very noisy, I like to set it to return a static result instead of constantly fetching from db.

# Python scripts

- `analysis.py` does the initial clustering to facilitate reviewing results
- `runnable_query.py` - converts the `debug_query` display printed by graphql for each query into something you can copy and paste into psql
- `utils.py` convert base58 to hex - useful for converting `transaction_digest` to something that can be run against `transactions` table
- `review.py` to merge multiple runs, select combinations that timed out, repro arbitrary runs

# Performance Parity Checks (WIP)

`./run_performance_parity.sh` will kick off the benchmarks stored in `evaluation/parity/benchmarks` and write to `experiments/evaluation/parity/benchmarks/uniqueTimestamp/suite/jsonFile.json`

Within the top-level `evaluation/parity/benchmarks`, we have dirs representing suites for `balance`, `coin`, and so on. The script will create a corresponding dir in `experiments/evaluation/parity/benchmarks/timestamp`, where the files nested in that path will mirror the structure of the original suite.
