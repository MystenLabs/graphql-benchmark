Evaluates whether benchmark replays are similar in performance to the original run as documented in json files.

Does not exhaustively probe all possible combinations for possible graphql requests; instead, it focuses on a few filter combinations for each top-level query that are representative of the overall performance of the graphql service.


Compiled from local graphql service built from commit `e267657909cf3334b8001b8e82c15cd60c184b6f` of `releases/sui-graphql-rpc-v2024.4.0-release`

Pointed to a db backfilled by indexer v1.25

cargo run --bin sui-graphql-rpc start-server --db-url "db-url" --config mainnet-graphql.toml
