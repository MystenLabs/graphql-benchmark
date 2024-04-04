Broadly, this initial benchmarking exercise will be to evaluate whether the existing graphql queries can consistently and deterministically handle arbitrary combinations of filters.
1. Use the benchmark suite with some initial set of filters to produce a set of timed-out queries to investigate.
2. Iterate and create new experiments as needed. For example, the transaction blocks query suite started off with values for each filter, observed a particular behavior when it came to handling filters against `tx_calls`, and then reran a smaller subset on just `signAddress | recvAddress | inputObject | changedObject` to determine whether the behavior was consistent across all filters. Since graphql queries for a type all have the same basis and are conditionally filtered, similar behavior should eventually arise.
3. Determine whether the full set of filters can be handled with some adjustment to the db schema - adding new columns and indexes, etc.
4. If not, settle on a subset of filters that can be handled.
5. Conduct additional experiments from steps 3 or 4 until we arrive at a set of parameters that do not time out.
