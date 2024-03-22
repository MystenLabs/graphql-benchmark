# Adding a new suite of tests

1. create new dir, such as `/events`
2. create new queries in `queries.ts`
3. `parameterize.ts` and accompanying `parameters.json` file to set parameters -> filters
4. `events.ts` to define how the client should query and how to return `PageInfo` for benchmark to paginate through
5. `eventsSuite` to be able to read parameters and run tests

The idea is to have sets of json files that define the parameters for tests.
For example when testing events - `eventType` that have no overlap with `emittingModule`, don't want to muddle this with separate sets of tests that check whether overlapping `eventType` and `emittingModule` perform well.


in `graphql-benchmark` you would run `npx ts-node events/events.ts parameters.json`


TODO: can we further generalize filter to all variables? I think so...

# Building and running
`pnpm install`
`pnpm ts-node events/events.ts sender.json`


# Reproducing

## curl
```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{
        "query": "query queryEvents($filter: EventFilter, $before: String, $after: String, $first: Int, $last: Int, $showSendingModule: Boolean = false, $showContents: Boolean = false) { events(filter: $filter, first: $first, after: $after, last: $last, before: $before) { pageInfo { hasNextPage hasPreviousPage endCursor startCursor } nodes { sendingModule @include(if: $showSendingModule) { package { address } name } sender { address } type { repr } json @include(if: $showContents) } } }",
        "variables": {
          "filter": {
            "sender": "0x02a212de6a9dfa3a69e22387acfbafbb1a9e591bd9d636e7895dcfc8de05f331"
          },
          "last": 50
        }
      }' \
  http://127.0.0.1:8000/graphql
```


TODO: use pnpm and add it as a dev dependency
