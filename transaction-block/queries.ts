// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { graphql } from "@mysten/sui.js/graphql/schemas/2024-01";

export const TransactionBlocksQuery = graphql(`
  query queryTxs(
    $filter: TransactionBlockFilter
    $before: String
    $after: String
    $first: Int
    $last: Int
  ) {
    transactionBlocksCalls(
      filter: $filter
      first: $first
      after: $after
      last: $last
      before: $before
    ) {
      pageInfo {
        hasNextPage
        hasPreviousPage
        endCursor
        startCursor
      }
      nodes {
        digest
      }
    }
  }
`);

export const queries = { queryTransactionBlocks: TransactionBlocksQuery };
