// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
import { GraphQLDocument } from "@mysten/graphql-transport";
import { parse } from "graphql";

// Raw GraphQL query for transactions from sui-indexer-alt-graphql
export const TransactionsQuery: GraphQLDocument = parse(`
  query queryTransactions(
    $filter: TransactionFilter
    $before: String
    $after: String
    $first: Int
    $last: Int
  ) {
    scanTransactions(
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
        sender {
          address
        }
        gasInput {
          gasSponsor {
            address
          }
          gasPrice
          gasBudget
        }
        effects {
          status
          timestamp
          checkpoint {
            sequenceNumber
          }
          epoch {
            epochId
          }
        }
      }
    }
  }
`);

export const queries = {
  queryTransactions: TransactionsQuery,
};
