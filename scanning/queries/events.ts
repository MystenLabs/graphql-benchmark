// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
import { GraphQLDocument } from "@mysten/graphql-transport";
import { parse } from "graphql";

// Raw GraphQL query for scanning events from sui-indexer-alt-graphql
export const ScanEventsQuery: GraphQLDocument = parse(`
  query scanEvents(
    $filter: EventFilter
    $before: String
    $after: String
    $first: Int
    $last: Int
  ) {
    scanEvents(
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
      edges {
        cursor
        node {
          sequenceNumber
          sender {
            address
          }
          contents {
            type {
              repr
            }
          }
          timestamp
        }
      }
    }
  }
`);

export const queries = {
  scanEvents: ScanEventsQuery,
};
