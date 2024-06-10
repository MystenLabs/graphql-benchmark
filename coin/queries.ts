// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { graphql } from "@mysten/sui.js/graphql/schemas/2024-01";

export const CoinsByType = graphql(`
  query CoinsByType(
    $first: Int
    $after: String
    $last: Int
    $before: String
    $type: String
  ) {
    coins(
      first: $first
      after: $after
      last: $last
      before: $before
      type: $type
    ) {
      pageInfo {
        startCursor
        endCursor
        hasPreviousPage
        hasNextPage
      }
      nodes {
        coinBalance
        address
        version
        digest
      }
    }
  }
`);

export const CoinsByOwner = graphql(`
  query CoinsByOwner(
    $first: Int
    $after: String
    $last: Int
    $before: String
    $address: SuiAddress!
    $type: String = "0x2::sui::SUI"
  ) {
    owner(address: $address) {
      coins(
        first: $first
        after: $after
        last: $last
        before: $before
        type: $type
      ) {
        pageInfo {
          startCursor
          endCursor
          hasPreviousPage
          hasNextPage
        }
        nodes {
          coinBalance
          address
          version
          digest
        }
      }
    }
  }
`);

export const queries = { coinsByType: CoinsByType, coinsByOwner: CoinsByOwner };
