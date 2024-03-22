// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
import { graphql } from '@mysten/sui.js/graphql/schemas/2024-01';

export const ObjectsQuery = graphql(`
query queryObjects($filter: ObjectFilter, $before: String, $after: String, $first: Int, $last: Int, $showContents: Boolean = false, $showParent: Boolean = false) {
    objects(
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
        address
        asMoveObject @include(if: $showContents) {
          contents {
            json
            type {
              repr
            }
          }
        }
        owner @include(if: $showParent) {
          ... on Parent {
            parent {
              address
            }
          }
          ... on AddressOwner {
            owner {
              address
            }
          }
        }
      }
    }
  }
`);

export const queries = { queryObjects: ObjectsQuery };
