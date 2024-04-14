// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { graphql } from "@mysten/sui.js/graphql/schemas/2024-01";

export const DFsByOwner = graphql(`
  query DFsByOwner(
    $first: Int
    $after: String
    $last: Int
    $before: String
    $address: String
    $showContents: Boolean = false
  ) {
    owner(address: $address) {
        dynamicFields(
            first: $first
            after: $after
            last: $last
            before: $before
        ) {
            pageInfo {
                startCursor
                endCursor
                hasPreviousPage
                hasNextPage
            }
            nodes @include(if: $showContents) {
                name {
                  type {
                    repr
                  }
                }
                value {
                    ... on MoveValue {
                        type {
                            repr
                        }
                        json
                    }
                    ... on MoveObject {
                        contents {
                            json
                            type {
                                repr
                            }
                        }
                    }
                }
            }
        }
    }
}`);

export const DFsByObject = graphql(`
query DFsByObject(
  $first: Int
  $after: String
  $last: Int
  $before: String
  $address: String
  $showContents: Boolean = false
) {
  object(address: $address) {
      dynamicFields(
          first: $first
          after: $after
          last: $last
          before: $before
      ) {
          pageInfo {
              startCursor
              endCursor
              hasPreviousPage
              hasNextPage
          }
          nodes @include(if: $showContents) {
              name {
                type {
                  repr
                }
              }
              value {
                  ... on MoveValue {
                      type {
                          repr
                      }
                      json
                  }
                  ... on MoveObject {
                      contents {
                          json
                          type {
                              repr
                          }
                      }
                  }
              }
          }
      }
  }
}`);

export const queries = { dfsByOwner: DFsByOwner, dfsByObject: DFsByObject };
