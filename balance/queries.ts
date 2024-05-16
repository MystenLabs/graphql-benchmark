// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { graphql } from "@mysten/sui.js/graphql/schemas/2024-01";

export const AddressBalanceByType = graphql(`
query queryAddressBalance($address: SuiAddress!, $type: String) {
    address(address: $address) {
      balance(type: $type) {
        coinType { repr }
        coinObjectCount
        totalBalance
      }
    }
  }
`);

export const AddressBalances = graphql(`
query queryAddressBalances($address: SuiAddress!, $first: Int, $after: String, $last: Int, $before: String) {
    address(address: $address) {
      balances(first: $first, after: $after, last: $last, before: $before) {
        pageInfo {
          startCursor
          endCursor
          hasPreviousPage
          hasNextPage
        }
        nodes {
          coinType {
            repr
          }
          coinObjectCount
          totalBalance
        }
      }
    }
  }
`);

export const queries = { addressBalanceByType: AddressBalanceByType, addressBalances: AddressBalances };
