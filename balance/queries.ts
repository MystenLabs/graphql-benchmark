// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { graphql } from "@mysten/sui.js/graphql/schemas/2024-01";

export const BalanceQuery = graphql(`
  query queryBalance(
    $address: SuiAddress!
    $coinType: String
  ) {
    address(address: $address){
      suiBalance: balance {
        totalBalance
      }
      balance(type: $coinType){
        totalBalance
      }
    }
  }
`);

export const queries = { queryBalance: BalanceQuery };
