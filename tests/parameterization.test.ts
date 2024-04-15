// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { generateCombinations } from "../parameterization";

test("generateCombinations generates expected number of power sets", () => {
  const parameters = {
    filter: {
      function: [
        "0x8d97f1cd6ac663735be08d1d2b6d02a159e711586461306ce60a2b7a6a565a9e::pyth",
        "0x000000000000000000000000000000000000000000000000000000000000dee9::clob_v2",
      ],
    },
    first: [10, 20, 30],
    last: [10, 20, 30],
  };

  let combinations = generateCombinations(parameters, ["function"]);
  expect(combinations.length).toBe(45);
});
