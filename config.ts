// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/**
 * Maps suites to default configurations, to be overriden by the user.
 */
export async function getSuiteConfiguration(suiteName: string) {
    switch (suiteName) {
      case "transaction-block": {
        let { queries } = await import("./transaction-block/queries");
        return {
          description: "TxBlocks suite description",
          queries,
          queryKey: "queryTransactionBlocks",
          dataPath: "transactionBlocks.pageInfo",
          typeStringFields: ["function"],
          paramsFilePath: "./transaction-block/parameters.json",
        }};
      case "object": {
        let { queries } = await import("./object/queries");

        return {
          description: "Objects suite description",
          queries,
          queryKey: "queryObjects",
          dataPath: "objects.pageInfo",
          typeStringFields: ["type"],
          paramsFilePath: "./object/parameters.json",
        }};
      case "event": {
        let { queries } = await import("./event/queries");

        return {
          description: "Events suite description",
          queries,
          queryKey: "queryEvents",
          dataPath: "events.pageInfo",
          typeStringFields: ["eventType", "emittingModule"],
          paramsFilePath: "./event/parameters.json",
        }};
      default:
        throw new Error(`Unknown suite: ${suiteName}`);
    }
  }
