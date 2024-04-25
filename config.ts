// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/**
 * Maps suites to default configurations, to be overriden by the user.
 */
export async function getSuiteConfiguration(suiteName: string) {
  switch (suiteName) {
    case "balance": {
      let { queries } = await import("./balance/queries");
      return {
        description: "Balance suite description",
        queries,
        queryKey: "queryBalance",
        dataPath: "",
        typeStringFields: ["address", "coinType"],
        paramsFilePath: "./balance/parameters.json",
      };
    }
    case "transaction-block": {
      let { queries } = await import("./transaction-block/queries");
      return {
        description: "TxBlocks suite description",
        queries,
        queryKey: "queryTransactionBlocks",
        dataPath: "transactionBlocks.pageInfo",
        typeStringFields: ["function"],
        paramsFilePath: "./transaction-block/parameters.json",
      };
    }
    case "object": {
      let { queries } = await import("./object/queries");

      return {
        description: "Objects suite description",
        queries,
        queryKey: "queryObjects",
        dataPath: "objects.pageInfo",
        typeStringFields: ["type"],
        paramsFilePath: "./object/parameters.json",
      };
    }
    case "event": {
      let { queries } = await import("./event/queries");

      return {
        description: "Events suite description",
        queries,
        queryKey: "queryEvents",
        dataPath: "events.pageInfo",
        typeStringFields: ["eventType", "emittingModule"],
        paramsFilePath: "./event/parameters.json",
      };
    }
    case "coinsByType":
    case "coinsByOwner": {
      let { queries } = await import("./coin/queries");
      let dataPath = suiteName === "coinsByType" ? "coins.pageInfo" : "owner.coins.pageInfo";

      return {
        description: "Coins suite description",
        queries,
        queryKey: suiteName,
        dataPath,
        typeStringFields: [],
        paramsFilePath: "./coin/parameters.json",
      };
    }
    case "dfsByOwner":
    case "dfsByObject":
      {
        let { queries } = await import("./dynamic-field/queries");
        let dataPath = suiteName === "dfsByOwner" ? "owner.dynamicFields.pageInfo" : "object.dynamicFields.pageInfo";
        let paramsFilePath = suiteName === "dfsByOwner" ? "./dynamic-field/parameters-owner.json" : "./dynamic-field/parameters-object.json";
        return {
          description: "Dynamic Fields suite description",
          queries,
          queryKey: suiteName,
          dataPath,
          typeStringFields: [],
          paramsFilePath,
        };
      }
    default:
      throw new Error(`Unknown suite: ${suiteName}`);
  }
}
