// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import fs from "fs";
import path from "path";

import {
  SuiGraphQLClient,
  GraphQLDocument,
  GraphQLQueryOptions,
} from "@mysten/sui.js/graphql";
import { ASTNode, print } from "graphql";
import { benchmark_connection_query, PageInfo } from "./benchmark";
import { Arguments } from "./cli";
import { EnsureArraysOnly, generateCombinations } from "./parameterization";
import { getSuiteConfiguration } from "./config";


export type Queries = Record<string, GraphQLDocument>;
export type Query = Extract<keyof Queries, string>;
export type Variables =
  Queries[Query] extends GraphQLDocument<unknown, infer V>
    ? V
    : Record<string, unknown>;
export type Parameters<T> = {
  filter?: EnsureArraysOnly<T>;
} & { [key: string]: any };

/**
 * This function runs a combination of filters emitted by `generateFilters` against a single graphQL
 * query and benchmarks their performance. Queries that can be paginated are paginated `numPages`
 * times, while other queries are run `numPage` times. This is repeated for both going forwards and
 * backwards.
 */
export async function runQuerySuite(
  args: Arguments,
) {
  const suiteConfig = await getSuiteConfiguration(args.suite);
  // Unique fields from suiteConfig and args
  const { queries, queryKey, dataPath, typeStringFields } = suiteConfig;
  const { limit, numPages, index, url } = args;

  // Merge others
  const description = args.description || suiteConfig.description;
  const jsonFilePath = args.paramsFilePath || suiteConfig.paramsFilePath;
  const inputJsonPathName = path.parse(jsonFilePath).name;

  const client = new SuiGraphQLClient({
    url,
    queries
  });

  // Read and parse the JSON file
  const jsonData = fs.readFileSync(
    path.resolve(__dirname, jsonFilePath),
    "utf-8",
  );

  const parameters = JSON.parse(jsonData) as Parameters<any>;
  const query = print(queries[queryKey] as ASTNode).replace(/\n/g, " ");
  const fileName = `${queryKey}-${inputJsonPathName}-${new Date().toISOString()}.json`;
  console.log(
    "Streaming to file: ",
    path.join(__dirname, "experiments", fileName),
  );

  // Create the directory if it doesn't exist
  const dir = path.join(__dirname, "experiments");
  if (!fs.existsSync(dir)){
      fs.mkdirSync(dir);
  }

  const stream = fs.createWriteStream(
    path.join(dir, fileName),
    {
      flags: "a",
    },
  );

  stream.write(
    `{"description": "${description}",\n"query": "${query}",\n"params": ${JSON.stringify(parameters)},\n"reports": [`,
  );

  let combinations = generateCombinations(parameters, typeStringFields);
  let totalRuns = combinations.length * 2;

  console.log("Total filter combinations to run: ", totalRuns);

  let i = -1;
  for (let paginateForwards of [true, false]) {
    for (let parameters of combinations) {
      i++;
      if (i <= index) {
        continue;
      }

      let report = await benchmark_connection_query(
        { paginateForwards, limit, numPages },
        async (paginationParams) => {
          let newVariables: Variables = {
            ...parameters,
            ...paginationParams,
          } as Variables;

          return await queryGeneric(client, queryKey, newVariables, dataPath);
        },
      );
      console.log("Completed run ", i);
      let indexed_report = { index: i, ...report };
      stream.write(`${JSON.stringify(indexed_report, null, 2)}`);

      if (i < totalRuns - 1) {
        stream.write(",");
      }
    }
  }
  stream.end("]}");
}

/**
 * Given a `client` and `queryName` to query, uses `dataPath` to extract the `PageInfo` object from
 * the response, and returns it along with the `variables` used in the query.
 */
export async function queryGeneric<
  Query extends Extract<keyof Queries, string>,
  Queries extends Record<string, GraphQLDocument>,
>(
  client: SuiGraphQLClient<Queries>,
  queryName: Query,
  variables: Queries[Query] extends GraphQLDocument<unknown, infer V>
    ? V
    : Record<string, unknown>,
  dataPath: string, // e.g., 'objects.pageInfo'
): Promise<{ pageInfo: PageInfo | string; variables: typeof variables }> {
  const options: Omit<GraphQLQueryOptions<any, any>, "query"> = { variables };

  let response = await client.execute(queryName, options);
  let data = response.data;

  // Dynamically access the data using the dataPath with safety
  let result = dataPath
    .split(".")
    .reduce((acc: any, curr: string) => acc?.[curr], data);

  // if response.data and response.errors both undefined, then unexpected
  if (response.data === undefined && response.errors === undefined) {
    result = "UNEXPECTED ERROR";
  }
  if (response.errors !== undefined) {
    let errorMessage = response.errors![0].message;
    if (errorMessage.includes("Request timed out") || errorMessage.includes("statement timeout")) {
      result = "TIMEOUT";
    } else {
      result = errorMessage;
    }
  }

  return {
    pageInfo: result, // Fallback to undefined if result is nullish
    variables,
  };
}
