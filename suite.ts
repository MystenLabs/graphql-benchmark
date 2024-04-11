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

export async function runSelectedSuite(args: Arguments) {
  const suiteConfig = await getSuiteConfiguration(args.suite);

  const client = new SuiGraphQLClient({
    url: args.url,
    queries: suiteConfig.queries,
  });

  const paramsFilePath = args.paramsFilePath || suiteConfig.paramsFilePath;

  runQuerySuite(
    suiteConfig.description,
    paramsFilePath,
    client,
    suiteConfig.queries,
    suiteConfig.queryKey,
    suiteConfig.dataPath,
    suiteConfig.typeStringFields,
    args.index,
  );
}

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
  description: string,
  jsonFilePath: string,
  client: SuiGraphQLClient<Queries>,
  queries: Record<string, GraphQLDocument>,
  queryKey: Query, // e.g., 'queryTransactionBlocks' or 'queryEvents'
  dataPath: string, // e.g., 'objects.pageInfo'
  typeStringFields: string[],
  index: number,
) {
  const inputJsonPathName = path.parse(jsonFilePath).name;

  // Read and parse the JSON file
  const jsonData = fs.readFileSync(
    path.resolve(__dirname, jsonFilePath),
    "utf-8",
  );

  const parameters = JSON.parse(jsonData) as Parameters<any>;

  let limit = 50;
  let numPages = 10;
  const query = print(queries[queryKey] as ASTNode).replace(/\n/g, " ");
  const fileName = `${queryKey}-${inputJsonPathName}-${new Date().toISOString()}.json`;
  console.log(
    "Streaming to file: ",
    path.join(__dirname, "experiments", fileName),
  );
  const stream = fs.createWriteStream(
    path.join(__dirname, "experiments", fileName),
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

      if (i < totalRuns) {
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
): Promise<{ pageInfo: PageInfo | undefined; variables: typeof variables }> {
  const options: Omit<GraphQLQueryOptions<any, any>, "query"> = { variables };

  let response = await client.execute(queryName, options);
  let data = response.data;

  // Dynamically access the data using the dataPath with safety
  let result = dataPath
    .split(".")
    .reduce((acc: any, curr: string) => acc?.[curr], data);

  return {
    pageInfo: result ?? undefined, // Fallback to undefined if result is nullish
    variables,
  };
}
