// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import fs from 'fs';
import path from 'path';
import { SuiGraphQLClient, GraphQLDocument } from '@mysten/sui.js/graphql';
import { print, ASTNode } from 'graphql';
import { benchmark_connection_query, PageInfo } from './benchmark';

/**
 * Query functions that plan to be benchmarked should have the following signature:
 * @param client - The SuiGraphQLClient instance
 * @param variables - The variables to be passed to the query
 * @returns - An object containing the pageInfo and the variables
 */
type QueryFunction<Queries extends Record<string, GraphQLDocument>, Variables> = (
  client: SuiGraphQLClient<Queries>,
  variables: Variables,
) => Promise<{ pageInfo: PageInfo | undefined; variables: Variables }>;

/// For a suite of tests, be able to generate a set of filters
type FilterParameters = any; // Generalize the filter parameters type
type GenerateFiltersFunction = (data: FilterParameters) => Generator<any>;

export async function runQuerySuite<Queries extends Record<string, GraphQLDocument>, Variables, Data extends FilterParameters>(
  client: SuiGraphQLClient<Queries>,
  queries: Record<string, ASTNode>,
  queryKey: string, // e.g., 'queryTransactionBlocks' or 'queryEvents'
  queryFunction: QueryFunction<Queries, Variables>,
  generateFilters: GenerateFiltersFunction,
  suiteDescription: string,
  jsonFilePath: string,
) {
  const inputJsonPathName = path.parse(jsonFilePath).name;

  // Read and parse the JSON file
  const jsonData = fs.readFileSync(path.resolve(__dirname, jsonFilePath), 'utf-8');
  const data: Data = JSON.parse(jsonData);

  let limit = 50;
  let numPages = 10;
  const description = suiteDescription;
  const query = print(queries[queryKey]).replace(/\n/g, ' ');
  const fileName = `${queryKey}-${inputJsonPathName}-${new Date().toISOString()}.json`;
  const stream = fs.createWriteStream(path.join(__dirname, fileName), { flags: 'a' });

  stream.write(`{"description": "${description}", "query": "${query}", "reports": [`);
  for (let paginateForwards of [true, false]) {
    for (let filter of generateFilters(data)) {
      let report = await benchmark_connection_query(
        { paginateForwards, limit, numPages },
        async (paginationParams) => {
          let new_variables: Variables = {
            ...paginationParams,
            filter,
          } as Variables;
          return await queryFunction(client, new_variables);
        },
      );
      stream.write(`${JSON.stringify(report, null, 2)},`);
    }
  }
  stream.end(']}');
}
