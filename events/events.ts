// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
import fs from 'fs';
import path from 'path';
import { SuiGraphQLClient } from '@mysten/sui.js/graphql';
import { VariablesOf } from '@mysten/sui.js/graphql/schemas/2024-01';
import { print } from 'graphql';

import { benchmark_connection_query, PageInfo } from '../benchmark';
import { EventFilterParameters, generateEventFilters } from './parameterize';
import { queries } from './queries';
import { runQuerySuite } from '../suite';

const client = new SuiGraphQLClient({
	url: 'http://127.0.0.1:9000',
	queries,
});

type Variables = VariablesOf<(typeof queries)['queryEvents']>;

async function queryEvents(
	client: SuiGraphQLClient<typeof queries>,
	variables: Variables,
): Promise<{ pageInfo: PageInfo | undefined; variables: Variables }> {
	let response = await client.execute('queryEvents', { variables });
	let data = response.data;
	return {
		pageInfo: data?.events.pageInfo,
		variables,
	};
}

// Get the JSON file path from the command line arguments
// const jsonFilePath = process.argv[2];
// const inputJsonPathName = path.parse(jsonFilePath).name;

// // Read the JSON file
// const jsonData = fs.readFileSync(path.resolve(__dirname, jsonFilePath), 'utf-8');

// // Parse the JSON data
// const data: EventFilterParameters = JSON.parse(jsonData);

// /// Orchestrates test suite
// async function eventsSuite(client: SuiGraphQLClient<typeof queries>) {
// 	let limit = 50;
// 	let numPages = 10;
// 	const description = "This suite tests the performance of the 'queryEvents' query with different filters and pagination options.";
// 	const reports = [];
// 	const query = print(queries['queryEvents']).replace(/\n/g, ' ');
// 	const fileName = `events-${inputJsonPathName}-${new Date().toISOString()}.json`;
// 	const stream = fs.createWriteStream(path.join(__dirname, fileName), { flags: 'a' });

// 	stream.write(`{"description": "${description}", "query": "${query}", "reports": [`);

// 	for (let paginateForwards of [true, false]) {
// 		for (let filter of generateEventFilters(data)) {
// 			let report = await benchmark_connection_query(
// 				{ paginateForwards, limit, numPages },
// 				async (paginationParams) => {
// 					let new_variables: Variables = {
// 						...paginationParams,
// 						filter,
// 					};
// 					return await queryEvents(client, new_variables);
// 				},
// 			);
// 			stream.write(`${JSON.stringify(report, null, 2)},`);
// 		}
// 	}

// 	stream.end(']}');
// }

// eventsSuite(client);

// npx ts-node events/events.ts parameters.json

runQuerySuite(
	client,
	queries,
	'queryEvents',
	queryEvents, // Define or import this function
	generateEventFilters, // This should be defined or imported
	"Events suite description",
	process.argv[2],
  );
