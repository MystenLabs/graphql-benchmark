// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
import { SuiGraphQLClient, GraphQLDocument, GraphQLQueryOptions } from '@mysten/sui.js/graphql';
import { VariablesOf } from '@mysten/sui.js/graphql/schemas/2024-01';

import { PageInfo } from '../benchmark';
import { generateObjectFilters } from './parameterize';
import { queries } from './queries';
import { runQuerySuite } from '../suite';

const client = new SuiGraphQLClient({
	url: 'http://127.0.0.1:9000',
	queries,
});

type Variables = VariablesOf<(typeof queries)['queryObjects']>;

async function queryObjects(
	client: SuiGraphQLClient<typeof queries>,
	variables: Variables,
): Promise<{ pageInfo: PageInfo | undefined; variables: Variables }> {
	let response = await client.execute('queryObjects', { variables });
	let data = response.data;
	return {
		pageInfo: data?.objects.pageInfo,
		variables,
	};
}

// runQuerySuite(
	// client,
	// queries,
	// 'queryObjects',
	// queryObjects, // Define or import this function
	// generateObjectFilters, // This should be defined or imported
	// "Objects suite description",
	// process.argv[2],
//   );




type GenericVariables = Record<string, any>; // Adjust this to match your actual variables type


async function queryGeneric<Query extends Extract<keyof Queries, string>, Queries extends Record<string, GraphQLDocument>>(
    client: SuiGraphQLClient<Queries>,
    queryName: Query,
    variables: Queries[Query] extends GraphQLDocument<unknown, infer V> ? V : Record<string, unknown>,
    dataPath: string // e.g., 'objects.pageInfo'
): Promise<{ pageInfo: PageInfo | undefined; variables: typeof variables }> {
    const options: Omit<GraphQLQueryOptions<any, any>, 'query'> = { variables };

    let response = await client.execute(queryName, options);
    let data = response.data;
    console.log(data);

    // Dynamically access the data using the dataPath with safety
    let result = dataPath.split('.').reduce((acc: any, curr: string) => acc?.[curr], data);

    return {
        pageInfo: result ?? undefined, // Fallback to undefined if result is nullish
        variables,
    };
}


queryGeneric(client, 'queryObjects', { filter: { type: '0x2::coin::Coin' } }, 'objects.pageInfo');
