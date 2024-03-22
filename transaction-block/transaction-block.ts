// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
import { SuiGraphQLClient } from '@mysten/sui.js/graphql';
import { VariablesOf } from '@mysten/sui.js/graphql/schemas/2024-01';

import { PageInfo } from '../benchmark';
import { generateTxFilters } from './parameterize';
import { queries } from './queries';
import { runQuerySuite } from '../suite';

const client = new SuiGraphQLClient({
	url: 'http://127.0.0.1:9000',
	queries,
});

type Variables = VariablesOf<(typeof queries)['queryTransactionBlocks']>;

async function queryTxBlocks(
	client: SuiGraphQLClient<typeof queries>,
	variables: Variables,
): Promise<{ pageInfo: PageInfo | undefined; variables: Variables }> {
	let response = await client.execute('queryTransactionBlocks', { variables });
	let data = response.data;
	return {
		pageInfo: data?.transactionBlocks.pageInfo,
		variables,
	};
}

runQuerySuite(
	client,
	queries,
	'queryTransactionBlocks',
	queryTxBlocks, // Define or import this function
	generateTxFilters, // This should be defined or imported
	"TxBlocks suite description",
	process.argv[2],
  );
