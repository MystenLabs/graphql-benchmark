// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { VariablesOf } from '@mysten/sui.js/graphql/schemas/2024-01';

import { emitParts, generateFilters, TypeDepth } from '../parameterization';
import { queries } from './queries';

enum TxTypeDepth {
	PACKAGE,
	MODULE,
}

function* emitTypes(type: string, depth: TxTypeDepth): Generator<string> {
	let part_depth = depth === TxTypeDepth.PACKAGE ? TypeDepth.PACKAGE : TypeDepth.MODULE;
	yield* emitParts(type, part_depth);
}

type Filter = VariablesOf<(typeof queries)['queryTransactionBlocks']>['filter'];

export interface TxBlockFilterParameters {
	function?: string[];
	kind?: string[];
	afterCheckpoint?: number[];
	atCheckpoint?: number[];
	beforeCheckpoint?: number[];
	signAddress?: string[];
	recvAddress?: string[];
	inputObject?: string[];
	changedObject?: string[];
	transactionIds?: string[];
}

function get_params(
	data: TxBlockFilterParameters,
	depth: TxTypeDepth,
): { key: string; values: string[] }[] {
	return Object.keys(data).flatMap((key: string) => {
		if (key === 'function' && data[key]) {
			return {
				key,
				values: (data[key] as string[]).flatMap((func) => {
					let result = [...emitTypes(func, depth)];
					return result;
				}),
			};
		} else if (key !== 'function' && key in data && data[key as keyof TxBlockFilterParameters]) {
			return { key, values: data[key as keyof TxBlockFilterParameters] as string[] };
		}
		return [];
	});
}

export function* generateTxFilters(data: TxBlockFilterParameters): Generator<Filter> {
	const params = get_params(data, TxTypeDepth.MODULE);
	yield* generateFilters<Filter>(params);
}

// Quick way to test
// import data from './parameters.json';
// let i = 0;
// for (let filter of generateTxFilters(data)) {
// i++;
// console.log(filter);
// }
// console.log('Total:', i);
