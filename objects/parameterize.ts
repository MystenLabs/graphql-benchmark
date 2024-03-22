// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { VariablesOf } from '@mysten/sui.js/graphql/schemas/2024-01';

import { emitParts, generateFilters } from '../parameterization';

import { queries } from './queries';

type Filter = VariablesOf<(typeof queries)['queryObjects']>['filter'];
type OtherParams = Omit<VariablesOf<(typeof queries)['queryObjects']>, 'filter'>;

export interface ObjectFilterParameters {
	type?: string[];
    owner?: string[];
    objectIds?: string[];
    objectKeys?: {
        objectId: string,
        version: string
    }[];
}

function get_filter_params(data: ObjectFilterParameters): { key: string; values: string[] }[] {
	return Object.keys(data).flatMap((key: string) => {
		if (key === 'type') {
			return {
				key,
				values: (data[key] as string[]).flatMap((type) => {
					return [...emitParts(type)];
				}),
			};
		} else if (key in data && data[key as keyof ObjectFilterParameters]) {
			return { key, values: data[key as keyof ObjectFilterParameters] as string[] };
		}
		return [];
	});
}

export function* generateObjectFilters(data: ObjectFilterParameters): Generator<Filter> {
	const params = get_filter_params(data);
	yield* generateFilters<Filter>(params);
}


// TODO: separate mechanism to handle other variables
