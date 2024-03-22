// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { VariablesOf } from '@mysten/sui.js/graphql/schemas/2024-01';

import { emitParts, generateFilters } from '../parameterization';
// Quick way to test
import data from './parameters.json';
import { queries } from './queries';

type Filter = VariablesOf<(typeof queries)['queryEvents']>['filter'];
type OtherParams = Omit<VariablesOf<(typeof queries)['queryEvents']>, 'filter'>;

export interface EventFilterParameters {
	sender?: string[];
	transactionDigest?: string[];
	emittingModule?: string[];
	eventType?: string[];
}

function get_filter_params(data: EventFilterParameters): { key: string; values: string[] }[] {
	return Object.keys(data).flatMap((key: string) => {
		if (key === 'eventType' || key === 'emittingModule') {
			return {
				key,
				values: (data[key] as string[]).flatMap((type) => {
					return [...emitParts(type)];
				}),
			};
		} else if (key in data && data[key as keyof EventFilterParameters]) {
			return { key, values: data[key as keyof EventFilterParameters] as string[] };
		}
		return [];
	});
}

export function* generateEventFilters(data: EventFilterParameters): Generator<Filter> {
	const params = get_filter_params(data);
	yield* generateFilters<Filter>(params);
}


// TODO: separate mechanism to handle other variables
