// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

export enum TypeDepth {
	PACKAGE,
	MODULE,
	TYPE,
	PARAMS,
}
/// Given some string representing p::m::t<params>, emits potentially 4 parts, p, p::m, p::m::t, and
/// p::m::t<params>
export function* emitParts(
	fullString: string,
	depth: TypeDepth = TypeDepth.PARAMS,
): Generator<string> {
	// 0-indexed, increment by 1
	const [type, params] = fullString.split('<');
	const parts = type.split('::');
	let current = '';
	for (let i = 0; i < parts.length; i++) {
		let part = parts[i];
		current = current ? `${current}::${part}` : part;
		if (i === depth + 1) break;
		yield current;
	}
	if (params && depth === TypeDepth.PARAMS) {
		yield `${current}<${params}`;
	}
}

/**
 * Takes an array of parameter objects, each with a key and an array of values, and generates all combinations.
 * The `index` is used to keep track of the current parameter being processed.
 * The `prefix` is used to store the current combination of parameters.
 * If `index` >= length of `params`, then the current combination is complete and is yielded.
 * Otherwise, the function is called recursively with the next parameter, and the current combination.
 */
function* generateCombinations(
	params: { key: string; values: string[] }[],
	index: number = 0,
	prefix: { key: string; value: string }[] = [],
): Generator<{ key: string; value: string }[]> {
	if (index >= params.length) {
		yield prefix;
	} else {
		for (let value of params[index].values) {
			yield* generateCombinations(params, index + 1, [
				...prefix,
				{ key: params[index].key, value },
			]);
		}
		yield* generateCombinations(params, index + 1, prefix);
	}
}

type Params = {
	key: string;
	values: string[];
}[];

/**
 * Given an array of parameter objects, each with a key and an array of values, generates all
 * possible combinations that conform to some type `T`.
 */
export function* generateFilters<T>(params: Params): Generator<T> {
	for (let combination of generateCombinations(params)) {
		yield combination.reduce((filter, { key, value }) => ({ ...filter, [key]: value }), {} as T);
	}
}

function verify() {
	let type = '0x2::coin::Coin<0x2::sui::SUI>';

	console.log('to params');
	for (let part of emitParts(type, TypeDepth.PARAMS)) {
		console.log(part);
	}

	console.log('to type');
	for (let part of emitParts(type, TypeDepth.TYPE)) {
		console.log(part);
	}

	console.log('to module');
	for (let part of emitParts(type, TypeDepth.MODULE)) {
		console.log(part);
	}

	console.log('to package');
	for (let part of emitParts(type, TypeDepth.PACKAGE)) {
		console.log(part);
	}

	type = '0x2';

	console.log('to params');
	for (let part of emitParts(type, TypeDepth.PARAMS)) {
		console.log(part);
	}

	console.log('to type');
	for (let part of emitParts(type, TypeDepth.TYPE)) {
		console.log(part);
	}

	console.log('to module');
	for (let part of emitParts(type, TypeDepth.MODULE)) {
		console.log(part);
	}

	console.log('to package');
	for (let part of emitParts(type, TypeDepth.PACKAGE)) {
		console.log(part);
	}
}

// verify()
