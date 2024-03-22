// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/**
 * Given a string of p[::m[::t[::params]]], emits the parts of the string based on the depth.
 */
export function expandTypeString(s: string): string[] {
  const parts = s.split('::');

  if (parts.length < 2) return [s]; // Return the original string if it doesn't match the pattern

  const p = parts[0];
  const m = parts[1];
  const tAndParams = parts.slice(2).join('::');

  const combinations = [p, `${p}::${m}`];
  if (tAndParams) {
    const t = tAndParams.split('<')[0]; // Assuming params are always enclosed in <>
    combinations.push(`${p}::${m}::${t}`);
    combinations.push(`${p}::${m}::${tAndParams}`);
  }

  return combinations;
}

type ArrayFieldsOptional<T> = {
  [P in keyof T]?: T[P] extends Array<infer U> ? U[] : never;
};

// A utility type to ensure the input only contains array fields
type EnsureArraysOnly<T> = {
  [P in keyof T]: T[P] extends Array<any> ? T[P] : never;
};

/**
 * Function that generates all possible of combinations of filters given an object whose fields are
 * arrays. It also takes an optional list of fields that signify which fields are type fields, and
 * should be expanded.
 */
export function generateFilterCombinations<T extends object>(obj: EnsureArraysOnly<T>, typeStringFields: Array<keyof T> = []): ArrayFieldsOptional<T>[] {
  const keys = Object.keys(obj) as Array<keyof T>;
  let combinations: ArrayFieldsOptional<T>[] = [{}];

  for (const key of keys) {
    const isSpecialStringField = typeStringFields.includes(key);
    let values = obj[key] as unknown as any[];

    // If this is a special string field, preprocess each value to expand it into its combinations
    if (isSpecialStringField) {
      values = values.flatMap(expandTypeString);
    }

    const newCombinations: ArrayFieldsOptional<T>[] = [];

    if (values) {
      for (const combination of combinations) {
        for (const value of values) {
          const newCombination = { ...combination, [key]: value } as ArrayFieldsOptional<T>;
          newCombinations.push(newCombination);
        }
      }
    }

    combinations = [...combinations, ...newCombinations];
  }

  return combinations;
}
