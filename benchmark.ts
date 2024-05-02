// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

export interface PageInfo {
  hasNextPage: boolean;
  hasPreviousPage: boolean;
  endCursor: string | null;
  startCursor: string | null;
}

export type BenchmarkParams = {
  paginateForwards: boolean;
  limit: number;
  numPages: number;
};

export type PaginationParams = {
  first?: number;
  after?: string;
  last?: number;
  before?: string;
};

export class PaginationV2 {
  paginateForwards: boolean;
  limit: number;
  cursor?: string;

  constructor(paginateForwards: boolean, limit: number) {
    this.paginateForwards = paginateForwards;
    this.limit = limit;
  }

  getParams(): PaginationParams {
    if (this.paginateForwards) {
      return {
        first: this.limit,
        after: this.cursor,
      };
    }
    return {
      last: this.limit,
      before: this.cursor,
    };
  }

  getCursor(): string | undefined {
    return this.cursor;
  }

  setCursor(pageInfo: PageInfo) {
    if (this.paginateForwards) {
      if (pageInfo.hasNextPage) {
        this.cursor = pageInfo.endCursor!;
      }
    } else {
      if (pageInfo.hasPreviousPage) {
        this.cursor = pageInfo.startCursor!;
      }
    }
  }
}

/// Caller is responsible for providing a `testFn` that returns the `PageInfo` for the benchmark to
/// paginate through. Tries to paginate `numPages` times, or repeats `numPages` times if there is no
/// next page.
export async function benchmark_connection_query(
  benchmarkParams: BenchmarkParams,
  testFn: (
    cursor: PaginationParams,
  ) => Promise<{ pageInfo: PageInfo | string; variables: any }>,
): Promise<Report> {
  let { paginateForwards, limit, numPages } = benchmarkParams;

  const cursors: Array<string> = [];
  let hasNextPage = true;
  let durations: number[] = [];

  let pagination = new PaginationV2(paginateForwards, limit);
  let queryParams;

  for (let i = 0; i < numPages && hasNextPage; i++) {
    let start = performance.now();
    let { pageInfo: result, variables } = await testFn(pagination.getParams());
    let duration = performance.now() - start;
    durations.push(duration);
    if (i == 0) {
      queryParams = variables;
    }

    if (typeof result === 'string') {
      console.log(result);
      // allow up to 3 retries
      if (i == 2) {
        break;
      }
      continue;
    }

    let cursor = pagination.getCursor();
    if (cursor) {
      cursors.push(cursor);
    }

    // Defer to pagination to update cursor
    pagination.setCursor(result);
  }

  // sleep for 1 second
  await new Promise((r) => setTimeout(r, 1000));

  return report(queryParams, cursors, metrics(durations));
}

type Metrics = {
  min: number;
  p50: number;
  p90: number;
  p95: number;
  mean: number;
  max: number;
  durations: number[];
};

export function metrics(durations: number[]): Metrics {
  // consider the initial 3 entries as warmup
  const all_durations = durations;
  durations = durations.slice(3);
  const sorted = durations.sort((a, b) => a - b);
  const p50 = sorted[Math.floor(durations.length * 0.5)];
  const p90 = sorted[Math.floor(durations.length * 0.9)];
  const p95 = sorted[Math.floor(durations.length * 0.95)];
  const sum = sorted.reduce((a, b) => a + b, 0);
  return {
    min: sorted[0],
    p50,
    p90,
    p95,
    mean: sum / durations.length,
    max: sorted[sorted.length - 1],
    durations: all_durations,
  };
}

type Report = {
  variables: any;
  cursors: string[];
  status: "COMPLETED" | "TIMED OUT" | "ERROR";
  metrics?: Metrics;
};

export function report<T>(
  variables: T,
  cursors: string[],
  metrics: Metrics,
): Report {
  // Set defaults and shared data
  let reportObject: Report = {
    status: "COMPLETED",
    variables,
    cursors,
  };

  if (!(metrics.durations.length > 3)) {
    reportObject.status = "TIMED OUT";
  } else {
    reportObject.metrics = metrics;
  }

  return reportObject;
}
