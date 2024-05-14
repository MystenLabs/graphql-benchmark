// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import yargs from "yargs";
import { hideBin } from "yargs/helpers";
import { runQuerySuite } from "./suite";

export interface Arguments {
  suite: string;
  paramsFilePath: string;
  limit: number;
  numPages: number;
  index: number;
  url: string;
  description: string | undefined;
  replay: boolean;
}

// Setup yargs
const argv = yargs(hideBin(process.argv))
  .usage("Usage: $0 [options]")
  .option("suite", {
    describe: "The name of the suite to run",
    type: "string",
    demandOption: true,
  })
  .option("paramsFilePath", {
    describe:
      "The path to the JSON file containing the parameters to benchmark",
    type: "string",
  })
  .option("limit", {
    describe: "The number of records to fetch per page",
    type: "number",
    default: 50,
  })
  .option("numPages", {
    describe: "The number of pages to fetch",
    type: "number",
    default: 10,
  })
  .option("index", {
    describe: "The index to start from",
    type: "number",
    default: -1,
  })
  .option("url", {
    describe: "The URL of the SuiGraphQLClient",
    type: "string",
    default: "http://127.0.0.1:8000", // Default URL
  })
  .option("description", {
    describe: "The description of the suite",
    type: "string"
  })
  .option("replay", {
    describe: "Replay a benchmark's list of reports in lieu of running a new benchmark.",
    type: "boolean",
    default: false

  })
  .help("h")
  .alias("h", "help")
  .parseSync();

runQuerySuite(argv as Arguments);
