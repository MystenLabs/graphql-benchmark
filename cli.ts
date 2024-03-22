import yargs from "yargs";
import { hideBin } from "yargs/helpers";
import { runSelectedSuite } from "./suite";

export interface Arguments {
  suite: string;
  paramsFilePath: string;
  limit: number;
  numPages: number;
  index: number;
  url: string;
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
  .help("h")
  .alias("h", "help")
  .parseSync();

runSelectedSuite(argv as Arguments);
