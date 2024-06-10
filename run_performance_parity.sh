#!/bin/bash

# Base directory relative to the top-level directory
base_dir="evaluation/parity/benchmarks"

# Function to generate a timestamp string
generate_timestamp() {
  timestamp=$(date +%Y%m%d%H%M%S)
  echo "$timestamp"
}

timestamp=$(generate_timestamp)

# Loop through each subdirectory in the base directory
for suite in balance coin dynamic-field object transaction-block; do
  suite_dir="$base_dir/$suite"

  # Check if the suite directory exists and is a directory
  if [ -d "$suite_dir" ]; then

    # Loop through each .json file in the suite directory
    for json_file in "$suite_dir"/*.json; do
      # Check if the file exists (in case there are no .json files)
      if [ -f "$json_file" ]; then
        # Determine the suite name and construct the command
        case $suite in
          balance)
            suite_name="addressBalances"
            ;;
          coin)
            suite_name="coinsByOwner"
            ;;
          dynamic-field)
            suite_name="dfsByObject"
            ;;
          object)
            suite_name="object"
            ;;
          transaction-block)
            suite_name="transaction-block"
            ;;
          *)
            echo "Unknown suite: $suite"
            continue
            ;;
        esac

        # Construct the output file name
        output_file_name="$base_dir/$timestamp/$suite/$(basename "$json_file")"

        # Construct the command
        cmd="pnpm ts-node cli.ts --suite $suite_name --params-file-path $json_file --replay --output-file-name $output_file_name"

        # Print and execute the command
        echo "Executing: $cmd"
        $cmd
      fi
    done
  fi
done
