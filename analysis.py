# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

from review import ReportSummaries, Report
from typing import List
import pandas as pd
import json
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from itertools import combinations, chain
import datetime
import argparse


# global variables to enforce consistency
TIMED_OUT = 1
PARAMS_INDEX = 'params_index'
STATUS = 'status'
NUM_FILTERS = 'num_filters'
COMBINATIONS = 'filter_combinations'
DETAILED_COMBINATIONS = 'detailed_filter_combinations'
NON_VARIABLE_KEYS = [STATUS, PARAMS_INDEX, NUM_FILTERS, DETAILED_COMBINATIONS]
NON_FILTER_KEYS = [STATUS, PARAMS_INDEX, DETAILED_COMBINATIONS]

# Load JSON data
def load_json_data(filepath):
    with open(filepath, 'r') as file:
        data = json.load(file)

    return data

# Transform data to DataFrame
def extract_filter_keys_and_status(reports: List[Report]) -> pd.DataFrame:
    """
    Given a `List[Report]`, converts `variables['filter']` into one-hot encoded columns and adds a binary column for the status.
    Also tallies the number of filters for each report into a new column `num_filters`.
    Sets the index of each report as a new column `params_index`.
    """
    # Prepare data for DataFrame
    transformed_data = []
    for report in reports:
        # Convert each report.variables into a dictionary of key: 1 for one-hot encoding
        flattened_variables = {k: v for k, v in report.variables.items() if k != 'filter'}
        # Flatten the nested 'filter' onto the top level, if it exists
        if 'filter' in report.variables:
            flattened_variables.update(report.variables['filter'])
        report_data = {key: 1 for key in flattened_variables.keys()}
        # Add binary status (1 for "TIMED OUT", 0 for "COMPLETED")
        report_data[STATUS] = TIMED_OUT if report.status == "TIMED OUT" else 0
        report_data[PARAMS_INDEX] = report.index
        report_data[DETAILED_COMBINATIONS] = report.variables
        transformed_data.append(report_data)

    # Create a DataFrame and fill missing values with 0
    df = pd.DataFrame(transformed_data).fillna(0)
    df[NUM_FILTERS] = df.drop(NON_FILTER_KEYS, axis=1).sum(axis=1)
    return df


def timeout_analysis(df):
    """
    Given a DataFrame with columns 'num_filters', 'status', and 'params_index', calculates the following metrics:
    - Number of timed-out queries
    - Number of successful queries
    - Ratio of timed-out queries to successful queries for each number of filters
    - Ratio of timed-out queries to total queries for each number of filters
    - Indexes of timed-out queries for each number of filters for further analysis
    """
    def metrics(x):
        # Counts
        timed_out_count = (x[STATUS] == TIMED_OUT).sum()
        success_count = (x[STATUS] != TIMED_OUT).sum()  # Adjust based on what 'success' means in your context

        # Ratios
        timeout_success_ratio = timed_out_count / success_count if success_count else float('inf')
        timeout_total_ratio = timed_out_count / len(x)

        # Indexes of timed-out queries
        timeout_indexes = x[PARAMS_INDEX][x[STATUS] == TIMED_OUT].tolist()
        # Details of filter combinations, as a list, in same order as timeout_indexes
        detailed_filter_combinations = x[DETAILED_COMBINATIONS][x[STATUS] == TIMED_OUT].tolist()

        indexed_combinations = {}
        for index, combination in zip(timeout_indexes, detailed_filter_combinations):
            temp = []
            for field, value in combination.items():
                if field == 'filter':
                    temp.extend(value.keys())
                else:
                    temp.append(field)
            key = ','.join(sorted(temp))
            entries = indexed_combinations.get(key, {})
            entries[index] = combination
            indexed_combinations[key] = entries

        return pd.Series([timed_out_count, success_count, timeout_success_ratio, timeout_total_ratio, indexed_combinations],
                         index=['timed_out', 'success', 'timeout_success_ratio', 'timeout_total_ratio', 'combinations'])

    # Apply the calculations and reset the index
    detailed_df = df.groupby('num_filters').apply(metrics).reset_index()

    return detailed_df

def calculate_coefs(df):
    """
    Calculates roughly how much each filter contributes to the likelihood of a query timing out.
    """
    X = df.drop(NON_VARIABLE_KEYS, axis=1)
    y = df[STATUS]

    X_train, X_test, y_train, _ = train_test_split(X, y, test_size=0.2, random_state=42)

    model = LogisticRegression(max_iter=1000)
    model.fit(X_train, y_train)

    model.predict(X_test)

    # Coefficients
    coefficients = pd.DataFrame(model.coef_.flatten(), X.columns, columns=['Coefficient'])
    # Add raw counts to the coefficients DataFrame
    sorted_coefficients = coefficients.sort_values(by='Coefficient', ascending=True)
    print(sorted_coefficients)

    return sorted_coefficients

def powerset(iterable):
    "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in range(len(s)+1))

# Main function to load, validate, and analyze data
def main(filepath):
    data = load_json_data(filepath)

    try:
        summaries = ReportSummaries(**data)
    except Exception as e:
        print("Data validation error:", e)
        return

    df = extract_filter_keys_and_status(summaries.reports)

    # check if there are any "TIMED OUT", and return early if there aren't any
    value_exists = (df[STATUS] == 1).any()

    if not value_exists:
        print("No timed out queries found in the data")
        return

    calculate_coefs(df)

    timed_out_df = timeout_analysis(df)
    filename = 'analysis/' + datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    print("Writing analysis to json file: ", filename + ".json")

    with open(filename + '.json', 'w') as f:
        json.dump(timed_out_df.to_dict(orient='records'), f, indent=2)

    print("Writing analysis to csv file: ", filename + ".csv")

    timed_out_df.to_csv(filename + '.csv', index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="json file to analyze")
    args = parser.parse_args()
    main(args.file)
