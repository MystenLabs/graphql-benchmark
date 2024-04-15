# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

import json
import argparse
from typing import List, Literal, Dict, Any, Optional
import requests
from pydantic import BaseModel
import datetime


class Metrics(BaseModel):
    min: float
    p50: float
    p90: float
    p95: float
    mean: float
    max: float
    durations: Optional[List[float]] = None


class Report(BaseModel):
    index: int
    status: Literal['COMPLETED', 'TIMED OUT']
    variables: Dict[str, Any]
    cursors: List[str]
    metrics: Optional[Metrics] = None


class ReportSummaries(BaseModel):
    description: str
    query: str
    # params: Dict[str, Any]
    reports: List[Report]


def merge(args):
    with open(args.files[0], 'r') as f:
        data = json.load(f)
        initial = ReportSummaries(**data)


    for file in args.files[1:]:
        with open(file, 'r') as f:
            data = json.load(f)
            report_summaries = ReportSummaries(**data)
            initial.reports.extend(report_summaries.reports)

    filename = 'merged_' + datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + '.json'
    output_dict = initial.dict()
    output_dict['description'] = 'Merged ' + ', '.join(args.files)
    with open(filename, 'w') as f:
        json.dump(initial.dict(), f, indent=2)




def exceeds(args):
    with open(args.file, 'r') as f:
        data = json.load(f)
        report_summaries = ReportSummaries(**data)

    exceeded = []
    not_exceeded = []
    exceeded_filters = []
    not_exceeded_filters = []

    for report in report_summaries.reports:
        if report.status == 'TIMED OUT' or (report.metrics is not None and report.metrics.max > args.max_duration):
            exceeded.append(report.dict())
            exceeded_filters.append(list(report.variables['filter'].keys()))
        else:
            not_exceeded.append(report.dict())
            not_exceeded_filters.append(list(report.variables['filter'].keys()))

    with open('exceeds.json', 'w') as f:
        json.dump(exceeded, f, indent=2)

    with open('not_exceeded.json', 'w') as f:
        json.dump(not_exceeded, f, indent=2)

    with open('exceeded_filters.json', 'w') as f:
        json.dump(exceeded_filters, f, indent=2)

    with open('not_exceeded_filters.json', 'w') as f:
        json.dump(not_exceeded_filters, f, indent=2)


def send_request(url, data):

    response = requests.post(url, json=data)
    print(data)
    print(response.status_code)
    print(response.text)
    return response.json()

def repro(args):
    with open(args.file, 'r') as f:
        data = json.load(f)
        report_summaries = ReportSummaries(**data)

    reports = report_summaries.reports

    url = "http://localhost:8000/graphql"

    send_request(url, {
        "variables": reports[args.idx]['variables'],
        "query": report_summaries.query
    })

def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_exceeds = subparsers.add_parser('exceeds')
    parser_exceeds.add_argument("--file", required=True, help="json file to analyze")
    parser_exceeds.add_argument("--max-duration", type=int, required=True, help="maximum duration in milliseconds")
    parser_exceeds.set_defaults(func=exceeds)

    parser_repro = subparsers.add_parser('repro')
    parser_repro.add_argument("--file", required=True, help="json file to analyze")
    parser_repro.add_argument("--idx", type=int, required=True, help="index of the report to reproduce")
    parser_repro.set_defaults(func=repro)

    parser_merge = subparsers.add_parser('merge')
    parser_merge.add_argument("--files", nargs='+', help="json files to merge")
    parser_merge.set_defaults(func=merge)


    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
