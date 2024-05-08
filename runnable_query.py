# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

import re
import ast
import argparse

def format_bytea(bytea_value):
    """Formats a list of bytes into a PostgreSQL bytea literal in hex format."""
    return f"'\\x{''.join(format(byte, '02x') for byte in bytea_value)}'::bytea"

def parse_binds_from_comment(sql):
    """Parses the bind values from the SQL comment."""
    match = re.search(r'-- binds: (.+)$', sql, re.MULTILINE)
    if match:
        # Use ast.literal_eval for safe evaluation of the string as a Python expression
        binds = ast.literal_eval(match.group(1))
        return binds
    return []

def replace_placeholders(sql, binds):
    """Replaces placeholders in the SQL query with the binds values."""
    for i, value in enumerate(binds, start=1):
        placeholder = f"${i}"

        if isinstance(value, list) and all(isinstance(byte, int) and 0 <= byte <= 255 for byte in value):
            formatted_value = format_bytea(value)
        # list of list of bytes
        elif isinstance(value, list) and all(isinstance(byte, list) and all(isinstance(b, int) and 0 <= b <= 255 for b in byte) for byte in value):
            formatted_value = f"ARRAY[{', '.join(format_bytea(byte) for byte in value)}]"
        elif isinstance(value, str):
            formatted_value = f"'{value}'"
        else:
            formatted_value = str(value)

        sql = sql.replace(placeholder, formatted_value, 1)

    return sql


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Replace placeholders in a SQL query with bind values')
    parser.add_argument('--sql', type=str, help='The SQL query with bind values in a comment')
    args = parser.parse_args()

    binds = parse_binds_from_comment(args.sql)
    formatted_sql = replace_placeholders(args.sql, binds)
    print(formatted_sql)
