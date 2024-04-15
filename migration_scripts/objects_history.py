# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

import signal
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import psycopg2
import psycopg2.extras
import psycopg2.pool
from tqdm import tqdm
import argparse
import logging
import re
from dotenv import load_dotenv

load_dotenv()

# Database connection parameters
conn_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
}

max_connections = 200
connection_pool = psycopg2.pool.SimpleConnectionPool(1, max_connections, **conn_params)

def create_partition(conn, partition_idx):
    """
    Update the table with `address` and `rel` from `tx_addresses`.
    """
    query = f"""CREATE INDEX CONCURRENTLY IF NOT EXISTS
    objects_history_coin_only_{partition_idx} ON objects_history_partition_{partition_idx}
    (coin_type, object_id, checkpoint_sequence_number) WHERE coin_type IS NOT NULL AND owner_type = 1;"""

    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(query, (partition_idx, partition_idx))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error updating index: {e}")

def attach_partition(conn):
    for i in range(329):
        query = f"ALTER INDEX objects_history_coin_only ATTACH PARTITION objects_history_coin_only_{i};"
        try:
            with conn.cursor() as cur:
                cur.execute(query)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Error attaching partition: {e}")

# with ThreadPoolExecutor(max_workers=100) as executor:
    # futures = [executor.submit(create_partition, connection_pool.getconn(), idx) for idx in range(291, 329)]

    # for future in as_completed(futures):
        # future.result()

# print("Done creating indexes.")

with connection_pool.getconn() as conn:
    attach_partition(conn)
