# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
import psycopg2.extras
import psycopg2.pool
import argparse
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

def create_partition(conn, index_name, index_condition, partition_idx):
    query = f"""CREATE INDEX IF NOT EXISTS
    {index_name}_{partition_idx} ON objects_history_partition_{partition_idx}
    {index_condition};"""

    print(f"Creating index for partition {partition_idx}...")
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(query, (partition_idx, partition_idx))
    except Exception as e:
        print(f"Error updating index: {e}")
    print(f"Done creating index for partition {partition_idx}.")
    connection_pool.putconn(conn)

def attach_partition(conn, index_name, index_condition, start, end):
    query = f"""CREATE INDEX IF NOT EXISTS {index_name}
    ON ONLY objects_history {index_condition};"""
    try:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error creating index: {e}")
        connection_pool.putconn(conn)
        return

    print("Starting to attach indexes")

    for i in range(start, end):
        query = f"ALTER INDEX {index_name} ATTACH PARTITION {index_name}_{i};"
        try:
            with conn.cursor() as cur:
                cur.execute(query)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Error attaching partition: {e}")
        print(f"Done attaching partition {i}.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--index-name", type=str, help="Name of the index to create", default="objects_history_coin_owner_object_id")
    parser.add_argument("--index-condition", type=str, help="Index condition", default="(checkpoint_sequence_number, owner_id, coin_type, object_id) WHERE coin_type IS NOT NULL AND owner_type = 1")
    parser.add_argument("--start", type=int, help="Start index", default=0)
    parser.add_argument("--end", type=int, help="End index", default=399)
    parser.add_argument("--max-connections", type=int, help="Max connections", default=200)
    parser.add_argument("--max-workers", type=int, help="Max workers", default=100)

    args = parser.parse_args()
    global connection_pool
    connection_pool = psycopg2.pool.SimpleConnectionPool(1, args.max_connections, **conn_params)


    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = [executor.submit(create_partition, connection_pool.getconn(), idx) for idx in range(args.start, args.end)]

        for future in as_completed(futures):
            future.result()

    print("Done creating indexes.")

    with connection_pool.getconn() as conn:
        attach_partition(conn, args.index_name, args.index_condition, args.start, args.end)
