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

def signal_handler(signum, frame):
    global shutdown_requested
    shutdown_requested = True
    print("shutting down")

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
shutdown_requested = False

logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

def log_error(thread_id, start_tx, end_cp, error_message):
    logging.error(f"Thread {thread_id} error at range {start_tx}-{end_cp}: {error_message}")

def log_shutdown(thread_id, start_tx, end_cp):
    logging.info(f"Thread {thread_id} shutdown at range {start_tx}-{end_cp}")

def log_progress(thread_id, start_tx, end_cp, chunk_size):
    logging.info(f"Thread {thread_id} processed range {start_tx}-{end_cp} of size {chunk_size}")

# Database connection parameters
conn_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

max_connections = 100
connection_pool = psycopg2.pool.SimpleConnectionPool(1, max_connections, **conn_params)



def latest_tx_sequence_number():
    conn = connection_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(tx_sequence_number) FROM tx_addresses;")
            return cur.fetchone()[0]
    finally:
        connection_pool.putconn(conn)

def latest_cp_sequence_number():
    conn = connection_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(sequence_number) FROM checkpoints;")
            return cur.fetchone()[0]
    finally:
        connection_pool.putconn(conn)


def process_range(pbar, thread_id, func, start, end, chunk_size = 10000):
    """Process start and end range in chunks of chunk_size"""
    conn = connection_pool.getconn()
    completed = 0
    try:
        for idx in range(start, end + 1, chunk_size):
            if shutdown_requested:
                conn.cancel()
                log_shutdown(thread_id, idx, end)
                return
            end_of_chunk = min(idx + chunk_size - 1, end)
            func(conn, idx, end_of_chunk)
            pbar.update(chunk_size)
            completed += chunk_size
    except Exception as e:
        log_error(thread_id, start + completed, end, str(e))
        raise
    finally:
        connection_pool.putconn(conn)



def update_address_with_cp(conn, start, end, rel):
    """
    Update the `tx_senders` or `tx_recipients` tables with `checkpoint_sequence_number`. The other
    tables will derive the necessary data from these two address tables.
    """
    query = f"""
    WITH filtered_transactions AS (
        SELECT tx_sequence_number, checkpoint_sequence_number, transaction_kind
        FROM transactions
        WHERE tx_sequence_number BETWEEN %s AND %s
    ),
    filtered_recipients AS (
        SELECT tx_sequence_number, cp_sequence_number, recipient
        FROM tx_recipients
        WHERE tx_sequence_number BETWEEN %s AND %s order by tx_sequence_number
    )
    -- Insert using a simpler join
    INSERT INTO tx_recipients_rel (cp_sequence_number, tx_sequence_number, recipient, transaction_kind)
    SELECT r.cp_sequence_number, r.tx_sequence_number, r.recipient, t.transaction_kind
    FROM filtered_transactions t
    JOIN filtered_recipients r ON t.tx_sequence_number = r.tx_sequence_number AND t.checkpoint_sequence_number = r.cp_sequence_number;
    """
    try:
        with conn.cursor() as cur:
            # cur.execute("SET statement_timeout = 20000;")
            cur.execute(query, (start, end, start, end))
        conn.commit()
    except Exception as e:
        print(query % (start, end, start, end))
        print(f"Error updating tx_{rel}s table: {e}")

def create_merged_address_table(conn, start, end):
    query = """
    WITH s AS (
        SELECT sender as address, * FROM tx_senders WHERE tx_sequence_number BETWEEN %s AND %s
    ),
    r AS (
        SELECT recipient as address, * FROM tx_recipients WHERE tx_sequence_number BETWEEN %s AND %s
    )
    INSERT INTO tx_addresses (tx_sequence_number, address, cp_sequence_number, rel)
    SELECT
        COALESCE(s.tx_sequence_number, r.tx_sequence_number) AS tx_sequence_number,
        COALESCE(s.address, r.address) AS address,
        COALESCE(s.cp_sequence_number, r.cp_sequence_number) AS cp_sequence_number,
        CASE
            WHEN s.address IS NOT NULL AND r.address IS NULL THEN 0 -- Sender
            WHEN s.address IS NULL AND r.address IS NOT NULL THEN 2 -- Recipient
            ELSE 1 -- SenderAndRecipient
        END AS rel
    FROM s
    FULL OUTER JOIN r ON s.tx_sequence_number = r.tx_sequence_number AND s.address = r.address
    ON CONFLICT (address, tx_sequence_number, cp_sequence_number) DO NOTHING;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (start, end, start, end))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error updating tx_addresses table: {e} given start: {start} and end: {end}")


def update_table_with_addr(conn, start, end, table, new_table_name, fields, primary_key = None):
    """
    Update the table with `address` and `rel` from `tx_addresses`.
    """

    handle_primary_key_conflict = "ON CONFLICT DO NOTHING" if primary_key is None else f"""
    ON CONFLICT ({', '.join(primary_key)}) DO UPDATE
    SET address = EXCLUDED.address, rel = EXCLUDED.rel"""

    query = f"""
    WITH txs AS (
        SELECT tx_sequence_number, cp_sequence_number, address, rel
        FROM tx_addresses
        WHERE tx_sequence_number BETWEEN %s AND %s
    ),
    partial AS (
        SELECT {', '.join(fields)}, txs.tx_sequence_number, txs.cp_sequence_number, txs.address, txs.rel
        FROM {table}
        JOIN txs USING (tx_sequence_number)
    )
    INSERT INTO {new_table_name} (tx_sequence_number, cp_sequence_number, address, rel, {', '.join(fields)})
    SELECT tx_sequence_number, cp_sequence_number, address, rel, {', '.join(fields)}
    FROM partial
    {handle_primary_key_conflict};
    """
    try:
        with conn.cursor() as cur:
            # cur.execute("SET LOCAL statement_timeout = 5000;")
            cur.execute(query, (start, end))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error updating {new_table_name}: {e}")

def start_threads(func, start, end, thread_ranges=[]):
    total_range = end - start + 1
    range_per_thread = math.ceil(total_range / max_connections)
    if not thread_ranges:
        print("Initializing range for each thread to process")
        thread_ranges = [(start + i * range_per_thread, min(start + (i + 1) * range_per_thread - 1, end)) for i in range(max_connections)]
    else:
        total_range = 0
        for start, end in thread_ranges:
            total_range += end - start + 1

    # fan-out
    with tqdm(total=total_range, desc="Processing checkpoints") as pbar:
        with ThreadPoolExecutor(max_workers=max_connections) as executor:

            futures = [executor.submit(process_range, pbar, i, func, thread_start, thread_end) for i, (thread_start, thread_end) in enumerate(thread_ranges)]

            if shutdown_requested and not all(future.done() for future in futures):
                print("Shutting down threads...")
                executor.shutdown(wait=False)
                for future in futures:
                    future.cancel()

            # Wait for all threads to complete and catch any exceptions
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error during database update operation: {e}")



def process_log(log_file):
    pattern = r'(\d+)-(\d+)'
    thread_ranges = []
    with open(log_file) as f:
        for line in f:
            match = re.search(pattern, line)
            if match:
                start, end = match.groups()
                thread_ranges.append((int(start), int(end)))
    return thread_ranges


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", help="Start of the range to process", type=int, default=0)
    parser.add_argument("--end", help="End of the range to process", type=int)
    parser.add_argument("--resume", help="Continue processing per a log file", action="store_true")
    parser.add_argument("--log", help="Log file to continue processing", type=str)
    parser.add_argument("--add-addresses", help="Add address (sender or recipient) to the table", action="store_true")
    parser.add_argument("--rel", help="Relation to update", type=str, default='sender')
    parser.add_argument("--add-tx-kind", help="Add transaction_kind to the table", action="store_true")
    parser.add_argument("--table", help="Table to update", type=str)
    parser.add_argument("--merge-addresses", help="Merge sender and recipient addresses", action="store_true")

    args = parser.parse_args()

    thread_ranges = []
    if args.resume and args.log:
        thread_ranges = process_log(args.log)

    if args.end:
        end_tx = args.end
    else:
        end_tx = latest_tx_sequence_number()

    if args.add_tx_kind:
        if args.table == 'tx_senders' or args.table == 'tx_recipients':
            rel = 'sender' if args.table == 'tx_senders' else 'recipient'
            def update_table_wrapper(conn, start, end):
                update_address_with_cp(conn, start, end, rel)
            start_threads(update_table_wrapper, args.start, latest_tx_sequence_number(), thread_ranges)
    elif args.merge_addresses:
        start_threads(create_merged_address_table, args.start, end_tx, thread_ranges)
    elif args.add_addresses:
        if args.table == 'tx_calls':
            def update_table_wrapper(conn, start, end):
                fields = ['package', 'module', 'func']
                new_table_name = 'tx_calls_rel'
                update_table_with_addr(conn, start, end, 'tx_calls', new_table_name, fields, None)
        elif args.table == 'tx_changed_objects':
            def update_table_wrapper(conn, start, end):
                fields = ['object_id']
                new_table_name = 'tx_changed_objects_rel'
                update_table_with_addr(conn, start, end, 'tx_changed_objects', new_table_name, fields, None)
        elif args.table == 'tx_input_objects':
            def update_table_wrapper(conn, start, end):
                fields = ['object_id']
                new_table_name = 'tx_input_objects_rel'
                update_table_with_addr(conn, start, end, 'tx_input_objects', new_table_name, fields, None)
        start_threads(update_table_wrapper, args.start, end_tx, thread_ranges)
