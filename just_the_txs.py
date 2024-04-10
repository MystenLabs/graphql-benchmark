"""
CREATE TABLE tx_sequence_numbers (
    tx_sequence_number bigint NOT NULL,
    checkpoint_sequence_number bigint NOT NULL,
    PRIMARY KEY (tx_sequence_number)
);

CREATE TABLE tx_calls_cp (
    tx_sequence_number bigint NOT NULL,
    checkpoint_sequence_number bigint NOT NULL,
    package bytea NOT NULL,
    module text NOT NULL,
    func text NOT NULL,
    address bytea NOT NULL,
    rel smallint NOT NULL,
    PRIMARY KEY (package, module, func, address, tx_sequence_number)
);
CREATE INDEX pkg_tx ON tx_calls_cp (package, tx_sequence_number, checkpoint_sequence_number);
CREATE INDEX pkg_addr_tx ON tx_calls_cp (package, address, tx_sequence_number, checkpoint_sequence_number);
CREATE INDEX pkg_addr_rel_tx ON tx_calls_cp (package, address, rel, tx_sequence_number, checkpoint_sequence_number);
CREATE INDEX pkg_mod_tx ON tx_calls_cp (package, module, tx_sequence_number, checkpoint_sequence_number);
CREATE INDEX pkg_mod_addr_tx ON tx_calls_cp (package, module, address, tx_sequence_number, checkpoint_sequence_number);
CREATE INDEX pkg_mod_addr_rel_tx ON tx_calls_cp (package, module, address, rel, tx_sequence_number, checkpoint_sequence_number);


CREATE TABLE tx_senders_cp (
    tx_sequence_number bigint NOT NULL,
    address bytea NOT NULL,
    checkpoint_sequence_number bigint NOT NULL,
    PRIMARY KEY (address, tx_sequence_number, checkpoint_sequence_number)
);
CREATE INDEX tx_seq_num_senders ON tx_senders_cp (tx_sequence_number);

CREATE TABLE tx_recipients_cp (
    tx_sequence_number bigint NOT NULL,
    address bytea NOT NULL,
    checkpoint_sequence_number bigint NOT NULL,
    PRIMARY KEY (address, tx_sequence_number, checkpoint_sequence_number)
);
CREATE INDEX tx_seq_num_recipients ON tx_recipients_cp (tx_sequence_number);
# these are needed to speed up the migration for other tx_ lookup tables


CREATE TABLE tx_addresses (
    tx_sequence_number bigint NOT NULL,
    address bytea NOT NULL,
    checkpoint_sequence_number bigint NOT NULL,
    rel smallint NOT NULL,
    PRIMARY KEY (address, rel, tx_sequence_number, checkpoint_sequence_number)
);

"""

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
    'port': os.getenv('DB_PORT'),
}

max_connections = 200
connection_pool = psycopg2.pool.SimpleConnectionPool(1, max_connections, **conn_params)



def latest_tx_sequence_number():
    conn = connection_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(tx_sequence_number) FROM transactions;")
            return cur.fetchone()[0]
    finally:
        connection_pool.putconn(conn)


def process_range(pbar, thread_id, func, start, end, chunk_size = 10000):
    """Process start and end range in chunks of chunk_size"""
    conn = connection_pool.getconn()
    try:
        for idx in range(start, end + 1, chunk_size):
            if shutdown_requested:
                log_shutdown(thread_id, idx, end)
                return
            end_of_chunk = min(idx + chunk_size - 1, end)
            func(conn, idx, end_of_chunk)
            pbar.update(chunk_size)
    except Exception as e:
        log_error(thread_id, start, end, str(e))
        raise
    finally:
        connection_pool.putconn(conn)


def setup_unpartitioned_table(conn, start, end):
    """Populates the unpartitioned table of (tx_sequence_number, checkpoint_sequence_number) """
    query = """
    WITH txs AS (
        SELECT tx_sequence_number, checkpoint_sequence_number
        FROM transactions
        WHERE tx_sequence_number BETWEEN %s AND %s
    )
    INSERT INTO tx_sequence_numbers (tx_sequence_number, checkpoint_sequence_number)
    SELECT tx_sequence_number, checkpoint_sequence_number
    FROM txs
    ON CONFLICT (tx_sequence_number) DO NOTHING;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (start, end))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error updating tx_sequence_numbers table: {e}")


def update_address_with_cp(conn, start, end, rel):
    """
    Update the `tx_senders` or `tx_recipients` tables with `checkpoint_sequence_number`. The other
    tables will derive the necessary data from these two address tables.
    """
    query = f"""
    WITH txs AS (
        SELECT tx_sequence_number, checkpoint_sequence_number
        FROM tx_sequence_numbers
        WHERE tx_sequence_number BETWEEN %s AND %s
    ),
    partial AS (
        SELECT txs.tx_sequence_number, txs.checkpoint_sequence_number, {rel}
        FROM tx_{rel}s
        JOIN txs USING (tx_sequence_number)
    )
    INSERT INTO tx_{rel}s_cp (tx_sequence_number, checkpoint_sequence_number, address)
    SELECT tx_sequence_number, checkpoint_sequence_number, {rel}
    FROM partial
    ON CONFLICT (address, tx_sequence_number, checkpoint_sequence_number) DO NOTHING;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (start, end))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error updating tx_{rel}s_cp table: {e}")

def create_merged_address_table(conn, start, end):
    query = """
    WITH s AS (
        SELECT * FROM tx_senders_cp WHERE tx_sequence_number BETWEEN %s AND %s
    ),
    r AS (
        SELECT * FROM tx_recipients_cp WHERE tx_sequence_number BETWEEN %s AND %s
    )
    INSERT INTO tx_addresses (tx_sequence_number, address, checkpoint_sequence_number, rel)
    SELECT
        COALESCE(s.tx_sequence_number, r.tx_sequence_number) AS tx_sequence_number,
        COALESCE(s.address, r.address) AS address,
        COALESCE(s.checkpoint_sequence_number, r.checkpoint_sequence_number) AS checkpoint_sequence_number,
        CASE
            WHEN s.address IS NOT NULL AND r.address IS NULL THEN 0 -- Sender
            WHEN s.address IS NULL AND r.address IS NOT NULL THEN 1 -- Recipient
            ELSE 2 -- SenderAndRecipient
        END AS rel
    FROM s
    FULL OUTER JOIN r ON s.tx_sequence_number = r.tx_sequence_number AND s.checkpoint_sequence_number = r.checkpoint_sequence_number AND s.address = r.address
    ON CONFLICT (tx_sequence_number, rel, address, checkpoint_sequence_number) DO NOTHING;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (start, end, start, end))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error updating tx_addresses table: {e} given start: {start} and end: {end}")


def update_table_with_addr(conn, start, end, table, fields, primary_key):
    """
    Update the table with `address` and `rel` from `tx_addresses`.
    """

    query = f"""
    WITH txs AS (
        SELECT tx_sequence_number, checkpoint_sequence_number, address, rel
        FROM tx_addresses
        WHERE tx_sequence_number BETWEEN %s AND %s
    ),
    partial AS (
        SELECT {', '.join(fields)}, txs.tx_sequence_number, txs.checkpoint_sequence_number, txs.address, txs.rel
        FROM {table}
        JOIN txs USING (tx_sequence_number)
    )
    INSERT INTO {table}_cp (tx_sequence_number, checkpoint_sequence_number, address, rel, {', '.join(fields)})
    SELECT tx_sequence_number, checkpoint_sequence_number, address, rel, {', '.join(fields)}
    FROM partial
    ON CONFLICT ({primary_key}, address, tx_sequence_number) DO NOTHING;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (start, end))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error updating {table}_cp: {e}")

def start_threads(func, start, end, thread_ranges=[]):
    total_range = end - start + 1
    range_per_thread = math.ceil(total_range / max_connections)
    if not thread_ranges:
        print("Initializing range for each thread to process")
        thread_ranges = [(start + i * range_per_thread, min(start + (i + 1) * range_per_thread - 1, end)) for i in range(max_connections)]
    else:
        # extract min and max from thread_ranges
        start = min([start for start, _ in thread_ranges])
        end = max([end for _, end in thread_ranges])
        total_range = end - start + 1

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

def resume_from_log(log_file):
    thread_ranges = process_log(log_file)
    for start, end in thread_ranges:
        start_threads(setup_unpartitioned_table, start, end)

def process_log(log_file):
    pattern = r"root - INFO - Thread (\d+) shutdown at range (\d+)-(\d+)"
    thread_ranges = []
    with open(log_file) as f:
        for line in f:
            match = re.match(pattern, line)
            if match:
                _, start, end = match.groups()
                thread_ranges.append((int(start), int(end)))
    return thread_ranges


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--setup", help="Creates an unpartitioned table of columns `tx_sequence_number` and `checkpoint_sequence_number`", action="store_true")
    parser.add_argument("--start", help="Start of the range to process", type=int, default=0)
    parser.add_argument("--end", help="End of the range to process", type=int)
    parser.add_argument("--resume", help="Continue processing per a log file", action="store_true")
    parser.add_argument("--log", help="Log file to continue processing", type=str)
    parser.add_argument("--add-addresses", help="Add address (sender or recipient) to the table", action="store_true")
    parser.add_argument("--rel", help="Relation to update", type=str, default='sender')
    parser.add_argument("--add-cp", help="Add checkpoint_sequence_number to the table", action="store_true")
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

    if args.setup:
        start_threads(setup_unpartitioned_table, args.start, end_tx, thread_ranges)
    elif args.add_cp:
        if args.table == 'tx_senders':
            def update_table_wrapper(conn, start, end):
                update_address_with_cp(conn, start, end, 'sender')
            start_threads(update_table_wrapper, args.start, end_tx, thread_ranges)
        elif args.table == 'tx_recipients':
            def update_table_wrapper(conn, start, end):
                update_address_with_cp(conn, start, end, 'recipient')
            start_threads(update_table_wrapper, args.start, end_tx, thread_ranges)
    elif args.merge_addresses:
        start_threads(create_merged_address_table, args.start, end_tx, thread_ranges)
    elif args.add_addresses:
        if args.table == 'tx_calls':
            def update_table_wrapper(conn, start, end):
                fields = ['package', 'module', 'func']
                primary_key = 'package, module, func'
                update_table_with_addr(conn, start, end, 'tx_calls', fields, primary_key, args.rel)
            start_threads(update_table_wrapper, args.start, end_tx, thread_ranges)
