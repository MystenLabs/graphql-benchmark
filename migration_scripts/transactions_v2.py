import signal
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
import psycopg2.extras
import psycopg2.pool
from tqdm import tqdm
import argparse
import logging
from dotenv import load_dotenv

def signal_handler(signum, frame):
    global shutdown_requested
    shutdown_requested = True
    print("shutting down")

def cancel_all_connections():
    global connection_pool
    connection_pool.closeall()


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


def create_main_table_statement():
    return """
    CREATE TABLE IF NOT EXISTS transactions_v2 (
        tx_sequence_number BIGINT NOT NULL,
        transaction_digest BYTEA NOT NULL,
        raw_transaction BYTEA NOT NULL,
        raw_effects BYTEA NOT NULL,
        checkpoint_sequence_number BIGINT NOT NULL,
        timestamp_ms BIGINT NOT NULL,
        object_changes BYTEA[] NOT NULL,
        balance_changes BYTEA[] NOT NULL,
        events BYTEA[] NOT NULL,
        transaction_kind SMALLINT NOT NULL,
        success_command_count SMALLINT NOT NULL
    );
    """

def create_partition_statement(n):
    """
    Statement to create a table for a specific partition of transactions. Note that we do not attach
    this table as a partition to the main table, so we can facilitate writes.
    """
    return f"""
    CREATE TABLE IF NOT EXISTS transactions_v2_{n} (
        tx_sequence_number BIGINT NOT NULL,
        transaction_digest BYTEA NOT NULL,
        raw_transaction BYTEA NOT NULL,
        raw_effects BYTEA NOT NULL,
        checkpoint_sequence_number BIGINT NOT NULL,
        timestamp_ms BIGINT NOT NULL,
        object_changes BYTEA[] NOT NULL,
        balance_changes BYTEA[] NOT NULL,
        events BYTEA[] NOT NULL,
        transaction_kind SMALLINT NOT NULL,
        success_command_count SMALLINT NOT NULL
    );
    """

def create_all_tables(conn, partition_count):
    with conn.cursor() as cur:
        cur.execute(create_main_table_statement())
        for i in range(partition_count):
            cur.execute(create_partition_statement(i))
        conn.commit()

def process_range(pbar, thread_id, start, end, chunk_size = 10000):
    """Process start and end range in chunks of chunk_size. Assumes that the original start and end all fall under one partition"""

    to_partition = calculate_partition(start)

    conn = connection_pool.getconn()
    completed = 0
    try:
        # min_checkpoint = cp_for_tx(conn, start)
        # max_checkpoint = cp_for_tx(conn, end)
        for idx in range(start, end + 1, chunk_size):
            if shutdown_requested:
                conn.cancel()
                log_shutdown(thread_id, idx, end)
                return
            end_of_chunk = min(idx + chunk_size - 1, end)
            migrate_partition(thread_id, conn, idx, end_of_chunk, to_partition)
            pbar.update(chunk_size)
            completed += chunk_size
    except Exception as e:
        log_error(thread_id, start + completed, end, str(e))
        raise
    finally:
        connection_pool.putconn(conn)

def calculate_partition(tx_sequence_number, partition_size=10000000):
    """
    Calculate the partition number for a given transaction sequence number
    based on a specified size of each partition. 0-indexed.

    Parameters:
    - tx_sequence_number (int): The transaction sequence number.
    - partition_size (int): The number of transactions each partition holds.

    Returns:
    - int: Partition number.
    """
    return tx_sequence_number // partition_size

def migrate_partition(thread_id, conn, start_tx, end_tx, to_partition):
    try:
        with conn.cursor() as cur:
            min_checkpoint = cp_for_tx(cur, start_tx)
            max_checkpoint = cp_for_tx(cur, end_tx)
            query = f"""
                insert into transactions_v2_{to_partition} select * from transactions where tx_sequence_number between {start_tx} and {end_tx} and checkpoint_sequence_number between {min_checkpoint} and {max_checkpoint}
            """
            cur.execute(query)
            conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print(e.pgcode)
        print(e.pgerror)
        print(e.diag.severity)
        print(e.diag.message_primary)
    except Exception as e:
        print(f"Error migrating partition {to_partition}: {e}")
        log_error(thread_id, start_tx, end_tx, f"Error migrating partition {to_partition}: {e}")

def cp_for_tx(cur, tx_sequence_number):
    query = f"""
    select checkpoint_sequence_number from transactions where tx_sequence_number = {tx_sequence_number}
    """
    cur.execute(query)
    return cur.fetchone()[0]

def start_threads(start, end, connections, thread_ranges=[]):
    total_partitions = 131
    total_range = end - start + 1
    # range_per_thread = math.ceil(total_range / connections)
    range_per_partition = 10000000
    if not thread_ranges:
        for partition in range(total_partitions):
            partition_start = start + partition * range_per_partition
            partition_end = min(partition_start + range_per_partition - 1, end)
            thread_ranges.append((partition_start, partition_end))
    else:
        total_range = 0
        for start, end in thread_ranges:
            total_range += end - start + 1

    # fan-out
    with tqdm(total=total_range, desc="Migrating transactions") as pbar:
        with ThreadPoolExecutor(max_workers=connections) as executor:

            futures = [executor.submit(process_range, pbar, i, thread_start, thread_end) for i, (thread_start, thread_end) in enumerate(thread_ranges)]

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

def turn_autovacuum_off(conn, partitions):
    with conn.cursor() as cur:
        for i in range(partitions):
            cur.execute(f"ALTER TABLE transactions_v2_{i} SET (autovacuum_enabled = false);")
        conn.commit()

def drop_all_tables(conn, partitions):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS transactions_v2")
        for i in range(partitions):
            cur.execute(f"DROP TABLE IF EXISTS transactions_v2_{i}")
        conn.commit()


def process_partition(partition_id):
    print(f"Processing partition {partition_id}")
    conn = connection_pool.getconn()
    try:
        turn_autovacuum_on(conn, partition_id)
        add_constraints(conn, partition_id)
        attach_partitions(conn, partition_id)
    finally:
        print(f"Finished processing partition {partition_id}")
        connection_pool.putconn(conn)

def turn_autovacuum_on(conn, partition_id):
    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE transactions_v2_{partition_id} SET (autovacuum_enabled = true);")
        cur.execute(f"VACUUM ANALYZE transactions_v2_{partition_id};")
        conn.commit()

def add_constraints(conn, partition_id):
    with conn.cursor() as cur:
        cur.execute(f"""
            ALTER TABLE transactions_v2_{partition_id} ADD PRIMARY KEY (tx_sequence_number),
            ADD CONSTRAINT transactions_v2_{partition_id}_partition_check CHECK (tx_sequence_number >= {partition_id * 10000000} AND tx_sequence_number < {(partition_id + 1) * 10000000});""")
        conn.commit()

def attach_partitions(conn, partition_id):
    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE transactions_v2 ATTACH PARTITION transactions_v2_{partition_id} FOR VALUES FROM ({partition_id * 10000000}) TO ({(partition_id + 1) * 10000000});")
        cur.execute(f"ALTER TABLE transactions_v2_{partition_id} DROP CONSTRAINT transactions_v2_{partition_id}_partition_check;")
        conn.commit()

if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--bulk-load", action="store_true", required=False, help="bulk load transactions from a file")
    parser.add_argument("--reset-db", action="store_true", required=False, help="reset the database", default=False)
    parser.add_argument("--attach-partitions", action="store_true", required=False, help="attach partitions to main table")
    parser.add_argument("--port", required=False, help="port to connect to")
    parser.add_argument("--max-connections", required=False, default=131, help="max connections to use")
    args = parser.parse_args()

    conn_params = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': args.port if args.port else os.getenv('DB_PORT')
    }

    connection_pool = psycopg2.pool.ThreadedConnectionPool(1, args.max_connections, **conn_params)

    # currently we have 1300225138 transactions. partition into tables with 10m transactions each so
    # we'll have 130 tables
    max_partitions = 131
    if args.bulk_load:
        conn = connection_pool.getconn()
        if args.reset_db:
            drop_all_tables(conn, max_partitions)
        create_all_tables(conn, max_partitions)
        turn_autovacuum_off(conn, max_partitions)
        connection_pool.putconn(conn)

        start_threads(0, 1300225138, args.max_connections)
    elif args.attach_partitions:
        with ThreadPoolExecutor(max_workers=args.max_connections) as executor:
            futures = [executor.submit(process_partition, i) for i in range(args.max_connections)]

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error during database update operation: {e}")
