import csv
import queue
import threading
import time
from io import StringIO
from typing import Any, Dict, List

from config import (
    PARTITION_CONFIG,
    SOURCE_TABLE,
    STAGING_TABLE,
    SOURCE_COLUMNS,
    STAGING_COLUMNS,
)
from db import get_connection
from partitioning import (
    get_or_create_strategy,
    get_partition_key,
    load_partition_cache,
    validate_partition_config,
)


BATCH_SIZE = 10000

# Number of worker threads that run staging COPY inserts in parallel. Each
# worker owns its own database connection, because a psycopg2 connection cannot
# be used by more than one thread at a time.
NUM_WORKERS = 8

# How many full batches may wait in the queue before the producer (partition
# key assignment) blocks. With every worker busy and the queue full, the main
# thread stops until a worker frees up -- this is the backpressure mechanism.
QUEUE_MAXSIZE = NUM_WORKERS

# Rows fetched per round-trip from the server-side source cursor. Larger means
# fewer round-trips but more memory held per fetch.
FETCH_ITERSIZE = 50000

# Sentinel placed on the queue to tell a worker to shut down.
_SHUTDOWN = object()

# Sentinel returned by next() when the source stream is exhausted.
_NO_MORE_ROWS = object()


def fetch_source_rows(conn) -> List[Dict[str, Any]]:
    """
    Read rows from the source table.

    Columns are driven by SOURCE_COLUMNS in config, plus `id` for ordering.
    """
    columns = ", ".join(["id", *SOURCE_COLUMNS])

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT {columns}
            FROM {SOURCE_TABLE}
            ORDER BY id;
            """
        )

        return cur.fetchall()


def stream_source_rows(conn, itersize: int = FETCH_ITERSIZE):
    """
    Stream rows from the source table through a server-side (named) cursor.

    Yields rows lazily so the caller can start assigning partition keys and
    inserting while later rows are still being fetched, instead of materializing
    all rows up front. `itersize` controls how many rows are pulled per
    server round-trip.

    The connection passed here must be dedicated to this read: a named cursor is
    only valid within its transaction, so the connection must not be committed
    while the stream is being consumed.
    """
    columns = ", ".join(["id", *SOURCE_COLUMNS])

    cur = conn.cursor(name="source_stream")
    cur.itersize = itersize

    try:
        cur.execute(
            f"""
            SELECT {columns}
            FROM {SOURCE_TABLE}
            ORDER BY id;
            """
        )

        for row in cur:
            yield row
    finally:
        cur.close()


def insert_staging_batch(conn, batch) -> None:
    """
    Bulk insert processed rows into the staging table using PostgreSQL COPY.

    Columns are driven by STAGING_COLUMNS in config.
    """
    if not batch:
        return

    buffer = StringIO()
    writer = csv.writer(buffer)

    for row in batch:
        writer.writerow(row)

    buffer.seek(0)

    columns = ", ".join(STAGING_COLUMNS)

    with conn.cursor() as cur:
        cur.copy_expert(
            f"""
            COPY {STAGING_TABLE} ({columns})
            FROM STDIN WITH CSV
            """,
            buffer,
        )


def build_staging_row(
    row: Dict[str, Any],
    partition_key: int,
    strategy_id: int,
) -> tuple:
    """
    Convert a source row into the tuple format expected by COPY.

    The order must match STAGING_COLUMNS: partition_key, strategy_id, then
    every source column in SOURCE_COLUMNS order.
    """
    return (
        partition_key,
        strategy_id,
        *(row[column] for column in SOURCE_COLUMNS),
    )


def _staging_worker(
    work_queue: "queue.Queue",
    stop_event: threading.Event,
    error_box: Dict[str, Any],
    counter: Dict[str, Any],
) -> None:
    """
    Pull staging batches off the queue and COPY each one into the staging table.

    Each worker owns its own connection for the life of the run. It keeps
    running until it receives the shutdown sentinel. If a batch fails, it
    records the first error, sets the stop event so the producer and the other
    workers wind down, and then just drains the remaining queue items (calling
    task_done on each) so nothing blocks.
    """
    conn = get_connection()

    try:
        while True:
            batch = work_queue.get()

            try:
                if batch is _SHUTDOWN:
                    return

                # Another worker already failed: don't do more work, just let
                # the queue drain so the producer's sentinels can flush through.
                if stop_event.is_set():
                    continue

                insert_start = time.perf_counter()
                insert_staging_batch(conn, batch)
                conn.commit()
                insert_seconds = time.perf_counter() - insert_start

                with counter["lock"]:
                    counter["rows"] += len(batch)
                    counter["batches"] += 1
                    counter["insert_seconds"] += insert_seconds
                    total = counter["rows"]

                print(f"[{threading.current_thread().name}] "
                      f"committed {len(batch)} rows (total {total})")

            except Exception as exc:  # surfaced to the main thread below
                conn.rollback()

                if error_box["error"] is None:
                    error_box["error"] = exc

                stop_event.set()

            finally:
                work_queue.task_done()
    finally:
        conn.close()


def _enqueue(work_queue: "queue.Queue", batch, stop_event: threading.Event) -> float:
    """
    Put a batch on the queue, blocking the producer when the queue is full.

    Polls with a timeout instead of blocking forever so that, if the workers
    have stopped because of an error, the producer notices and bails out rather
    than deadlocking.

    Returns the seconds spent waiting for queue space. A large total across the
    run means the producer is faster than the workers can insert -- i.e. the
    pipeline is insert-bound.
    """
    start = time.perf_counter()

    while not stop_event.is_set():
        try:
            work_queue.put(batch, timeout=0.5)
            break
        except queue.Full:
            continue

    return time.perf_counter() - start


def run_etl(conn) -> Dict[str, Any]:
    """
    Main ETL process.

    1. Resolves the strategy for the current config (and commits it, so the
       worker connections can satisfy the staging strategy_id foreign key).
    2. Loads the partition group cache for that strategy.
    3. Reads source rows.
    4. Starts a pool of worker threads that run staging COPY inserts.
    5. On the main thread, assigns partition keys and builds staging batches.
       Partition-key assignment stays single-threaded because it mutates the
       shared cache and inserts partition groups.
    6. Hands each finished batch to a worker. New partition groups are committed
       on the main connection BEFORE the staging rows that reference them are
       dispatched.

    Returns a stats dict with a per-phase timing breakdown so the work can be
    profiled (see benchmark.py).
    """

    total_start = time.perf_counter()

    validate_partition_config(PARTITION_CONFIG)

    strategy_start = time.perf_counter()
    strategy_id = get_or_create_strategy(conn, PARTITION_CONFIG)
    conn.commit()  # make the strategy visible to the worker connections
    strategy_seconds = time.perf_counter() - strategy_start

    # The pipeline (everything we compare against serial) starts here.
    pipeline_start = time.perf_counter()

    cache_start = time.perf_counter()
    partition_cache = load_partition_cache(conn, strategy_id)
    cache_seconds = time.perf_counter() - cache_start

    # Source rows are streamed on a dedicated connection so the fetch overlaps
    # with key assignment and the worker inserts, instead of being a serial
    # prefix. A named cursor cannot survive a commit, so it gets its own
    # connection that is never committed -- the main `conn` keeps handling
    # partition-group inserts and commits.
    source_conn = get_connection()
    source_rows = stream_source_rows(source_conn)

    print(f"Using strategy id {strategy_id} ({PARTITION_CONFIG['strategy_name']})")
    print(f"Loaded {len(partition_cache)} cached partition groups")
    print(f"Streaming source rows ({FETCH_ITERSIZE} per fetch)")
    print(f"Starting {NUM_WORKERS} staging workers")

    work_queue: "queue.Queue" = queue.Queue(maxsize=QUEUE_MAXSIZE)
    stop_event = threading.Event()
    error_box: Dict[str, Any] = {"error": None}
    counter: Dict[str, Any] = {
        "rows": 0,
        "batches": 0,
        "insert_seconds": 0.0,
        "lock": threading.Lock(),
    }

    workers = [
        threading.Thread(
            target=_staging_worker,
            args=(work_queue, stop_event, error_box, counter),
            name=f"staging-worker-{i + 1}",
            daemon=True,
        )
        for i in range(NUM_WORKERS)
    ]

    for worker in workers:
        worker.start()

    staging_batch = []

    # Producer-side timing.
    fetch_seconds = 0.0
    key_assign_seconds = 0.0
    commit_seconds = 0.0
    enqueue_wait_seconds = 0.0

    row_iter = iter(source_rows)
    produce_start = time.perf_counter()

    try:
        while not stop_event.is_set():
            # Pull the next row from the server-side stream. This is where the
            # fetch cost lands now -- spread across the loop and overlapping the
            # worker inserts, rather than a serial prefix.
            fetch_start = time.perf_counter()
            row = next(row_iter, _NO_MORE_ROWS)
            fetch_seconds += time.perf_counter() - fetch_start

            if row is _NO_MORE_ROWS:
                break

            key_start = time.perf_counter()
            partition_key, key_text, key_parts = get_partition_key(
                conn=conn,
                row=row,
                config=PARTITION_CONFIG,
                partition_cache=partition_cache,
                strategy_id=strategy_id,
            )
            staging_batch.append(build_staging_row(row, partition_key, strategy_id))
            key_assign_seconds += time.perf_counter() - key_start

            if len(staging_batch) >= BATCH_SIZE:
                # Commit new partition groups before the staging rows that
                # reference them get written by a worker on another connection.
                commit_start = time.perf_counter()
                conn.commit()
                commit_seconds += time.perf_counter() - commit_start

                enqueue_wait_seconds += _enqueue(work_queue, staging_batch, stop_event)
                staging_batch = []

        # Final partial batch.
        if staging_batch and not stop_event.is_set():
            commit_start = time.perf_counter()
            conn.commit()
            commit_seconds += time.perf_counter() - commit_start

            enqueue_wait_seconds += _enqueue(work_queue, staging_batch, stop_event)
            staging_batch = []

        produce_seconds = time.perf_counter() - produce_start
    finally:
        # Tell every worker to stop, then wait for them to finish in-flight work.
        drain_start = time.perf_counter()

        for _ in workers:
            work_queue.put(_SHUTDOWN)

        for worker in workers:
            worker.join()

        drain_seconds = time.perf_counter() - drain_start

        # Close the stream (and its server cursor) before its connection, so an
        # early break does not leave the cursor dangling. The read connection is
        # never committed.
        row_iter.close()
        source_conn.close()

    if error_box["error"] is not None:
        raise RuntimeError(
            "A staging worker failed; aborting ETL."
        ) from error_box["error"]

    pipeline_seconds = time.perf_counter() - pipeline_start
    total_seconds = time.perf_counter() - total_start

    print(f"Done. Inserted {counter['rows']} rows into staging.")

    return {
        "mode": "threaded",
        "rows": counter["rows"],
        "batches": counter["batches"],
        "workers": NUM_WORKERS,
        "strategy_seconds": strategy_seconds,
        "cache_seconds": cache_seconds,
        "fetch_seconds": fetch_seconds,
        "key_assign_seconds": key_assign_seconds,
        "commit_seconds": commit_seconds,
        "enqueue_wait_seconds": enqueue_wait_seconds,
        "produce_seconds": produce_seconds,
        "drain_seconds": drain_seconds,
        "worker_insert_seconds": counter["insert_seconds"],
        "pipeline_seconds": pipeline_seconds,
        "total_seconds": total_seconds,
    }