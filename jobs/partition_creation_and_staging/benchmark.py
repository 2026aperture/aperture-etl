import time
from typing import Any, Dict, List
import csv
from io import StringIO

from db import get_connection
from config import (
    PARTITION_CONFIG,
    SOURCE_TABLE,
    STAGING_TABLE,
    PARTITION_GROUPS_TABLE,
    SOURCE_COLUMNS,
    STAGING_COLUMNS,
)
from partitioning import (
    build_partition_parts,
    build_key_text,
    get_or_create_strategy,
    get_partition_key,
    insert_partition_group,
    load_partition_cache,
    validate_partition_config,
)
from staging import BATCH_SIZE, build_staging_row, run_etl


def format_seconds(seconds: float) -> str:
    return f"{seconds:.4f} seconds"


def fetch_source_rows(conn) -> List[Dict[str, Any]]:
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

def insert_staging_batch(conn, batch) -> None:
    if not batch:
        return

    buffer = StringIO()
    writer = csv.writer(buffer)

    for row in batch:
        writer.writerow(row)

    buffer.seek(0)

    columns = ", ".join(STAGING_COLUMNS)

    with conn.cursor(cursor_factory=None) as cur:
        cur.copy_expert(
            f"""
            COPY {STAGING_TABLE} ({columns})
            FROM STDIN WITH CSV
            """,
            buffer,
        )

def insert_staging_row(
    conn, row: Dict[str, Any], partition_key: int, strategy_id: int
) -> None:
    columns = ", ".join(STAGING_COLUMNS)
    placeholders = ", ".join(["%s"] * len(STAGING_COLUMNS))
    values = (
        partition_key,
        strategy_id,
        *(row[column] for column in SOURCE_COLUMNS),
    )

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {STAGING_TABLE} ({columns})
            VALUES ({placeholders});
            """,
            values,
        )


def clear_staging_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {STAGING_TABLE} RESTART IDENTITY;")


def clear_partition_groups(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {PARTITION_GROUPS_TABLE} RESTART IDENTITY CASCADE;")


def count_source_rows(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) AS n FROM {SOURCE_TABLE};")
        return cur.fetchone()["n"]


def verify_staging(conn, expected_rows: int, strategy_id: int) -> bool:
    """
    Confirm a staging load is correct: every source row landed, no NULL
    strategy_id, and every row is tagged with the strategy we just ran.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                count(*)                         AS total,
                count(strategy_id)               AS non_null_strategy,
                count(*) FILTER (
                    WHERE strategy_id = %s
                )                                AS matching_strategy
            FROM {STAGING_TABLE};
            """,
            (strategy_id,),
        )
        r = cur.fetchone()

    ok = (
        r["total"] == expected_rows
        and r["non_null_strategy"] == expected_rows
        and r["matching_strategy"] == expected_rows
    )

    status = "PASS" if ok else "FAIL"
    print(
        f"  [{status}] staging rows={r['total']} (expected {expected_rows}), "
        f"non-null strategy_id={r['non_null_strategy']}, "
        f"strategy_id=={strategy_id}: {r['matching_strategy']}"
    )

    return ok


def serial_run_etl(conn, strategy_id: int) -> Dict[str, Any]:
    """
    Serial baseline that mirrors staging.run_etl but inserts every batch inline
    on this one connection. Returns a stats dict with the same per-phase timing
    breakdown as the threaded path, measured over the same scope (cache load,
    source fetch, key assignment, inserts) so the two are directly comparable.

    Assumes partition groups are already warm (cache hits), so the timing
    reflects staging inserts, not new partition-group creation.
    """
    pipeline_start = time.perf_counter()

    cache_start = time.perf_counter()
    partition_cache = load_partition_cache(conn, strategy_id)
    cache_seconds = time.perf_counter() - cache_start

    fetch_start = time.perf_counter()
    rows = fetch_source_rows(conn)
    fetch_seconds = time.perf_counter() - fetch_start

    staging_batch = []
    key_assign_seconds = 0.0
    commit_seconds = 0.0
    insert_seconds = 0.0
    batches = 0

    def flush():
        nonlocal commit_seconds, insert_seconds, batches
        # Partition-group commit (mirrors the threaded producer commit).
        commit_start = time.perf_counter()
        conn.commit()
        commit_seconds += time.perf_counter() - commit_start
        # Staging COPY + commit (the serial insert).
        insert_start = time.perf_counter()
        insert_staging_batch(conn, staging_batch)
        conn.commit()
        insert_seconds += time.perf_counter() - insert_start
        batches += 1

    for row in rows:
        key_start = time.perf_counter()
        partition_key, _, _ = get_partition_key(
            conn=conn,
            row=row,
            config=PARTITION_CONFIG,
            partition_cache=partition_cache,
            strategy_id=strategy_id,
        )
        staging_batch.append(build_staging_row(row, partition_key, strategy_id))
        key_assign_seconds += time.perf_counter() - key_start

        if len(staging_batch) >= BATCH_SIZE:
            flush()
            staging_batch = []

    if staging_batch:
        flush()
        staging_batch = []

    return {
        "mode": "serial",
        "rows": len(rows),
        "batches": batches,
        "cache_seconds": cache_seconds,
        "fetch_seconds": fetch_seconds,
        "key_assign_seconds": key_assign_seconds,
        "commit_seconds": commit_seconds,
        "insert_seconds": insert_seconds,
        "pipeline_seconds": time.perf_counter() - pipeline_start,
    }


def print_breakdown(stats: Dict[str, Any]) -> None:
    """Print whichever timing phases are present in a stats dict."""
    label_for = {
        "cache_seconds": "Load partition cache",
        "fetch_seconds": "Fetch source rows",
        "key_assign_seconds": "Assign partition keys (main thread)",
        "commit_seconds": "Commit partition groups (main thread)",
        "enqueue_wait_seconds": "Producer blocked on full queue (backpressure)",
        "produce_seconds": "Producer loop total (main thread)",
        "drain_seconds": "Drain: wait for workers to finish",
        "insert_seconds": "Staging COPY inserts (serial)",
        "worker_insert_seconds": "Staging COPY inserts (sum across workers)",
        "pipeline_seconds": "PIPELINE TOTAL (fair comparison)",
    }

    order = [
        "cache_seconds",
        "fetch_seconds",
        "key_assign_seconds",
        "commit_seconds",
        "enqueue_wait_seconds",
        "produce_seconds",
        "drain_seconds",
        "insert_seconds",
        "worker_insert_seconds",
        "pipeline_seconds",
    ]

    print(f"  rows={stats['rows']} batches={stats['batches']}", end="")
    if "workers" in stats:
        print(f" workers={stats['workers']}", end="")
    print()

    for key in order:
        if key in stats:
            print(f"    {label_for[key]:<48} {format_seconds(stats[key])}")


def compare_serial_vs_threaded(conn) -> None:
    """
    Compare serial staging inserts against the threaded staging in run_etl.

    Both runs start with partition groups already warm, so the only difference
    being measured is the staging-insert strategy. Both are timed over the same
    scope (cache load -> fetch -> key assignment -> inserts) and compared on
    pipeline_seconds. Each run is verified for correctness before its timing is
    reported, and a per-phase breakdown is printed so you can see where the
    time actually goes.
    """
    print("\n==================================================")
    print("Serial vs threaded staging comparison")
    print("==================================================")

    validate_partition_config(PARTITION_CONFIG)

    strategy_id = get_or_create_strategy(conn, PARTITION_CONFIG)
    conn.commit()

    source_rows = count_source_rows(conn)
    print(f"Source rows: {source_rows}")

    if source_rows == 0:
        print("Source table is empty; nothing to benchmark.")
        return

    # Warm partition groups so neither run pays for new-group creation.
    print("\nWarming partition groups (threaded run_etl)...")
    clear_partition_groups(conn)
    clear_staging_table(conn)
    run_etl(conn)
    print("Warm-up done.")

    # --- Serial ---
    print("\n--- Serial staging ---")
    clear_staging_table(conn)
    serial_stats = serial_run_etl(conn, strategy_id)
    serial_ok = verify_staging(conn, source_rows, strategy_id)
    print_breakdown(serial_stats)

    # --- Threaded ---
    print("\n--- Threaded staging (run_etl) ---")
    clear_staging_table(conn)
    threaded_stats = run_etl(conn)
    threaded_ok = verify_staging(conn, source_rows, strategy_id)
    print_breakdown(threaded_stats)

    # --- Summary ---
    serial_pipeline = serial_stats["pipeline_seconds"]
    threaded_pipeline = threaded_stats["pipeline_seconds"]

    print("\nSummary (pipeline = cache + fetch + key assignment + inserts)")
    print("-------")
    print(f"Rows:     {source_rows}")
    print(f"Serial:   {format_seconds(serial_pipeline)}  ({'OK' if serial_ok else 'FAILED'})")
    print(f"Threaded: {format_seconds(threaded_pipeline)}  ({'OK' if threaded_ok else 'FAILED'})")

    if threaded_pipeline > 0:
        print(f"Speedup:  {serial_pipeline / threaded_pipeline:.2f}x")

    # Show how much of the pipeline is the part threading can't help (the
    # single-threaded fetch + key assignment), so the result is interpretable.
    non_insert = (
        threaded_stats["fetch_seconds"]
        + threaded_stats["cache_seconds"]
        + threaded_stats["key_assign_seconds"]
    )
    print(
        f"\nNote: fetch + cache + key assignment = {format_seconds(non_insert)} "
        f"of the threaded pipeline is single-threaded and cannot be sped up by "
        f"more workers."
    )

    if not (serial_ok and threaded_ok):
        print("\nWARNING: a correctness check FAILED -- see the [FAIL] line above.")


def benchmark_run(conn, clear_partitions: bool = False, clear_staging: bool = True) -> None:
    print("\n==============================")
    print("Starting benchmark run")
    print("==============================")

    if clear_staging:
        start = time.perf_counter()
        clear_staging_table(conn)
        end = time.perf_counter()
        print(f"Cleared staging table: {format_seconds(end - start)}")

    if clear_partitions:
        start = time.perf_counter()
        clear_partition_groups(conn)
        end = time.perf_counter()
        print(f"Cleared partition groups: {format_seconds(end - start)}")

    total_start = time.perf_counter()

    # 0. Resolve the strategy for the current config
    strategy_id = get_or_create_strategy(conn, PARTITION_CONFIG)

    # 1. Load cache
    cache_start = time.perf_counter()
    partition_cache = load_partition_cache(conn, strategy_id)
    cache_end = time.perf_counter()

    # 2. Pull source rows
    fetch_start = time.perf_counter()
    rows = fetch_source_rows(conn)
    fetch_end = time.perf_counter()

    # 3. Process rows
    key_build_total = 0.0
    cache_lookup_total = 0.0
    partition_insert_total = 0.0
    staging_insert_total = 0.0

    cache_hits = 0
    cache_misses = 0
    inserted_count = 0

    BATCH_SIZE = 1000
    staging_batch = []

    for index, row in enumerate(rows, start=1):
        # Build key
        key_build_start = time.perf_counter()
        key_parts = build_partition_parts(row, PARTITION_CONFIG)
        key_text = build_key_text(key_parts)
        key_build_end = time.perf_counter()
        key_build_total += key_build_end - key_build_start

        # Cache lookup
        lookup_start = time.perf_counter()
        partition_key = partition_cache.get(key_text)
        lookup_end = time.perf_counter()
        cache_lookup_total += lookup_end - lookup_start

        # Insert partition group if missing
        if partition_key is None:
            cache_misses += 1

            partition_insert_start = time.perf_counter()
            partition_key = insert_partition_group(
                conn, strategy_id, key_text, key_parts
            )
            partition_insert_end = time.perf_counter()

            partition_insert_total += partition_insert_end - partition_insert_start
            partition_cache[key_text] = partition_key
        else:
            cache_hits += 1

        # Insert staging row
        staging_batch.append(
            (
                partition_key,
                strategy_id,
                *(row[column] for column in SOURCE_COLUMNS),
            )
        )

        if len(staging_batch) >= BATCH_SIZE:
            staging_insert_start = time.perf_counter()
            
            insert_staging_batch(conn, staging_batch)
            conn.commit()
            
            staging_insert_end = time.perf_counter()
            staging_insert_total += staging_insert_end - staging_insert_start

            inserted_count += len(staging_batch)
            
            staging_batch.clear()

        if index % 1000 == 0:
            elapsed = time.perf_counter() - total_start
            print(
                f"Processed {index}/{len(rows)} rows | "
                f"inserted={inserted_count} | "
                f"cache hits={cache_hits} | "
                f"cache misses={cache_misses} | "
                f"elapsed={elapsed:.2f}s"
            )

    if staging_batch:
        insert_staging_batch(conn, staging_batch)
        conn.commit()
        staging_batch.clear()

    total_end = time.perf_counter()

    print("\nBenchmark Results")
    print("-----------------")
    print(f"Rows processed: {len(rows)}")
    print(f"Cache size after run: {len(partition_cache)}")
    print(f"Cache hits: {cache_hits}")
    print(f"Cache misses / new partition inserts: {cache_misses}")

    print("\nTiming")
    print("------")
    print(f"Load partition cache: {format_seconds(cache_end - cache_start)}")
    print(f"Pull source rows: {format_seconds(fetch_end - fetch_start)}")
    print(f"Build partition keys total: {format_seconds(key_build_total)}")
    print(f"Cache lookup total: {format_seconds(cache_lookup_total)}")
    print(f"Insert new partition groups total: {format_seconds(partition_insert_total)}")
    print(f"Insert staging rows total: {format_seconds(staging_insert_total)}")
    print(f"Total benchmark time: {format_seconds(total_end - total_start)}")

    if rows:
        print("\nPer-row averages")
        print("----------------")
        print(f"Build partition key avg: {format_seconds(key_build_total / len(rows))}")
        print(f"Cache lookup avg: {format_seconds(cache_lookup_total / len(rows))}")
        print(f"Staging insert avg: {format_seconds(staging_insert_total / len(rows))}")

        if cache_misses > 0:
            print(
                "New partition insert avg: "
                f"{format_seconds(partition_insert_total / cache_misses)}"
            )


def main():
    with get_connection() as conn:
        print("Choose benchmark mode:")
        print("1. First run: clear partition groups, then insert new keys")
        print("2. Second run: keep existing partition groups, test cache hits")
        print("3. Run both back-to-back")
        print("4. Compare serial vs threaded staging (+ correctness check)")

        choice = input("Enter 1, 2, 3, or 4: ").strip()

        if choice == "1":
            benchmark_run(conn, clear_partitions=True, clear_staging=True)

        elif choice == "2":
            benchmark_run(conn, clear_partitions=False, clear_staging=True)

        elif choice == "3":
            print("\nFIRST RUN: partition groups will be empty")
            benchmark_run(conn, clear_partitions=True, clear_staging=True)

            print("\nSECOND RUN: partition groups already exist")
            benchmark_run(conn, clear_partitions=False, clear_staging=True)

        elif choice == "4":
            compare_serial_vs_threaded(conn)

        else:
            print("Invalid choice. Please run again and enter 1, 2, 3, or 4.")


if __name__ == "__main__":
    main()