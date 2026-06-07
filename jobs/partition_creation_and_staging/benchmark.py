import time
from typing import Any, Dict, List
import csv
from io import StringIO

from db import get_connection
from config import PARTITION_CONFIG, SOURCE_TABLE, STAGING_TABLE, PARTITION_GROUPS_TABLE
from partitioning import (
    build_partition_parts,
    build_key_text,
    insert_partition_group,
    load_partition_cache,
)


def format_seconds(seconds: float) -> str:
    return f"{seconds:.4f} seconds"


def fetch_source_rows(conn) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                id,
                inserted_time,
                gsid,
                raw_heatmap,
                environment,
                app_id,
                app_version,
                device_id,
                region,
                app_name,
                platform,
                os
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

    with conn.cursor(cursor_factory=None) as cur:
        cur.copy_expert(
            f"""
            COPY {STAGING_TABLE} (
                partition_key,
                inserted_time,
                gsid,
                raw_heatmap,
                environment,
                app_id,
                app_version,
                device_id,
                region,
                app_name,
                platform,
                os
            )
            FROM STDIN WITH CSV
            """,
            buffer,
        )

def insert_staging_row(conn, row: Dict[str, Any], partition_key: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {STAGING_TABLE} (
                partition_key,
                inserted_time,
                gsid,
                raw_heatmap,
                environment,
                app_id,
                app_version,
                device_id,
                region,
                app_name,
                platform,
                os
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                partition_key,
                row["inserted_time"],
                row["gsid"],
                row["raw_heatmap"],
                row["environment"],
                row["app_id"],
                row["app_version"],
                row["device_id"],
                row["region"],
                row["app_name"],
                row["platform"],
                row["os"],
            ),
        )


def clear_staging_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {STAGING_TABLE} RESTART IDENTITY;")


def clear_partition_groups(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {PARTITION_GROUPS_TABLE} RESTART IDENTITY CASCADE;")


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

    # 1. Load cache
    cache_start = time.perf_counter()
    partition_cache = load_partition_cache(conn)
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
            partition_key = insert_partition_group(conn, key_text, key_parts)
            partition_insert_end = time.perf_counter()

            partition_insert_total += partition_insert_end - partition_insert_start
            partition_cache[key_text] = partition_key
        else:
            cache_hits += 1

        # Insert staging row
        staging_batch.append(
            (
                partition_key,
                row["inserted_time"],
                row["gsid"],
                row["raw_heatmap"],
                row["environment"],
                row["app_id"],
                row["app_version"],
                row["device_id"],
                row["region"],
                row["app_name"],
                row["platform"],
                row["os"],
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

        choice = input("Enter 1, 2, or 3: ").strip()

        if choice == "1":
            benchmark_run(conn, clear_partitions=True, clear_staging=True)

        elif choice == "2":
            benchmark_run(conn, clear_partitions=False, clear_staging=True)

        elif choice == "3":
            print("\nFIRST RUN: partition groups will be empty")
            benchmark_run(conn, clear_partitions=True, clear_staging=True)

            print("\nSECOND RUN: partition groups already exist")
            benchmark_run(conn, clear_partitions=False, clear_staging=True)

        else:
            print("Invalid choice. Please run again and enter 1, 2, or 3.")


if __name__ == "__main__":
    main()