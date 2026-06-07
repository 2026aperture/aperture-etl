from typing import Any, Dict, List
from io import StringIO
import csv

from config import PARTITION_CONFIG, SOURCE_TABLE, STAGING_TABLE
from partitioning import (
    get_partition_key,
    load_partition_cache,
)


BATCH_SIZE = 1000


def fetch_source_rows(conn) -> List[Dict[str, Any]]:
    """
    Read rows from the source table.
    """
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
    """
    Bulk insert processed rows into the staging table using PostgreSQL COPY.
    """
    if not batch:
        return

    buffer = StringIO()
    writer = csv.writer(buffer)

    for row in batch:
        writer.writerow(row)

    buffer.seek(0)

    with conn.cursor() as cur:
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


def build_staging_row(row: Dict[str, Any], partition_key: int) -> tuple:
    """
    Convert a source row into the tuple format expected by COPY.
    """
    return (
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


def run_etl(conn) -> None:
    """
    Main ETL process.

    1. Loads the partition group cache
    2. Reads source rows
    3. Creates partition keys
    4. Batches staging rows
    5. Inserts staging rows using COPY
    """

    partition_cache = load_partition_cache(conn)
    source_rows = fetch_source_rows(conn)

    print(f"Loaded {len(partition_cache)} cached partition groups")
    print(f"Fetched {len(source_rows)} source rows")

    inserted_count = 0
    staging_batch = []

    for index, row in enumerate(source_rows, start=1):
        partition_key, key_text, key_parts = get_partition_key(
            conn=conn,
            row=row,
            config=PARTITION_CONFIG,
            partition_cache=partition_cache,
        )

        staging_batch.append(build_staging_row(row, partition_key))

        if len(staging_batch) >= BATCH_SIZE:
            insert_staging_batch(conn, staging_batch)
            conn.commit()

            inserted_count += len(staging_batch)
            staging_batch.clear()

            print(f"Inserted {inserted_count}/{len(source_rows)} rows into staging")

    if staging_batch:
        insert_staging_batch(conn, staging_batch)
        conn.commit()

        inserted_count += len(staging_batch)
        staging_batch.clear()

    print(f"Done. Inserted {inserted_count} rows into staging.")