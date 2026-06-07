# Partition Key Creation and Staging

## Overview

This process reads rows from `test_splunk_table`, creates a numeric `partition_key`, and inserts the processed rows into `test_mit_staging_c`.

## Tables

`test_splunk_table`

- Source-style table used for testing Splunk-like data.
- This table is read from, not modified.

`test_partition_groups`

- Stores each unique partition combination.
- Each unique `key_text` maps to one numeric `id`.
- That `id` is used as the `partition_key` in staging.

`test_mit_staging_c`

- Stores staged rows.
- Contains the original source fields plus `partition_key`.

## Partition Key Logic

Partition fields are controlled through Python config.

Supported fields include:

- `platform`
- `os`
- `region`
- `environment`
- `app_name`
- `app_version`
- `gsid_prefix`
- `device_id_prefix`

The config also controls how many characters of `gsid` or `device_id` are used for prefix-based keys.

For each row:

1. Build `key_text` from the configured fields.
2. Check the in-memory partition cache.
3. Reuse the existing partition key if found.
4. Insert a new row into `test_partition_groups` if not found.
5. Use the returned `id` as the row’s `partition_key`.

## Cache

Existing partition groups are loaded into a Python dictionary before processing starts.

This avoids querying Postgres for every row.

Postgres remains the source of truth. The cache is only used for faster lookup during the run.

## Staging Inserts

Staging rows are inserted in batches using PostgreSQL `COPY`.

Rows are collected into a batch, written to an in-memory CSV buffer, and bulk inserted into `test_mit_staging_c`.

## Benchmarking

With 10,000 test rows:

- Row-by-row / `executemany` staging inserts took about 20 seconds per 1,000 rows.
- PostgreSQL `COPY` inserted 10,000 staging rows in about 2 seconds.

The main bottleneck was staging inserts, not partition key creation or cache lookup.

## Current Flow

1. Load partition groups into cache.
2. Pull rows from `test_splunk_table`.
3. Build `key_text` for each row.
4. Get or create the numeric partition key.
5. Add the processed row to a staging batch.
6. Insert staging batches using PostgreSQL `COPY`.
7. Commit after each batch.

## Notes

Partition group inserts are still done one at a time when a new key is found.

Bulk inserting new partition groups can be added later if needed.
