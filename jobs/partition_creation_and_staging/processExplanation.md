# Partition Key Creation and Staging

## Overview

This process reads rows from `test_splunk_table`, creates a numeric `partition_key`, and inserts the processed rows into `test_mit_staging_c`.

## Tables

`test_splunk_table`

- Source-style table used for testing Splunk-like data.
- This table is read from, not modified.

`test_partition_strategies`

- Stores one row per distinct partition configuration.
- `config_hash` is a stable fingerprint of `strategy_name` + `fields` +
  the referenced `substrings`, computed in Python.
- The ETL upserts on `config_hash`, so the same config reuses the same
  strategy row and changing the config mints a new one.
- This is how we know which strategy created any given partition group.

`test_partition_groups`

- Stores each unique partition combination.
- Each unique `(strategy_id, key_text)` maps to one numeric `id`.
- `strategy_id` references `test_partition_strategies`.
- That `id` is used as the `partition_key` in staging.

`test_mit_staging_c`

- Stores staged rows.
- Contains the original source fields plus `partition_key` and `strategy_id`.
- `strategy_id` is denormalized lineage (also reachable through the partition
  group) so staged data can be filtered by strategy without a join.

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

### Threaded staging inserts

The COPY inserts are I/O-bound, so they run on a pool of worker threads
(`NUM_WORKERS`, default 4). The main thread stays single-threaded for partition
key assignment (it mutates the shared cache and inserts partition groups) and
acts as the producer:

- Each worker owns its own database connection. A psycopg2 connection cannot be
  shared across threads, so connections are never shared.
- Finished batches are handed to workers through a bounded `queue.Queue`
  (`QUEUE_MAXSIZE`). When every worker is busy and the queue is full, the main
  thread blocks on `put` until a worker frees up. That is the backpressure
  mechanism.
- The strategy row is committed on the main connection before any worker runs,
  so the staging `strategy_id` foreign key is satisfied. New partition groups
  are committed on the main connection before the staging rows that reference
  them are dispatched.
- Each worker commits its own batch independently, so batches commit out of
  order and there is no single overarching transaction. On a worker failure the
  first error is recorded, a stop event halts the producer and the other
  workers, the queue drains, and `run_etl` re-raises the original error.

## Benchmarking

With 10,000 test rows:

- Row-by-row / `executemany` staging inserts took about 20 seconds per 1,000 rows.
- PostgreSQL `COPY` inserted 10,000 staging rows in about 2 seconds.

The main bottleneck was staging inserts, not partition key creation or cache lookup.

## Current Flow

1. Resolve the strategy for the current config (get or create by `config_hash`).
2. Load that strategy's partition groups into cache.
3. Stream rows from `test_splunk_table` through a server-side cursor on a
   dedicated read connection, so the fetch overlaps key assignment and inserts.
4. Build `key_text` for each row.
5. Get or create the numeric partition key under the strategy.
6. Add the processed row (with `partition_key` and `strategy_id`) to a staging batch.
7. Hand each full batch to a worker thread, which inserts it via PostgreSQL `COPY`.
8. Each worker commits its own batch on its own connection.

## Notes

Partition group inserts are still done one at a time when a new key is found.

Bulk inserting new partition groups can be added later if needed.
