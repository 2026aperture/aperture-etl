"""
Edit this file to change which columns are processed and how partition keys
are generated. Adding a new column should only require changes here and in the
database tables, not in the Python code.

To add a new column:
1. Add it to SOURCE_COLUMNS below.
2. Add the column to the source and staging tables in the database.
3. (Optional) Add it to PARTITION_CONFIG["fields"] to include it in the key.

Partition fields can be either:
- A direct column listed in SOURCE_COLUMNS (e.g. "app_version"), or
- A "substring" field defined in PARTITION_CONFIG["substrings"], which uses the
  first or last N characters of ANY source column. Each substring field
  declares its "source" column, a "length", and a "mode" of "prefix" (leading
  characters, the default) or "suffix" (trailing characters).
"""

# Columns read from the source table and carried through to staging.
# This is the single source of truth for which columns flow through the ETL.
SOURCE_COLUMNS = [
    "inserted_time",
    "gsid",
    "raw_heatmap",
    "environment",
    "app_id",
    "app_version",
    "device_id",
    "region",
    "app_name",
    "platform",
    "os",
]

# Columns written to the staging table: the generated partition_key, the
# strategy that produced it, plus every source column.
STAGING_COLUMNS = ["partition_key", "strategy_id", *SOURCE_COLUMNS]

PARTITION_CONFIG = {
    "strategy_name": "substring_test",
    "fields": ["app_version", "region_prefix", "os_suffix"],
    # Named substring fields. Each maps to a source column, how many characters
    # to use ("length"), and whether to take them from the start ("prefix") or
    # the end ("suffix"). Reference the field name (the key) in "fields".
    # The source can be ANY column in SOURCE_COLUMNS, not just IDs.
    "substrings": {
        "region_prefix": {"source": "region", "length": 2, "mode": "prefix"},
        "os_suffix": {"source": "os", "length": 2, "mode": "suffix"}
    },
}

SOURCE_TABLE = "public.test_splunk_table"
STAGING_TABLE = "public.test_mit_staging_c"
PARTITION_GROUPS_TABLE = "public.test_partition_groups"
PARTITION_STRATEGIES_TABLE = "public.test_partition_strategies"
