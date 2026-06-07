"""
Edit this file to change how partition keys are generated.

Allowed fields:
- gsid_prefix
- device_id_prefix
- platform
- environment
- app_id
- app_version
- region
- app_name
- os
"""

PARTITION_CONFIG = {
    "strategy_name": "app_version_test",
    "fields": ["app_version"],
    "prefix_lengths": {
        "gsid": 0,
        "device_id": 0,
    },
}

SOURCE_TABLE = "public.test_splunk_table"
STAGING_TABLE = "public.test_mit_staging_c"
PARTITION_GROUPS_TABLE = "public.test_partition_groups"
