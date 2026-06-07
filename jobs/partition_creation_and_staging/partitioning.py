import json
from typing import Any, Dict, Tuple

from config import PARTITION_GROUPS_TABLE


ALLOWED_PARTITION_FIELDS = {
    "gsid_prefix",
    "device_id_prefix",
    "platform",
    "environment",
    "app_id",
    "app_version",
    "region",
    "app_name",
    "os",
}


def normalize(value: Any) -> str:
    """
    Normalize values so that equivalent values map to the same partition key.

    Examples:
    - " Android " -> "android"
    - "United States" -> "united-states"
    - None -> "unknown"
    """
    if value is None or value == "":
        return "unknown"

    return (
        str(value)
        .strip()
        .lower()
        .replace(" ", "-")
        .replace("/", "-")
    )


def get_prefix(value: Any, length: int | None) -> str | None:
    """
    Return the first `length` characters of an ID.

    If length is 0 or None, this returns None.
    That means the prefix should not be available unless explicitly requested.
    """
    if not length or length <= 0:
        return None

    if value is None or value == "":
        return "unknown"

    return normalize(str(value)[:length])


def validate_partition_config(config: Dict[str, Any]) -> None:
    """
    Validate the config before processing rows.
    """

    fields = config.get("fields")

    if not isinstance(fields, list) or not fields:
        raise ValueError("PARTITION_CONFIG must include a non-empty fields list.")

    unsupported_fields = set(fields) - ALLOWED_PARTITION_FIELDS

    if unsupported_fields:
        raise ValueError(
            f"Unsupported partition fields: {sorted(unsupported_fields)}. "
            f"Allowed fields are: {sorted(ALLOWED_PARTITION_FIELDS)}"
        )

    prefix_lengths = config.get("prefix_lengths", {})

    for field in fields:
        if field == "gsid_prefix" and prefix_lengths.get("gsid", 0) <= 0:
            raise ValueError(
                "fields includes 'gsid_prefix', but prefix_lengths['gsid'] "
                "is missing or <= 0."
            )

        if field == "device_id_prefix" and prefix_lengths.get("device_id", 0) <= 0:
            raise ValueError(
                "fields includes 'device_id_prefix', but prefix_lengths['device_id'] "
                "is missing or <= 0."
            )


def build_partition_parts(row: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, str]:
    """
    Build the fields requested for partitioning, applying normalization and prefixing as needed.
    """
    prefix_lengths = config.get("prefix_lengths", {})

    computed_values = {
        "gsid_prefix": get_prefix(row.get("gsid"), prefix_lengths.get("gsid", 0)),
        "device_id_prefix": get_prefix(
            row.get("device_id"),
            prefix_lengths.get("device_id", 0),
        ),
        "platform": normalize(row.get("platform")),
        "environment": normalize(row.get("environment")),
        "app_id": normalize(row.get("app_id")),
        "app_version": normalize(row.get("app_version")),
        "region": normalize(row.get("region")),
        "app_name": normalize(row.get("app_name")),
        "os": normalize(row.get("os")),
    }

    key_parts = {}

    for field in config["fields"]:
        value = computed_values[field]

        if value is None:
            raise ValueError(
                f"{field} was requested, but the prefix length for it was not set."
            )

        key_parts[field] = value

    return key_parts


def build_key_text(key_parts: Dict[str, str]) -> str:
    """
    Build a stable text representation of the key parts.

    Sorting makes equivalent dictionaries generate the same key_text,
    even if their field order differs.
    """
    return "/".join(
        f"{field}={key_parts[field]}"
        for field in sorted(key_parts.keys())
    )


def load_partition_cache(conn) -> Dict[str, int]:
    """
    Load existing partition key mappings into a Python dictionary.

    This is the fast hashmap:
    {
        "gsid_prefix=zh/platform=android": 17
    }
    """
    cache = {}

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, key_text
            FROM {PARTITION_GROUPS_TABLE};
            """,
        )

        for row in cur.fetchall():
            cache[row["key_text"]] = row["id"]

    return cache


def insert_partition_group(
    conn,
    key_text: str,
    key_parts: Dict[str, str],
) -> int:
    """
    Insert a new partition group if needed and return the numeric partition key.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {PARTITION_GROUPS_TABLE} (
                key_text,
                key_parts
            )
            VALUES (%s, %s::jsonb)
            ON CONFLICT (key_text)
            DO UPDATE SET key_parts = EXCLUDED.key_parts
            RETURNING id;
            """,
            (
                key_text,
                json.dumps(key_parts),
            ),
        )

        return cur.fetchone()["id"]


def get_partition_key(
    conn,
    row: Dict[str, Any],
    config: Dict[str, Any],
    partition_cache: Dict[str, int],
) -> Tuple[int, str, Dict[str, str]]:
    """
    Get the numeric partition key for a row.

    First checks the Python dictionary.
    If not found, inserts into Postgres and updates the dictionary.
    """
    key_parts = build_partition_parts(row, config)
    key_text = build_key_text(key_parts)

    if key_text in partition_cache:
        return partition_cache[key_text], key_text, key_parts

    partition_key = insert_partition_group(
        conn,
        key_text,
        key_parts,
    )

    partition_cache[key_text] = partition_key

    return partition_key, key_text, key_parts
