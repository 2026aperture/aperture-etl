import hashlib
import json
from typing import Any, Dict, Tuple

from config import (
    PARTITION_GROUPS_TABLE,
    PARTITION_STRATEGIES_TABLE,
    SOURCE_COLUMNS,
)


VALID_SUBSTRING_MODES = ("prefix", "suffix")


def allowed_partition_fields(config: Dict[str, Any]) -> set:
    """
    The fields that may appear in PARTITION_CONFIG["fields"].

    Any source column can be used directly, plus any named substring field
    declared in PARTITION_CONFIG["substrings"]. This is derived from config so
    that adding a column never requires editing this file.
    """
    return set(SOURCE_COLUMNS) | set(config.get("substrings", {}).keys())


def build_strategy_definition(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the portion of the config that defines a strategy.

    Only the parts that affect partition key generation are included: the
    strategy name, the configured fields, and the substring specs those fields
    actually reference. Unreferenced substrings are dropped so they cannot
    spuriously create a "new" strategy.
    """
    substrings = config.get("substrings", {})
    fields = list(config.get("fields", []))

    used_substrings = {
        field: substrings[field]
        for field in fields
        if field in substrings
    }

    return {
        "strategy_name": config.get("strategy_name"),
        "fields": fields,
        "substrings": used_substrings,
    }


def compute_strategy_hash(definition: Dict[str, Any]) -> str:
    """
    Compute a stable fingerprint of a strategy definition.

    The hash is order-independent: reordering `fields` (which does not change
    the generated keys, since key_text is sorted) yields the same hash, so it
    does not mint a new strategy.
    """
    canonical = {
        "strategy_name": definition.get("strategy_name"),
        "fields": sorted(definition.get("fields", [])),
        "substrings": definition.get("substrings", {}),
    }

    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"))

    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def get_or_create_strategy(conn, config: Dict[str, Any]) -> int:
    """
    Resolve the strategy id for the current config, creating it if needed.

    The same config content always maps to the same strategy row (matched on
    config_hash). Changing the config produces a new strategy, so every
    partition group records exactly which strategy created it.
    """
    definition = build_strategy_definition(config)
    config_hash = compute_strategy_hash(definition)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {PARTITION_STRATEGIES_TABLE} (
                strategy_name,
                definition,
                config_hash
            )
            VALUES (%s, %s::jsonb, %s)
            ON CONFLICT (config_hash)
            DO UPDATE SET strategy_name = EXCLUDED.strategy_name
            RETURNING id;
            """,
            (
                definition["strategy_name"],
                json.dumps(definition),
                config_hash,
            ),
        )

        return cur.fetchone()["id"]


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


def get_substring(value: Any, length: int | None, mode: str = "prefix") -> str | None:
    """
    Return `length` characters from a value, taken from the start ("prefix")
    or the end ("suffix").

    If length is 0 or None, this returns None.
    That means the substring should not be available unless explicitly requested.
    """
    if not length or length <= 0:
        return None

    if value is None or value == "":
        return "unknown"

    text = str(value)
    chunk = text[-length:] if mode == "suffix" else text[:length]

    return normalize(chunk)


def validate_partition_config(config: Dict[str, Any]) -> None:
    """
    Validate the config before processing rows.
    """

    fields = config.get("fields")

    if not isinstance(fields, list) or not fields:
        raise ValueError("PARTITION_CONFIG must include a non-empty fields list.")

    allowed = allowed_partition_fields(config)
    unsupported_fields = set(fields) - allowed

    if unsupported_fields:
        raise ValueError(
            f"Unsupported partition fields: {sorted(unsupported_fields)}. "
            f"Allowed fields are: {sorted(allowed)}"
        )

    substrings = config.get("substrings", {})

    for field in fields:
        if field not in substrings:
            continue

        spec = substrings[field]

        source = spec.get("source")

        if not source:
            raise ValueError(
                f"substrings['{field}'] must define a 'source' column."
            )

        if source not in SOURCE_COLUMNS:
            raise ValueError(
                f"substrings['{field}']['source'] is '{source}', which is not "
                f"a known source column. Allowed sources are: "
                f"{sorted(SOURCE_COLUMNS)}"
            )

        if spec.get("length", 0) <= 0:
            raise ValueError(
                f"fields includes substring field '{field}', but "
                f"substrings['{field}']['length'] is missing or <= 0."
            )

        mode = spec.get("mode", "prefix")

        if mode not in VALID_SUBSTRING_MODES:
            raise ValueError(
                f"substrings['{field}']['mode'] is '{mode}', but must be one "
                f"of {list(VALID_SUBSTRING_MODES)}."
            )


def build_partition_parts(row: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, str]:
    """
    Build the fields requested for partitioning, applying normalization and
    prefixing as needed.

    Each configured field is resolved generically:
    - If it is a named substring field, take the first or last N characters of
      its source column, depending on its mode.
    - Otherwise, treat it as a source column and normalize its value.
    """
    substrings = config.get("substrings", {})

    key_parts = {}

    for field in config["fields"]:
        if field in substrings:
            spec = substrings[field]
            value = get_substring(
                row.get(spec["source"]),
                spec.get("length", 0),
                spec.get("mode", "prefix"),
            )

            if value is None:
                raise ValueError(
                    f"{field} was requested, but the substring length for it "
                    f"was not set."
                )
        else:
            value = normalize(row.get(field))

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


def load_partition_cache(conn, strategy_id: int) -> Dict[str, int]:
    """
    Load existing partition key mappings for one strategy into a dictionary.

    Each strategy has its own namespace of partition keys, so the cache is
    scoped to the strategy being run. This is the fast hashmap:
    {
        "gsid_prefix=zh/platform=android": 17
    }
    """
    cache = {}

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, key_text
            FROM {PARTITION_GROUPS_TABLE}
            WHERE strategy_id = %s;
            """,
            (strategy_id,),
        )

        for row in cur.fetchall():
            cache[row["key_text"]] = row["id"]

    return cache


def insert_partition_group(
    conn,
    strategy_id: int,
    key_text: str,
    key_parts: Dict[str, str],
) -> int:
    """
    Insert a new partition group if needed and return the numeric partition key.

    Partition groups are unique per (strategy_id, key_text), so the same
    key_text under a different strategy gets its own partition key.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {PARTITION_GROUPS_TABLE} (
                strategy_id,
                key_text,
                key_parts
            )
            VALUES (%s, %s, %s::jsonb)
            ON CONFLICT (strategy_id, key_text)
            DO UPDATE SET key_parts = EXCLUDED.key_parts
            RETURNING id;
            """,
            (
                strategy_id,
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
    strategy_id: int,
) -> Tuple[int, str, Dict[str, str]]:
    """
    Get the numeric partition key for a row under the given strategy.

    First checks the Python dictionary.
    If not found, inserts into Postgres and updates the dictionary.
    """
    key_parts = build_partition_parts(row, config)
    key_text = build_key_text(key_parts)

    if key_text in partition_cache:
        return partition_cache[key_text], key_text, key_parts

    partition_key = insert_partition_group(
        conn,
        strategy_id,
        key_text,
        key_parts,
    )

    partition_cache[key_text] = partition_key

    return partition_key, key_text, key_parts
