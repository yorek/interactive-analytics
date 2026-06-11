"""Parallel Snowflake load test comparing standard vs interactive warehouses.

Runs one of four query workloads in parallel across N concurrent users (threads),
each running I iterations (workload is selectable via --workload; default query1).
query0 is a single-row point lookup by tenant and event date; query1 uses literal
filters; query2 binds a random TENANT_ID (1–10000), EVENT_DATE
range (within the last three calendar months), and REGION on every query; query3
binds tenant, date range, regions, and event type for daily counts sorted by date.
Reports latency stats and
average result rows per query to the console.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import os
import random
import re
import statistics
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Sequence
import snowflake.connector
from dotenv import load_dotenv

_SPCS_TOKEN_PATH = Path("/snowflake/session/token")
_ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(_ENV_PATH)

def _env_str(key: str, fallback: str) -> str:
    return os.environ.get(key, fallback)

DEFAULT_DATABASE = _env_str("BENCH_DATABASE", "IW_PLAYGROUND")
DEFAULT_SCHEMA = _env_str("BENCH_SCHEMA", "IW_TEST")
DEFAULT_SEED = int(_env_str("BENCH_SEED", "42"))
DEFAULT_STANDARD_WAREHOUSE = _env_str("BENCH_STANDARD_WAREHOUSE", "STD_WH")
DEFAULT_INTERACTIVE_WAREHOUSE = _env_str("BENCH_INTERACTIVE_WAREHOUSE", "IW_WH")

TENANT_ID_MIN = 1
TENANT_ID_MAX = 10000

Q0_LOOKBACK_MONTHS = 12

Q2_REGIONS: tuple[str, ...] = ("us-east", "us-west", "eu-west", "ap-south")
Q2_LOOKBACK_MONTHS = 3

Q3_REGIONS: tuple[str, ...] = ("us-east", "us-west", "us-central")
Q3_EVENT_TYPES: tuple[str, ...] = (
    "view",
    "click",
    "search",
    "add_to_cart",
    "remove_from_cart",
    "update_quantity",
    "add_to_wishlist",
    "begin_checkout",
    "purchase",
    "refund",
    "sign_up",
    "login",
)
Q3_DATE_RANGE_MONTHS_MIN = 3
Q3_DATE_RANGE_MONTHS_MAX = 7
Q3_END_LOOKBACK_MONTHS = 12

# Snowflake accepts 1–32; default 32 is the platform maximum for high-concurrency runs.
DEFAULT_MAX_CONCURRENCY_LEVEL = 32
SNOWFLAKE_MAX_CONCURRENCY_LEVEL_LIMIT = 32

# Unquoted Snowflake identifiers only (safe to splice into SQL after this check).
_SAFE_DB_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

# XS interactive warehouse cache warm rate per Snowflake docs (~300–350 MB/s).
_CACHE_WARM_BYTES_PER_SEC = 300 * 1024 * 1024

_WORKLOAD_BASE_TABLE = "EVENTS"
_INTERACTIVE_TABLE_SUFFIX = "_IT"

_COMPARISON_TABLE_NAMES: tuple[str, ...] = (
    _WORKLOAD_BASE_TABLE,
)

WAREHOUSE_CHOICES: tuple[str, ...] = (
    DEFAULT_STANDARD_WAREHOUSE,
    DEFAULT_INTERACTIVE_WAREHOUSE,
)

def snowflake_connect(connection_name: str) -> Any:
    """Open a Snowflake connection using qmark parameter binding for better performances.

    When running inside Snowpark Container Services (SPCS), the OAuth session token
    mounted at ``/snowflake/session/token`` is used and ``connection_name`` is ignored.
    The OAuth session has no default warehouse, so the bootstrap warehouse is taken
    from ``SNOWFLAKE_WAREHOUSE`` (per-thread code still issues its own ``USE WAREHOUSE``).
    Otherwise, the named connection from ``~/.snowflake/connections.toml`` is used.
    """
    if _SPCS_TOKEN_PATH.exists():
        kwargs: dict[str, Any] = dict(
            host=os.environ["SNOWFLAKE_HOST"],
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            token=_SPCS_TOKEN_PATH.read_text(),
            authenticator="oauth",
            paramstyle="qmark",
        )
        bootstrap_wh = os.environ.get("SNOWFLAKE_WAREHOUSE")
        if bootstrap_wh:
            kwargs["warehouse"] = bootstrap_wh
        bootstrap_db = os.environ.get("SNOWFLAKE_DATABASE")
        if bootstrap_db:
            kwargs["database"] = bootstrap_db
        bootstrap_schema = os.environ.get("SNOWFLAKE_SCHEMA")
        if bootstrap_schema:
            kwargs["schema"] = bootstrap_schema
        return snowflake.connector.connect(**kwargs)
    return snowflake.connector.connect(
        connection_name=connection_name,
        paramstyle="qmark",
    )


def _validate_unquoted_identifier(name: str, label: str) -> str:
    """Return `name` if it is a safe unquoted Snowflake identifier, else raise."""
    if not _SAFE_DB_IDENT.match(name):
        raise argparse.ArgumentTypeError(
            f"Invalid {label} {name!r}: use letters, digits, underscore, "
            "or $; must start with a letter or underscore."
        )
    return name


def validate_database_identifier(name: str) -> str:
    """`argparse` type for `--database`: letters, digits, `_`, `$`; safe to embed in SQL."""
    return _validate_unquoted_identifier(name, "database name")


def validate_schema_identifier(name: str) -> str:
    """`argparse` type for `--schema`: letters, digits, `_`, `$`; safe to embed in SQL."""
    return _validate_unquoted_identifier(name, "schema name")


def parse_seed(value: str) -> int | None:
    """`argparse` type for ``--seed``: integer, or ``random`` for a new seed each run."""
    s = value.strip()
    if s.lower() == "random":
        return None
    try:
        return int(s, 10)
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"invalid seed {value!r}: use an integer or 'random'"
        ) from e


def resolve_base_seed(seed: int | None) -> int:
    """Return a concrete base seed; ``None`` means pick one at random."""
    if seed is None:
        return random.randint(1, 1_000_000)
    return seed


def workload_table_name(warehouse: str) -> str:
    """Return the benchmark table for `warehouse` (e.g. EVENTS vs EVENTS_IT)."""
    if is_interactive_warehouse(warehouse):
        return f"{_WORKLOAD_BASE_TABLE}{_INTERACTIVE_TABLE_SUFFIX}"
    return _WORKLOAD_BASE_TABLE


def _events_table_ref(database: str, schema: str, table_name: str) -> str:
    """Fully qualified EVENTS / EVENTS_IT table reference."""
    return f"{database}.{schema}.{table_name}"


def _month_bounds(year: int, month: int) -> tuple[date, date]:
    """Return inclusive month start and exclusive month end."""
    month_start = date(year, month, 1)
    if month == 12:
        month_end = date(year + 1, 1, 1)
    else:
        month_end = date(year, month + 1, 1)
    return month_start, month_end


def _recent_month_ranges(anchor: date, months: int) -> list[tuple[date, date]]:
    """Return (month_start, month_end) pairs for `anchor` month and prior months."""
    ranges: list[tuple[date, date]] = []
    year, month = anchor.year, anchor.month
    for _ in range(months):
        ranges.append(_month_bounds(year, month))
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return ranges


def _random_multi_month_range(
    rng: random.Random,
    anchor: date,
    *,
    span_months_min: int,
    span_months_max: int,
    end_lookback_months: int,
) -> tuple[date, date]:
    """Return (range_start, range_end) for a random multi-month window ending in lookback."""
    span_months = rng.randint(span_months_min, span_months_max)
    _, range_end = rng.choice(_recent_month_ranges(anchor, end_lookback_months))
    year, month = range_end.year, range_end.month
    for _ in range(span_months):
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    range_start, _ = _month_bounds(year, month)
    return range_start, range_end


def _random_tenant_id(rng: random.Random) -> int:
    return rng.randint(TENANT_ID_MIN, TENANT_ID_MAX)


def _random_event_date(rng: random.Random, anchor: date | None = None) -> str:
    """Return a random ISO date within the recent-month lookback window."""
    today = anchor or date.today()
    month_start, month_end = rng.choice(
        _recent_month_ranges(today, Q0_LOOKBACK_MONTHS)
    )
    day_count = (month_end - month_start).days
    offset = rng.randrange(day_count) if day_count else 0
    return (month_start + timedelta(days=offset)).isoformat()


def _sql_query0(database: str, schema: str, table_name: str) -> str:
    """Query 0: single-row point lookup by tenant and event date."""
    events = _events_table_ref(database, schema, table_name)
    return f"""
SELECT EVENT_TS
FROM {events}
WHERE TENANT_ID = ? AND EVENT_DATE = ?
LIMIT 1;
"""


def _sql_query1(database: str, schema: str, table_name: str) -> str:
    """Query 1: count events by event type for a fixed date range and region."""
    events = _events_table_ref(database, schema, table_name)
    return f"""
SELECT 
    EVENT_TYPE, COUNT(*) AS order_count 
FROM 
    {events} 
WHERE 
    TENANT_ID=1 
AND 
    EVENT_DATE >= '2026-01-01' AND EVENT_DATE < '2026-02-01' 
AND 
    REGION IN ('us-east', 'us-west') 
GROUP BY 
    EVENT_TYPE 
ORDER BY 
    order_count DESC 
LIMIT 20;
"""


def _sql_query2(database: str, schema: str, table_name: str) -> str:
    """Query 2: count events by event type for tenant, date range, and region."""
    events = _events_table_ref(database, schema, table_name)
    return f"""
SELECT 
    EVENT_TYPE, COUNT(*) AS order_count 
FROM 
    {events} 
WHERE 
    TENANT_ID = ? 
AND 
    EVENT_DATE >= ? AND EVENT_DATE < ?
AND 
    REGION IN (?) 
GROUP BY 
    EVENT_TYPE 
ORDER BY 
    order_count DESC 
LIMIT 20;"""


def _sql_query3(database: str, schema: str, table_name: str) -> str:
    """Query 3: daily event counts by date for tenant, region, and event type."""
    events = _events_table_ref(database, schema, table_name)
    return f"""
SELECT 
    EVENT_DATE,
    COUNT(*) AS EVENT_COUNT
FROM 
    {events} 
WHERE 
    TENANT_ID = ? 
AND 
    EVENT_DATE >= ? AND EVENT_DATE < ?
AND 
    REGION IN (?, ?, ?) 
AND
    EVENT_TYPE = ?
GROUP BY 
    EVENT_DATE
ORDER BY 
    EVENT_DATE DESC 
LIMIT 50;
"""


def _bind_query0(
    rng: random.Random, anchor: date | None = None
) -> tuple[int, str]:
    """Bind tenant_id and one event date."""
    return _random_tenant_id(rng), _random_event_date(rng, anchor)


def _bind_query2(
    rng: random.Random, anchor: date | None = None
) -> tuple[int, str, str, str]:
    """Bind tenant_id, one calendar month, and one region."""
    today = anchor or date.today()
    month_start, month_end = rng.choice(
        _recent_month_ranges(today, Q2_LOOKBACK_MONTHS)
    )
    return (
        _random_tenant_id(rng),
        month_start.isoformat(),
        month_end.isoformat(),
        rng.choice(Q2_REGIONS),
    )


def _bind_query3(
    rng: random.Random, anchor: date | None = None
) -> tuple[int, str, str, str, str, str, str]:
    """Bind tenant_id, multi-month date range, three regions, and event type."""
    today = anchor or date.today()
    range_start, range_end = _random_multi_month_range(
        rng,
        today,
        span_months_min=Q3_DATE_RANGE_MONTHS_MIN,
        span_months_max=Q3_DATE_RANGE_MONTHS_MAX,
        end_lookback_months=Q3_END_LOOKBACK_MONTHS,
    )
    return (
        _random_tenant_id(rng),
        range_start.isoformat(),
        range_end.isoformat(),
        *Q3_REGIONS,
        rng.choice(Q3_EVENT_TYPES),
    )


@dataclass(frozen=True, slots=True)
class Workload:
    """One benchmark query shape: SQL builder plus optional bind generator."""

    build_sql: Callable[[str, str, str], str]
    random_binds: Callable[[random.Random], tuple[Any, ...]] | None = None

    @property
    def description(self) -> str:
        return (self.build_sql.__doc__ or "").strip()


WORKLOADS: dict[str, Workload] = {
    "query0": Workload(build_sql=_sql_query0, random_binds=_bind_query0),
    "query1": Workload(build_sql=_sql_query1),
    "query2": Workload(build_sql=_sql_query2, random_binds=_bind_query2),
    "query3": Workload(build_sql=_sql_query3, random_binds=_bind_query3),
}
WORKLOAD_NAMES: tuple[str, ...] = tuple(WORKLOADS)


def query_sql(workload: str, database: str, schema: str, warehouse: str) -> str:
    """Return SQL for `workload`, using EVENTS or EVENTS_IT based on `warehouse`."""
    try:
        spec = WORKLOADS[workload]
    except KeyError as exc:
        raise ValueError(f"unknown workload: {workload!r}") from exc
    table_name = workload_table_name(warehouse)
    return spec.build_sql(database, schema, table_name)


def workload_doc(workload: str) -> str:
    """Docstring text for `workload` (for CLI / harness logging)."""
    return WORKLOADS[workload].description


def workload_binds(workload: str, rng: random.Random) -> tuple[Any, ...] | None:
    """Return bind parameters for one execution of `workload`, or None for literals."""
    generator = WORKLOADS[workload].random_binds
    return generator(rng) if generator is not None else None


def is_interactive_warehouse(warehouse: str) -> bool:
    """True if `warehouse` is an interactive warehouse that needs cache-warm prep."""
    return warehouse.upper() == DEFAULT_INTERACTIVE_WAREHOUSE


def parse_compare_warehouses(value: str) -> tuple[str, str]:
    """`argparse` type for ``--compare``: exactly two known warehouses, comma-separated."""
    parts = [p.strip().upper() for p in value.split(",") if p.strip()]
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(
            f"--compare expects exactly two comma-separated warehouses, got {len(parts)}"
        )
    valid = set(WAREHOUSE_CHOICES)
    for wh in parts:
        if wh not in valid:
            raise argparse.ArgumentTypeError(
                f"unknown warehouse {wh!r}; choose from: {', '.join(WAREHOUSE_CHOICES)}"
            )
    if parts[0] == parts[1]:
        raise argparse.ArgumentTypeError(
            f"--compare requires two distinct warehouses, got {parts[0]!r} twice"
        )
    return parts[0], parts[1]


def _cursor_column_index(cur: Any) -> dict[str, int]:
    """Map uppercased SHOW/DESCRIBE column names to row indices."""
    desc = cur.description or ()
    return {str(c[0]).upper(): i for i, c in enumerate(desc)}


def _fetch_warehouse_rows(
    cur: Any,
    warehouse_names: Sequence[str],
) -> tuple[dict[str, int], list[tuple[Any, ...]]]:
    """Return SHOW WAREHOUSES IN ACCOUNT rows filtered to `warehouse_names`."""
    names = [str(n).upper() for n in warehouse_names if str(n).strip()]
    if not names:
        return {}, []

    placeholders = ", ".join("?" * len(names))
    cur.execute("SHOW WAREHOUSES IN ACCOUNT;")
    cur.execute(
        f'SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE "name" IN ({placeholders})',
        names,
    )
    return _cursor_column_index(cur), cur.fetchall()


def fetch_warehouse_sizes(
    connection_name: str,
    warehouse_names: Sequence[str],
) -> dict[str, str]:
    """Return uppercased warehouse name -> size label (e.g. X-Small, Large)."""
    if not warehouse_names:
        return {}

    conn = snowflake_connect(connection_name)
    try:
        cur = conn.cursor()
        try:
            cols, rows = _fetch_warehouse_rows(cur, warehouse_names)
            name_i = cols.get("NAME")
            size_i = cols.get("SIZE")
            if name_i is None or size_i is None:
                return {}

            return {
                str(row[name_i]).upper(): str(row[size_i])
                for row in rows
            }
        finally:
            cur.close()
    finally:
        conn.close()


def warehouse_display_name(warehouse: str, sizes: dict[str, str]) -> str:
    """Human-readable warehouse label, appending SHOW WAREHOUSES size when known."""
    size = sizes.get(warehouse.upper())
    if size:
        return f"{warehouse} ({size})"
    return warehouse


def _clamp_max_concurrency_level(level: int) -> int:
    """Return a Snowflake-valid MAX_CONCURRENCY_LEVEL (1–32)."""
    clamped = max(1, min(int(level), SNOWFLAKE_MAX_CONCURRENCY_LEVEL_LIMIT))
    if clamped != int(level):
        print(
            f"[warehouse] warning: requested MAX_CONCURRENCY_LEVEL={level} "
            f"is out of range; using {clamped}",
            flush=True,
        )
    return clamped


def ensure_warehouse_max_concurrency(
    connection_name: str,
    warehouse: str,
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY_LEVEL,
) -> None:
    """Set MAX_CONCURRENCY_LEVEL on `warehouse` before the benchmark run."""
    level = _clamp_max_concurrency_level(max_concurrency)
    conn = snowflake_connect(connection_name=connection_name)
    try:
        cur = conn.cursor()
        try:
            cur.execute(f"ALTER WAREHOUSE {warehouse} SET MAX_CONCURRENCY_LEVEL = {level}")
            print(
                f"[warehouse] {warehouse} MAX_CONCURRENCY_LEVEL={level}",
                flush=True,
            )
        finally:
            cur.close()
    finally:
        conn.close()


def _fetch_warehouse_status(cur: Any, warehouse: str) -> tuple[str | None, str | None]:
    """Return (state, type) from SHOW WAREHOUSES, or (None, None) if not found."""
    cols, rows = _fetch_warehouse_rows(cur, [warehouse])
    state_i = cols.get("STATE")
    type_i = cols.get("TYPE")
    if state_i is None or not rows:
        return None, None

    row = rows[0]
    state = str(row[state_i])
    wh_type = str(row[type_i]) if type_i is not None and row[type_i] is not None else None
    return state, wh_type


def _format_warehouse_status(state: str | None, wh_type: str | None) -> str:
    """Human-readable warehouse state + TYPE for log lines."""
    parts: list[str] = []
    if state is not None:
        parts.append(f"state={state.upper()}")
    parts.append(f"type={wh_type if wh_type is not None else 'unknown'}")
    return " ".join(parts)


def _parse_tables_from_warehouse_ddl(ddl: str) -> list[str]:
    """Extract table identifiers from a CREATE/ALTER INTERACTIVE WAREHOUSE TABLES (...) clause."""
    match = re.search(r"\bTABLES\s*\(([^)]*)\)", ddl, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return []
    inner = match.group(1)
    parts = re.split(r",(?=(?:[^'\"]*[\"'][^'\"]*[\"'])*[^'\"]*$)", inner)
    names: list[str] = []
    for part in parts:
        token = part.strip().strip('"').strip("'")
        if token:
            names.append(token)
    return names


def _resolve_table_identity(
    table_ref: str,
    default_database: str,
    default_schema: str,
) -> tuple[str, str, str]:
    """Return (database, schema, table) for a SHOW/ADD TABLES style identifier."""
    segments = [s.strip().strip('"') for s in table_ref.split(".") if s.strip()]
    if len(segments) == 3:
        return segments[0], segments[1], segments[2]
    if len(segments) == 2:
        return default_database, segments[0], segments[1]
    if len(segments) == 1:
        return default_database, default_schema, segments[0]
    raise ValueError(f"invalid table reference: {table_ref!r}")


def _fetch_attached_table_refs(
    cur: Any,
    warehouse: str,
    database: str,
    schema: str,
) -> list[str]:
    """List interactive tables attached to `warehouse` (best-effort across Snowflake versions)."""
    try:
        cur.execute(f"SHOW TABLES IN WAREHOUSE {warehouse}")
        cols = _cursor_column_index(cur)
        db_i = cols.get("DATABASE_NAME")
        sch_i = cols.get("SCHEMA_NAME")
        name_i = cols.get("NAME")
        if name_i is not None:
            refs: list[str] = []
            for row in cur.fetchall():
                parts: list[str] = []
                if db_i is not None and row[db_i]:
                    parts.append(str(row[db_i]))
                if sch_i is not None and row[sch_i]:
                    parts.append(str(row[sch_i]))
                parts.append(str(row[name_i]))
                refs.append(".".join(parts))
            if refs:
                return refs
    except Exception:  # noqa: BLE001
        pass

    cur.execute("SELECT GET_DDL('WAREHOUSE', ?)", (warehouse,))
    ddl_row = cur.fetchone()
    if ddl_row and ddl_row[0]:
        refs = _parse_tables_from_warehouse_ddl(str(ddl_row[0]))
        if refs:
            return refs

    # Fallback: interactive tables in the workload schema (may include tables not attached).
    try:
        cur.execute(f"SHOW TABLES IN SCHEMA {database}.{schema}")
        cols = _cursor_column_index(cur)
        name_i = cols.get("NAME")
        interactive_i = cols.get("IS_INTERACTIVE")
        if name_i is None or interactive_i is None:
            return []
        refs = []
        for row in cur.fetchall():
            flag = row[interactive_i]
            if flag in (True, "Y", "y", "true", "TRUE", 1):
                refs.append(str(row[name_i]))
        if refs:
            print(
                "[interactive] attached-table list unavailable; using interactive "
                f"tables in {database}.{schema} as fallback",
                flush=True,
            )
        return refs
    except Exception:  # noqa: BLE001
        return []


def _fetch_table_bytes(
    cur: Any,
    database: str,
    schema: str,
    table_name: str,
) -> int | None:
    """Return SHOW TABLES bytes for one table, or None if not found."""
    cur.execute(
        f"SHOW TABLES LIKE ? IN SCHEMA {database}.{schema}",
        (table_name,),
    )
    cols = _cursor_column_index(cur)
    name_i = cols.get("NAME")
    bytes_i = cols.get("BYTES")
    if name_i is None or bytes_i is None:
        return None
    target = table_name.upper()
    for row in cur.fetchall():
        if str(row[name_i]).upper() == target:
            raw = row[bytes_i]
            return None if raw is None else int(raw)
    return None


def _format_bytes(num_bytes: int) -> str:
    """Human-readable byte size (binary units)."""
    n = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024.0 or unit == "TiB":
            if unit == "B":
                return f"{int(n)} {unit}"
            return f"{n:.2f} {unit}"
        n /= 1024.0
    return f"{num_bytes} B"


def _format_duration(seconds: float) -> str:
    """Human-readable duration for cache-warm estimates."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    if seconds < 3600:
        return f"{seconds / 60:.1f}m"
    return f"{seconds / 3600:.2f}h"


def ensure_interactive_warehouse_ready(
    connection_name: str,
    database: str,
    schema: str,
    warehouse: str = DEFAULT_INTERACTIVE_WAREHOUSE,
) -> None:
    """Resume the interactive warehouse if suspended; log cache-warm ETA only after a resume."""
    print(f"[interactive] checking warehouse {warehouse}...", flush=True)
    conn = snowflake_connect(connection_name=connection_name)
    try:
        cur = conn.cursor()
        try:
            state, wh_type = _fetch_warehouse_status(cur, warehouse)
            if state is None:
                print(
                    f"[interactive] warehouse {warehouse} not found in SHOW WAREHOUSES",
                    flush=True,
                )
                return

            normalized = state.upper()
            was_already_started = normalized == "STARTED"
            print(
                f"[interactive] warehouse {_format_warehouse_status(state, wh_type)}",
                flush=True,
            )
            resumed = False
            if normalized == "SUSPENDED":
                print(f"[interactive] resuming {warehouse}...", flush=True)
                cur.execute(f"ALTER WAREHOUSE {warehouse} RESUME IF SUSPENDED")
                resumed = True
            elif normalized not in ("STARTED", "RESUMING", "RESIZING"):
                print(
                    f"[interactive] unexpected state {normalized!r}; "
                    "attempting RESUME IF SUSPENDED",
                    flush=True,
                )
                cur.execute(f"ALTER WAREHOUSE {warehouse} RESUME IF SUSPENDED")
                resumed = True

            if resumed or normalized in ("RESUMING", "RESIZING"):
                deadline = time.monotonic() + 120.0
                while time.monotonic() < deadline:
                    state, wh_type = _fetch_warehouse_status(cur, warehouse)
                    if state and state.upper() == "STARTED":
                        print(
                            "[interactive] warehouse is STARTED "
                            f"({_format_warehouse_status(state, wh_type)})",
                            flush=True,
                        )
                        break
                    time.sleep(2.0)
                else:
                    print(
                        "[interactive] warehouse not STARTED after wait "
                        f"({_format_warehouse_status(state, wh_type)})",
                        flush=True,
                    )

            if was_already_started:
                return

            table_refs = _fetch_attached_table_refs(cur, warehouse, database, schema)
            if not table_refs:
                cols, rows = _fetch_warehouse_rows(cur, [warehouse])
                tables_i = cols.get("TABLES")
                n_tables = rows[0][tables_i] if tables_i is not None and rows else None
                suffix = (
                    f" (SHOW WAREHOUSES reports {n_tables} attached)"
                    if n_tables is not None
                    else ""
                )
                print(
                    f"[interactive] no attached tables discovered{suffix}",
                    flush=True,
                )
                return

            total_bytes = 0
            print("[interactive] attached tables:", flush=True)
            for ref in table_refs:
                db, sch, tbl = _resolve_table_identity(ref, database, schema)
                nbytes = _fetch_table_bytes(cur, db, sch, tbl)
                label = f"{db}.{sch}.{tbl}"
                if nbytes is None:
                    print(f"  - {label}: size unknown", flush=True)
                    continue
                total_bytes += nbytes
                warm_s = nbytes / _CACHE_WARM_BYTES_PER_SEC
                print(
                    f"  - {label}: {_format_bytes(nbytes)} "
                    f"(~{_format_duration(warm_s)} to warm at 300 MB/s)",
                    flush=True,
                )

            if total_bytes > 0:
                total_warm_s = total_bytes / _CACHE_WARM_BYTES_PER_SEC
                print(
                    "[interactive] estimated cache warm time: "
                    f"{_format_duration(total_warm_s)} "
                    f"for {_format_bytes(total_bytes)} total at 300 MB/s",
                    flush=True,
                )
            else:
                print(
                    "[interactive] could not resolve byte sizes for attached tables",
                    flush=True,
                )
        finally:
            cur.close()
    finally:
        conn.close()


def print_comparison_table_row_counts(
    connection_name: str,
    database: str,
    schema: str,
) -> None:
    """Log combined information_schema row count for benchmark table(s)."""
    conn = snowflake_connect(connection_name=connection_name)
    try:
        cur = conn.cursor()
        try:
            ph = ", ".join(["?"] * len(_COMPARISON_TABLE_NAMES))
            cur.execute(
                f"""
                SELECT table_schema, table_name, row_count
                FROM {database}.information_schema.tables
                WHERE UPPER(table_schema) = UPPER(?)
                  AND table_name IN ({ph})
                ORDER BY table_name
                """.format(database=database),
                (schema, *_COMPARISON_TABLE_NAMES),
            )
            by_name = {str(row[1]).upper(): row[2] for row in cur.fetchall()}
            total = 0
            for table_name in _COMPARISON_TABLE_NAMES:
                rc = by_name.get(table_name)
                if rc is None:
                    print("[tables] total row count: (not available)", flush=True)
                    return
                total += int(rc)
            print(f"[tables] total row count: {total:,}", flush=True)
        finally:
            cur.close()
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    """Configure and return CLI arguments for the load test."""
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--users", type=int, default=10, help="Concurrent users (threads).")
    p.add_argument("--iterations", type=int, default=10, help="Queries per user.")
    p.add_argument("--connection", default="PM", help="Snowflake connection name.")
    p.add_argument(
        "--database",
        default=DEFAULT_DATABASE,
        type=validate_database_identifier,
        help=f"Database containing TPCDS tables (default: {DEFAULT_DATABASE}).",
    )
    p.add_argument(
        "--schema",
        default=DEFAULT_SCHEMA,
        type=validate_schema_identifier,
        help=f"Schema containing EVENTS / EVENTS_IT (default: {DEFAULT_SCHEMA}).",
    )
    p.add_argument(
        "--workload",
        choices=WORKLOAD_NAMES,
        default="query1",
        help=(
            "Which query shape to run: query0 (single-row point lookup), "
            "query1 (literal filters; default), "
            "query2 (parameterized tenant_id, date range + region), "
            "query3 (parameterized daily event counts sorted by date)."
        ),
    )
    p.add_argument(
        "--seed",
        type=parse_seed,
        default=DEFAULT_SEED,
        help=f"RNG seed for reproducibility (default: {DEFAULT_SEED}); "
        "use 'random' for a different seed each run.",
    )

    grp = p.add_mutually_exclusive_group(required=True)
    grp.add_argument(
        "--warehouse",
        choices=WAREHOUSE_CHOICES,
        help="Run against a single warehouse.",
    )
    grp.add_argument(
        "--compare",
        type=parse_compare_warehouses,
        metavar="WH1,WH2",
        help=(
            "Compare two warehouses back-to-back (comma-separated), e.g. "
            f"'{DEFAULT_STANDARD_WAREHOUSE},{DEFAULT_INTERACTIVE_WAREHOUSE}'. "
            f"Choices: {', '.join(WAREHOUSE_CHOICES)}."
        ),
    )

    return p.parse_args()


def open_workload_connection(
    connection_name: str,
    warehouse: str,
    database: str,
    schema: str,
    workload: str,
) -> tuple[Any, Any, str]:
    """Open Snowflake connection, set warehouse and session, return (conn, cur, qsql).

    Caller must close `cur` then `conn` in ``finally`` blocks (same lifecycle as ``worker``).
    """
    conn = snowflake_connect(connection_name=connection_name)
    cur = conn.cursor()
    cur.execute(f"USE WAREHOUSE {warehouse}")
    cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
    qsql = query_sql(workload, database, schema, warehouse)
    return conn, cur, qsql


def _fetch_query_results(
    cur: Any,
    qsql: str,
    bind: tuple[Any, ...] | None,
) -> list[tuple[Any, ...]]:
    """Execute `qsql` and return all rows."""
    if bind is None:
        cur.execute(qsql)
    else:
        cur.execute(qsql, bind)
    return cur.fetchall()


def execute_workload_once(
    cur: Any,
    qsql: str,
    bind: tuple[Any, ...] | None,
) -> tuple[float, int]:
    """Run one workload query; return (latency_seconds, row_count). May raise."""
    t0 = time.perf_counter()
    rows = _fetch_query_results(cur, qsql, bind)
    return time.perf_counter() - t0, len(rows)


def warmup_workload_session(cur: Any, qsql: str, bind: tuple[Any, ...] | None) -> int:
    """Run one workload query to warm the session; excluded from benchmark timing."""
    return len(_fetch_query_results(cur, qsql, bind))


def worker(
    user_id: int,
    iterations: int,
    connection_name: str,
    database: str,
    schema: str,
    warehouse: str,
    workload: str,
    rng_seed: int,
) -> tuple[list[float], int, list[int], int]:
    """Run `iterations` of `workload` SQL on one connection; return (latencies_s, errors, row_counts, queries_executed)."""
    rng = random.Random(rng_seed)
    latencies: list[float] = []
    row_counts: list[int] = []
    errors = 0
    queries_executed = 0

    conn, cur, qsql = open_workload_connection(
        connection_name, warehouse, database, schema, workload
    )
    try:
        warmup_workload_session(cur, qsql, workload_binds(workload, rng))
        for _ in range(iterations):
            try:
                latency, nrows = execute_workload_once(
                    cur, qsql, workload_binds(workload, rng)
                )
                latencies.append(latency)
                row_counts.append(nrows)
            except Exception as exc:  # noqa: BLE001
                errors += 1
                print(
                    f"[user {user_id}] query error: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
            queries_executed += 1
    finally:
        cur.close()
        conn.close()

    return latencies, errors, row_counts, queries_executed


def run_phase(
    warehouse: str,
    users: int,
    iterations: int,
    connection_name: str,
    database: str,
    schema: str,
    workload: str,
    base_seed: int,
    warehouse_sizes: dict[str, str],
) -> dict:
    """Run all worker threads for one warehouse; return metrics including `latencies` and `row_counts`."""
    wh_line = warehouse_display_name(warehouse, warehouse_sizes)
    print(
        f"\n[run] warehouse={wh_line} workload={workload} users={users} iterations={iterations}"
        f" total_queries={users * iterations}",
        flush=True,
    )

    wall_start = time.perf_counter()
    all_latencies: list[float] = []
    all_row_counts: list[int] = []
    total_errors = 0
    total_queries_executed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=users) as ex:
        futures = [
            ex.submit(
                worker,
                user_id=i,
                iterations=iterations,
                connection_name=connection_name,
                database=database,
                schema=schema,
                warehouse=warehouse,
                workload=workload,
                # Per-user seed derived from base_seed so query2/query3 bind draws are
                # picked across compare-mode phases for fairness.
                rng_seed=base_seed + i,
            )
            for i in range(users)
        ]
        for fut in concurrent.futures.as_completed(futures):
            latencies, errors, row_counts, queries_executed = fut.result()
            all_latencies.extend(latencies)
            all_row_counts.extend(row_counts)
            total_errors += errors
            total_queries_executed += queries_executed

    wall_seconds = time.perf_counter() - wall_start

    return {
        "warehouse": warehouse,
        "users": users,
        "iterations": iterations,
        "wall_seconds": wall_seconds,
        "errors": total_errors,
        "queries_executed": total_queries_executed,
        "latencies": all_latencies,
        "row_counts": all_row_counts,
    }


def percentile(sorted_values: Sequence[float], pct: float) -> float:
    """Percentile of pre-sorted values using linear interpolation (NIST-style ranks)."""
    if not sorted_values:
        return float("nan")
    if len(sorted_values) == 1:
        return sorted_values[0]
    # Linear interpolation between closest ranks (NIST style).
    k = (len(sorted_values) - 1) * (pct / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = k - lo
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * frac


def summarize(latencies: Sequence[float]) -> dict[str, float]:
    """Aggregate latency list into avg/min/max/p50/p95/p99 (nan keys if empty)."""
    if not latencies:
        return {k: float("nan") for k in ("avg", "min", "max", "p50", "p95", "p99")}
    s = sorted(latencies)
    return {
        "avg": statistics.fmean(s),
        "min": s[0],
        "max": s[-1],
        "p50": percentile(s, 50),
        "p95": percentile(s, 95),
        "p99": percentile(s, 99),
    }


def print_single(result: dict, warehouse_sizes: dict[str, str]) -> None:
    """Print latency summary for one `run_phase` result dict."""
    stats = summarize(result["latencies"])
    print()
    title = warehouse_display_name(result["warehouse"], warehouse_sizes)
    print(f"=== Result: {title} ===")
    print(f"  queries executed: {result['queries_executed']}")
    rc = result["row_counts"]
    if rc:
        print(f"  avg rows/query: {statistics.fmean(rc):.2f}")
    else:
        print("  avg rows/query: n/a")
    print(f"  errors        : {result['errors']}")
    print(f"  wall seconds  : {result['wall_seconds']:.3f}")
    if result["wall_seconds"] > 0:
        qps = len(result["latencies"]) / result["wall_seconds"]
        print(f"  throughput    : {qps:.2f} q/s")
    print(f"  avg latency   : {stats['avg']:.3f} s")
    print(f"  min latency   : {stats['min']:.3f} s")
    print(f"  p50 latency   : {stats['p50']:.3f} s")
    print(f"  p95 latency   : {stats['p95']:.3f} s")
    print(f"  p99 latency   : {stats['p99']:.3f} s")
    print(f"  max latency   : {stats['max']:.3f} s")


def print_compare(
    a: dict, b: dict, warehouse_sizes: dict[str, str],
) -> None:
    """Print side-by-side stats for two `run_phase` results (e.g. standard vs interactive)."""
    sa = summarize(a["latencies"])
    sb = summarize(b["latencies"])
    metrics = ["avg", "p50", "p95", "p99", "min", "max"]
    name_a = warehouse_display_name(a["warehouse"], warehouse_sizes)
    name_b = warehouse_display_name(b["warehouse"], warehouse_sizes)

    print()
    print("=== Comparison ===")
    header = f"{'metric (s)':<12} {name_a:>32} {name_b:>32} {'delta (b-a)':>14}"
    print(header)
    print("-" * len(header))
    for m in metrics:
        va = sa[m]
        vb = sb[m]
        print(f"{m:<12} {va:>32.3f} {vb:>32.3f} {vb - va:>+14.3f}")
    wa, wb = a["wall_seconds"], b["wall_seconds"]
    ta = len(a["latencies"]) / wa if wa > 0 else None
    tb = len(b["latencies"]) / wb if wb > 0 else None
    if ta is not None or tb is not None:

        def qps_cell(x: float | None) -> str:
            return f"{x:>32.2f}" if x is not None else f"{'n/a':>32}"

        da = ta if ta is not None else 0.0
        db = tb if tb is not None else 0.0
        delta = db - da if ta is not None and tb is not None else float("nan")
        dcell = f"{delta:>+14.2f}" if ta is not None and tb is not None else f"{'n/a':>14}"
        print(f"{'throughput':<12} {qps_cell(ta)} {qps_cell(tb)} {dcell}")
    rca, rcb = a["row_counts"], b["row_counts"]
    ma = statistics.fmean(rca) if rca else None
    mb = statistics.fmean(rcb) if rcb else None
    if ma is not None or mb is not None:
        def cell(x: float | None) -> str:
            return f"{x:>32.2f}" if x is not None else f"{'n/a':>32}"

        da = ma if ma is not None else 0.0
        db = mb if mb is not None else 0.0
        delta = db - da if ma is not None and mb is not None else float("nan")
        dcell = f"{delta:>+14.2f}" if ma is not None and mb is not None else f"{'n/a':>14}"
        print(f"{'avg rows/q':<12} {cell(ma)} {cell(mb)} {dcell}")
    print()
    print(f"{'queries exec':<12} {a['queries_executed']:>32} {b['queries_executed']:>32}")
    print(f"errors        {a['errors']:>32} {b['errors']:>32}")
    print(f"wall seconds  {a['wall_seconds']:>32.3f} {b['wall_seconds']:>32.3f}")


def main() -> int:
    """Entry point: parse args, run phase(s), print results."""
    args = parse_args()

    base_seed = resolve_base_seed(args.seed)
    print(
        "[init] END-TO-END test: timings include network latency and local CPU load.",
        flush=True,
    )
    print(
        f"[init] database={args.database} schema={args.schema} workload={args.workload} "
        f"base_seed={base_seed}",
        flush=True,
    )
    wl_doc = workload_doc(args.workload)
    if wl_doc:
        print(f"[workload] {args.workload} : {wl_doc}", flush=True)

    print_comparison_table_row_counts(args.connection, args.database, args.schema)

    if args.compare:
        wh_names = args.compare
    else:
        wh_names = (args.warehouse,)

    warehouse_sizes = fetch_warehouse_sizes(args.connection, wh_names)
    if warehouse_sizes:
        for wn in wh_names:
            sz = warehouse_sizes.get(wn.upper(), "(not listed in SHOW WAREHOUSES)")
            print(f"[warehouse] {wn} size={sz}", flush=True)
    else:
        print(
            "[warehouse] could not resolve sizes from SHOW WAREHOUSES",
            flush=True,
        )

    for wh in wh_names:
        ensure_warehouse_max_concurrency(args.connection, wh)

    if args.compare:
        wh_a, wh_b = args.compare
        for wh in (wh_a, wh_b):
            if is_interactive_warehouse(wh):
                ensure_interactive_warehouse_ready(
                    args.connection, args.database, args.schema, wh
                )
        a = run_phase(
            warehouse=wh_a,
            users=args.users,
            iterations=args.iterations,
            connection_name=args.connection,
            database=args.database,
            schema=args.schema,
            workload=args.workload,
            base_seed=base_seed,
            warehouse_sizes=warehouse_sizes,
        )
        print_single(a, warehouse_sizes)
        b = run_phase(
            warehouse=wh_b,
            users=args.users,
            iterations=args.iterations,
            connection_name=args.connection,
            database=args.database,
            schema=args.schema,
            workload=args.workload,
            base_seed=base_seed,
            warehouse_sizes=warehouse_sizes,
        )
        print_single(b, warehouse_sizes)
        print_compare(a, b, warehouse_sizes)
    else:
        if is_interactive_warehouse(args.warehouse):
            ensure_interactive_warehouse_ready(
                args.connection, args.database, args.schema, args.warehouse
            )
        result = run_phase(
            warehouse=args.warehouse,
            users=args.users,
            iterations=args.iterations,
            connection_name=args.connection,
            database=args.database,
            schema=args.schema,
            workload=args.workload,
            base_seed=base_seed,
            warehouse_sizes=warehouse_sizes,
        )
        print_single(result, warehouse_sizes)

    return 0


if __name__ == "__main__":
    sys.exit(main())
