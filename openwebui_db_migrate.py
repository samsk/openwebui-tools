#!/usr/bin/env python3
"""Migrate OpenWebUI data between SQLAlchemy databases.

This script copies table data from source DB to target DB.
It skips framework tables by default and validates alembic version parity.

Requirements:
    - sqlalchemy (python3-sqlalchemy)
    - DB drivers (e.g., psycopg2-binary or psycopg for PostgreSQL)

Usage:
    # Basic migration from SQLite to PostgreSQL
    ./openwebui_db_migrate.py \
        --source-url sqlite:///path/to/webui.db \
        --target-url postgresql://user:pass@localhost:5432/openwebui

    # Dry run to see table order and validate connection
    ./openwebui_db_migrate.py \
        --source-url sqlite:///path/to/webui.db \
        --target-url postgresql://user:pass@localhost:5432/openwebui \
        --dry-run
"""

from __future__ import annotations

import argparse
import json
import warnings
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Sequence, Set, Tuple

from sqlalchemy import MetaData, create_engine, insert, text
from sqlalchemy.engine import Connection, Engine, Transaction
from sqlalchemy.exc import DBAPIError, IntegrityError, SAWarning, SQLAlchemyError
from sqlalchemy.sql.schema import Column, Table

DEFAULT_SKIP_TABLES: Set[str] = {"alembic_version", "migratehistory"}
DEFAULT_BATCH_SIZE: int = 500
DEFAULT_COMMIT_BATCH_SIZE: int = 10000


@dataclass
class Args:
    """CLI arguments.

    Args:
        source_url: Source SQLAlchemy URL.
        target_url: Target SQLAlchemy URL.
        batch_size: Read/insert batch size.
        commit_batch_size: Successful insert count before commit/restart.
        best_effort: Continue on skippable row errors.
        clean: Truncate target tables before import.
        tables: Optional include table names.
        exclude_tables: Optional exclude table names.
        dry_run: Plan and validate only.
        list_table_order: Print resolved table order and exit.
        echo_sql: Enable SQLAlchemy SQL echo.
    """

    source_url: str
    target_url: str
    batch_size: int
    commit_batch_size: int
    best_effort: bool
    clean: bool
    tables: Set[str]
    exclude_tables: Set[str]
    dry_run: bool
    list_table_order: bool
    echo_sql: bool


@dataclass
class TableStats:
    """Per-table migration stats.

    Args:
        table: Table name.
        scanned: Rows scanned from source.
        inserted: Rows inserted into target.
        skipped: Rows skipped in best-effort mode.
        failed_batches: Failed batch insert attempts.
    """

    table: str
    scanned: int = 0
    inserted: int = 0
    skipped: int = 0
    failed_batches: int = 0


def parse_table_set(raw_values: Sequence[str]) -> Set[str]:
    """Parse comma-splittable table list.

    Args:
        raw_values: List of raw CLI values.

    Returns:
        Normalized set of table names.
    """

    items: Set[str] = set()
    for raw_value in raw_values:
        for part in raw_value.split(","):
            name = part.strip()
            if name:
                items.add(name)
    return items


def build_parser() -> argparse.ArgumentParser:
    """Create argument parser.

    Returns:
        Configured parser instance.
    """

    parser = argparse.ArgumentParser(
        description="Migrate OpenWebUI data from source DB to target DB.",
    )
    parser.add_argument("--source-url", required=True, help="Source SQLAlchemy URL")
    parser.add_argument("--target-url", required=True, help="Target SQLAlchemy URL")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Rows per read/insert batch (default: {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--commit-batch-size",
        type=int,
        default=DEFAULT_COMMIT_BATCH_SIZE,
        help=(
            "Successful inserts before commit + transaction restart "
            f"(default: {DEFAULT_COMMIT_BATCH_SIZE})"
        ),
    )
    parser.add_argument(
        "--best-effort",
        action="store_true",
        help="Skip invalid rows after row-level retry",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean target tables before import",
    )
    parser.add_argument(
        "--tables",
        action="append",
        default=[],
        help="Only include these tables (comma-separated allowed)",
    )
    parser.add_argument(
        "--exclude-tables",
        action="append",
        default=[],
        help="Exclude these tables (comma-separated allowed)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and print plan only, no writes",
    )
    parser.add_argument(
        "--list-table-order",
        action="store_true",
        help="Print resolved table import order and exit",
    )
    parser.add_argument(
        "--echo-sql",
        action="store_true",
        help="Enable SQL echo for debugging",
    )
    return parser


def parse_args() -> Args:
    """Parse and validate CLI args.

    Returns:
        Parsed argument dataclass.
    """

    ns = build_parser().parse_args()
    if ns.batch_size <= 0:
        raise SystemExit("--batch-size must be > 0")
    if ns.commit_batch_size <= 0:
        raise SystemExit("--commit-batch-size must be > 0")
    tables = parse_table_set(ns.tables)
    exclude_tables = parse_table_set(ns.exclude_tables)
    return Args(
        source_url=ns.source_url,
        target_url=ns.target_url,
        batch_size=ns.batch_size,
        commit_batch_size=ns.commit_batch_size,
        best_effort=ns.best_effort,
        clean=ns.clean,
        tables=tables,
        exclude_tables=exclude_tables,
        dry_run=ns.dry_run,
        list_table_order=ns.list_table_order,
        echo_sql=ns.echo_sql,
    )


def create_db_engine(url: str, echo_sql: bool) -> Engine:
    """Build SQLAlchemy engine.

    Args:
        url: SQLAlchemy URL.
        echo_sql: Emit SQL statements when true.

    Returns:
        SQLAlchemy engine.
    """

    return create_engine(url, future=True, echo=echo_sql)


def get_migratehistory_max_id(connection: Connection) -> int | None:
    """Read max migratehistory id.

    Args:
        connection: Active DB connection.

    Returns:
        Maximum migratehistory id or None for empty table.
    """

    result = connection.execute(text("SELECT MAX(id) FROM migratehistory")).scalar()
    if result is None:
        return None
    return int(result)


def run_sqlite_pragma_checks(connection: Connection) -> None:
    """Run SQLite PRAGMA consistency checks.

    Args:
        connection: Source DB connection.
    """

    if connection.dialect.name != "sqlite":
        return

    integrity_rows = connection.exec_driver_sql("PRAGMA integrity_check").fetchall()
    integrity_values = [str(row[0]).lower() for row in integrity_rows]
    if integrity_values != ["ok"]:
        raise RuntimeError(f"sqlite PRAGMA integrity_check failed: {integrity_rows!r}")

    quick_rows = connection.exec_driver_sql("PRAGMA quick_check").fetchall()
    quick_values = [str(row[0]).lower() for row in quick_rows]
    if quick_values != ["ok"]:
        raise RuntimeError(f"sqlite PRAGMA quick_check failed: {quick_rows!r}")

    fk_rows = connection.exec_driver_sql("PRAGMA foreign_key_check").fetchall()
    if fk_rows:
        raise RuntimeError(f"sqlite PRAGMA foreign_key_check failed: {fk_rows!r}")


def reflect_tables(engine: Engine) -> MetaData:
    """Reflect DB schema.

    Args:
        engine: SQLAlchemy engine.

    Returns:
        Reflected metadata.
    """

    metadata = MetaData()
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r"Did not recognize type 'vector' of column 'vector'",
            category=SAWarning,
        )
        metadata.reflect(bind=engine)
    return metadata


def resolve_table_names(
    source_meta: MetaData,
    target_meta: MetaData,
    include_tables: Set[str],
    exclude_tables: Set[str],
) -> List[str]:
    """Resolve migratable table names.

    Args:
        source_meta: Reflected source metadata.
        target_meta: Reflected target metadata.
        include_tables: Optional allow-list.
        exclude_tables: Additional deny-list.

    Returns:
        Sorted list of table names to process.
    """

    source_tables = set(source_meta.tables.keys())
    target_tables = set(target_meta.tables.keys())
    names = source_tables & target_tables
    names -= DEFAULT_SKIP_TABLES
    names -= exclude_tables
    if include_tables:
        names &= include_tables
    return sorted(names)


def table_dependency_order(metadata: MetaData, table_names: Sequence[str]) -> List[str]:
    """Build FK-safe table order.

    Args:
        metadata: Metadata containing reflected tables.
        table_names: Candidate table names.

    Returns:
        Ordered table names, parents first.
    """

    remaining: Set[str] = set(table_names)
    dependencies: Dict[str, Set[str]] = {name: set() for name in table_names}
    reverse_edges: Dict[str, Set[str]] = {name: set() for name in table_names}

    for table_name in table_names:
        table = metadata.tables[table_name]
        for fk in table.foreign_keys:
            parent_name = fk.column.table.name
            if parent_name in remaining and parent_name != table_name:
                dependencies[table_name].add(parent_name)
                reverse_edges[parent_name].add(table_name)

    ready = sorted([name for name in table_names if not dependencies[name]])
    ordered: List[str] = []
    while ready:
        current = ready.pop(0)
        ordered.append(current)
        for child in sorted(reverse_edges[current]):
            dependencies[child].discard(current)
            if not dependencies[child] and child not in ordered and child not in ready:
                ready.append(child)
        ready.sort()

    if len(ordered) != len(table_names):
        cyclic = sorted(set(table_names) - set(ordered))
        ordered.extend(cyclic)
    return ordered


def single_column_pk(table: Table) -> Column[Any] | None:
    """Return single-column PK when available.

    Args:
        table: SQLAlchemy table.

    Returns:
        Single PK column or None.
    """

    pk_columns = list(table.primary_key.columns)
    if len(pk_columns) != 1:
        return None
    return pk_columns[0]


def quote_ident_for_dialect(connection: Connection, name: str) -> str:
    """Quote identifier using connection dialect.

    Args:
        connection: Active DB connection.
        name: Identifier name.

    Returns:
        Dialect-safe quoted identifier.
    """

    return connection.dialect.identifier_preparer.quote(name)


def quoted_table_ref(connection: Connection, table: Table) -> str:
    """Build quoted table reference for SQL string queries.

    Args:
        connection: Active DB connection.
        table: SQLAlchemy table.

    Returns:
        Qualified table reference.
    """

    table_name = quote_ident_for_dialect(connection, table.name)
    if table.schema:
        schema_name = quote_ident_for_dialect(connection, table.schema)
        return f"{schema_name}.{table_name}"
    return table_name


def quoted_column_list(connection: Connection, table: Table) -> str:
    """Build quoted projection list for table columns.

    Args:
        connection: Active DB connection.
        table: SQLAlchemy table.

    Returns:
        Comma separated quoted columns.
    """

    return ", ".join(quote_ident_for_dialect(connection, col.name) for col in table.columns)


def ordered_column_names(table: Table) -> List[str]:
    """Resolve deterministic order column names.

    Args:
        table: SQLAlchemy table.

    Returns:
        Ordered column name list.
    """

    pk_cols = [col.name for col in table.primary_key.columns]
    if pk_cols:
        return pk_cols
    return sorted(table.columns.keys())


def rows_to_dicts(rows: Sequence[Any]) -> List[Dict[str, Any]]:
    """Convert SQLAlchemy rows to list of plain dicts.

    Args:
        rows: SQLAlchemy rows.

    Returns:
        List of dict mappings.
    """

    return [dict(row._mapping) for row in rows]


def decode_bytes(value: bytes) -> str:
    """Decode bytes with utf-8 fallback.

    Args:
        value: Raw bytes value.

    Returns:
        Decoded text.
    """

    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return value.decode("latin1", errors="replace")


def normalize_value_for_column(value: Any, column: Column[Any]) -> Any:
    """Normalize source value for target column type.

    Args:
        value: Source value.
        column: Target SQLAlchemy column.

    Returns:
        Normalized value.
    """

    if value is None:
        return None

    if isinstance(value, bytes):
        value = decode_bytes(value)

    try:
        python_type = column.type.python_type
    except NotImplementedError:
        python_type = None

    if python_type is bool and isinstance(value, (int, str)):
        if isinstance(value, int):
            return bool(value)
        lowered = value.strip().lower()
        if lowered in {"1", "true", "t", "yes", "y"}:
            return True
        if lowered in {"0", "false", "f", "no", "n"}:
            return False
        return None

    is_json_column = "json" in column.type.__class__.__name__.lower()
    if is_json_column:
        if isinstance(value, (dict, list)):
            return value
        if isinstance(value, str):
            text_value = value.strip()
            if not text_value:
                return {} if not column.nullable else None
            try:
                return json.loads(text_value)
            except json.JSONDecodeError:
                return {} if not column.nullable else None
        return {} if not column.nullable else None

    if isinstance(value, str):
        return value.replace("\x00", "")

    return value


def normalize_batch_for_target(
    batch: List[Dict[str, Any]],
    target_table: Table,
) -> List[Dict[str, Any]]:
    """Normalize and filter source rows for target table.

    Args:
        batch: Source row dicts.
        target_table: Target SQLAlchemy table.

    Returns:
        Rows with target-only keys and normalized values.
    """

    target_columns = {column.name: column for column in target_table.columns}
    normalized: List[Dict[str, Any]] = []
    for row in batch:
        new_row: Dict[str, Any] = {}
        for column_name, column in target_columns.items():
            if column_name not in row:
                continue
            new_row[column_name] = normalize_value_for_column(row[column_name], column)
        normalized.append(new_row)
    return normalized


def iter_rows_keyset(
    connection: Connection,
    table: Table,
    batch_size: int,
) -> Iterator[List[Dict[str, Any]]]:
    """Read rows using keyset paging.

    Args:
        connection: Source DB connection.
        table: Source table.
        batch_size: Rows per batch.

    Yields:
        Row batches as plain dicts.
    """

    pk_column = single_column_pk(table)
    if pk_column is None:
        return

    last_pk: Any | None = None
    table_ref = quoted_table_ref(connection, table)
    columns_sql = quoted_column_list(connection, table)
    pk_sql = quote_ident_for_dialect(connection, pk_column.name)
    while True:
        query = (
            f"SELECT {columns_sql} FROM {table_ref} "
            f"ORDER BY {pk_sql} ASC LIMIT :batch_size"
        )
        params: Dict[str, Any] = {"batch_size": batch_size}
        if last_pk is not None:
            query = (
                f"SELECT {columns_sql} FROM {table_ref} "
                f"WHERE {pk_sql} > :last_pk ORDER BY {pk_sql} ASC LIMIT :batch_size"
            )
            params["last_pk"] = last_pk
        rows = connection.exec_driver_sql(query, params).fetchall()
        if not rows:
            break
        batch = rows_to_dicts(rows)
        yield batch
        last_pk = batch[-1][pk_column.name]


def iter_rows_stream(
    connection: Connection,
    table: Table,
    batch_size: int,
) -> Iterator[List[Dict[str, Any]]]:
    """Read rows using streaming scan.

    Args:
        connection: Source DB connection.
        table: Source table.
        batch_size: Rows per batch.

    Yields:
        Row batches as plain dicts.
    """

    order_columns = ordered_column_names(table)
    order_sql = ", ".join(quote_ident_for_dialect(connection, col) for col in order_columns)
    table_ref = quoted_table_ref(connection, table)
    columns_sql = quoted_column_list(connection, table)
    query = f"SELECT {columns_sql} FROM {table_ref} ORDER BY {order_sql}"
    result = connection.exec_driver_sql(query)

    while True:
        rows = result.fetchmany(batch_size)
        if not rows:
            break
        yield rows_to_dicts(rows)


def iter_rows_offset(
    connection: Connection,
    table: Table,
    batch_size: int,
) -> Iterator[List[Dict[str, Any]]]:
    """Read rows using LIMIT/OFFSET fallback.

    Args:
        connection: Source DB connection.
        table: Source table.
        batch_size: Rows per batch.

    Yields:
        Row batches as plain dicts.
    """

    offset = 0
    order_columns = ordered_column_names(table)
    order_sql = ", ".join(quote_ident_for_dialect(connection, col) for col in order_columns)
    table_ref = quoted_table_ref(connection, table)
    columns_sql = quoted_column_list(connection, table)

    while True:
        query = (
            f"SELECT {columns_sql} FROM {table_ref} "
            f"ORDER BY {order_sql} LIMIT :batch_size OFFSET :offset_rows"
        )
        rows = connection.exec_driver_sql(
            query,
            {"batch_size": batch_size, "offset_rows": offset},
        ).fetchall()
        if not rows:
            break
        batch = rows_to_dicts(rows)
        yield batch
        offset += len(batch)


def iter_table_batches(
    connection: Connection,
    table: Table,
    batch_size: int,
) -> Iterator[List[Dict[str, Any]]]:
    """Pick batching strategy and iterate rows.

    Args:
        connection: Source DB connection.
        table: Source table.
        batch_size: Rows per batch.

    Yields:
        Row batches as plain dicts.
    """

    pk_column = single_column_pk(table)
    if pk_column is not None:
        for batch in iter_rows_keyset(connection, table, batch_size):
            yield batch
        return

    try:
        for batch in iter_rows_stream(connection, table, batch_size):
            yield batch
    except Exception:
        for batch in iter_rows_offset(connection, table, batch_size):
            yield batch


def is_skippable_error(error: SQLAlchemyError) -> bool:
    """Detect row errors that can be skipped.

    Args:
        error: SQLAlchemy exception.

    Returns:
        True if safe to skip in best-effort mode.
    """

    if isinstance(error, IntegrityError):
        return True
    return False


def quote_ident(name: str) -> str:
    """Quote SQL identifier safely.

    Args:
        name: Identifier name.

    Returns:
        Double-quoted identifier.
    """

    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def quote_table_name(table: Table) -> str:
    """Build quoted table reference.

    Args:
        table: SQLAlchemy table.

    Returns:
        Fully qualified quoted table name.
    """

    if table.schema:
        return f"{quote_ident(table.schema)}.{quote_ident(table.name)}"
    return quote_ident(table.name)


def clean_target_tables(
    connection: Connection,
    target_meta: MetaData,
    table_order: Sequence[str],
) -> None:
    """Clean target tables before import.

    Args:
        connection: Target DB connection.
        target_meta: Reflected target metadata.
        table_order: FK-safe parent-first order.
    """

    reverse_order = list(reversed(table_order))
    if connection.dialect.name == "postgresql":
        table_sql = ", ".join(quote_table_name(target_meta.tables[name]) for name in reverse_order)
        if table_sql:
            connection.execute(text(f"TRUNCATE TABLE {table_sql} RESTART IDENTITY CASCADE"))
        return

    for table_name in reverse_order:
        table = target_meta.tables[table_name]
        connection.execute(table.delete())


def reset_postgresql_sequences(
    connection: Connection,
    target_meta: MetaData,
    table_names: Sequence[str],
) -> None:
    """Reset PostgreSQL serial sequences to match imported data.

    Args:
        connection: Target DB connection.
        target_meta: Reflected target metadata.
        table_names: Migrated table names.
    """

    if connection.dialect.name != "postgresql":
        return

    for table_name in table_names:
        table = target_meta.tables[table_name]
        qualified_table_for_lookup = (
            f"{table.schema}.{table.name}" if table.schema else table.name
        )
        quoted_table = quote_table_name(table)
        for column in table.columns:
            if not column.primary_key:
                continue
            try:
                python_type = column.type.python_type
            except NotImplementedError:
                continue
            if python_type is not int:
                continue

            sequence_name = connection.execute(
                text("SELECT pg_get_serial_sequence(:table_name, :column_name)"),
                {"table_name": qualified_table_for_lookup, "column_name": column.name},
            ).scalar()
            if not sequence_name:
                continue

            quoted_column = quote_ident(column.name)
            connection.execute(
                text(
                    f"SELECT setval(:sequence_name, "
                    f"COALESCE((SELECT MAX({quoted_column}) FROM {quoted_table}), 0) + 1, false)"
                ),
                {"sequence_name": sequence_name},
            )


def insert_batch_best_effort(
    connection: Connection,
    target_table: Table,
    batch: List[Dict[str, Any]],
    stats: TableStats,
) -> int:
    """Insert batch with best-effort fallback.

    Args:
        connection: Target DB connection.
        target_table: Target table.
        batch: Rows to insert.
        stats: Mutable table stats.

    Returns:
        Number of inserted rows.
    """

    inserted = 0
    stmt = insert(target_table)
    try:
        with connection.begin_nested():
            connection.execute(stmt, batch)
        return len(batch)
    except SQLAlchemyError:
        stats.failed_batches += 1

    for row in batch:
        try:
            with connection.begin_nested():
                connection.execute(stmt, row)
            inserted += 1
        except SQLAlchemyError as row_error:
            if is_skippable_error(row_error):
                stats.skipped += 1
                continue
            raise
    return inserted


def insert_batch_strict(
    connection: Connection,
    target_table: Table,
    batch: List[Dict[str, Any]],
) -> int:
    """Insert batch in strict mode.

    Args:
        connection: Target DB connection.
        target_table: Target table.
        batch: Rows to insert.

    Returns:
        Inserted row count.
    """

    connection.execute(insert(target_table), batch)
    return len(batch)


def migrate_table(
    source_connection: Connection,
    target_connection: Connection,
    current_tx: Transaction,
    source_table: Table,
    target_table: Table,
    args: Args,
    commit_since_restart: int,
) -> Tuple[TableStats, int, Transaction]:
    """Migrate one table.

    Args:
        source_connection: Source DB connection.
        target_connection: Target DB connection.
        current_tx: Active transaction object.
        source_table: Reflected source table.
        target_table: Reflected target table.
        args: Runtime arguments.
        commit_since_restart: Inserts since last transaction restart.

    Returns:
        Table stats, updated restart counter, active transaction.
    """

    stats = TableStats(table=source_table.name)
    for batch in iter_table_batches(source_connection, source_table, args.batch_size):
        if not batch:
            continue
        stats.scanned += len(batch)
        prepared_batch = normalize_batch_for_target(batch=batch, target_table=target_table)

        if args.best_effort:
            inserted = insert_batch_best_effort(
                connection=target_connection,
                target_table=target_table,
                batch=prepared_batch,
                stats=stats,
            )
        else:
            inserted = insert_batch_strict(
                connection=target_connection,
                target_table=target_table,
                batch=prepared_batch,
            )
        stats.inserted += inserted
        commit_since_restart += inserted

        if commit_since_restart >= args.commit_batch_size:
            current_tx.commit()
            current_tx = target_connection.begin()
            commit_since_restart = 0
    return stats, commit_since_restart, current_tx


def print_order(table_order: Sequence[str]) -> None:
    """Print table order.

    Args:
        table_order: Ordered table names.
    """

    print(f"Table import order ({len(table_order)} tables):")
    for index, table_name in enumerate(table_order, start=1):
        print(f"{index}. {table_name}")


def count_table_rows(connection: Connection, table: Table) -> int:
    """Count rows in table.

    Args:
        connection: Active DB connection.
        table: Reflected SQLAlchemy table.

    Returns:
        Row count for the table.
    """

    table_ref = quoted_table_ref(connection, table)
    result = connection.exec_driver_sql(f"SELECT COUNT(*) FROM {table_ref}").scalar()
    if result is None:
        return 0
    return int(result)


def print_dry_run_counts(
    source_connection: Connection,
    target_connection: Connection,
    source_meta: MetaData,
    target_meta: MetaData,
    table_order: Sequence[str],
) -> None:
    """Print dry-run table row counts.

    Args:
        source_connection: Source DB connection.
        target_connection: Target DB connection.
        source_meta: Reflected source metadata.
        target_meta: Reflected target metadata.
        table_order: Ordered table names.
    """

    source_total = 0
    target_total = 0
    table_width = max((len(name) for name in table_order), default=5)
    print(f"Dry-run plan ({len(table_order)} tables):")
    print(
        f"{'#':>3}  {'table':<{table_width}}  {'source':>10}  {'target':>10}  {'diff':>8}"
    )
    print(
        f"{'-' * 3}  {'-' * table_width}  {'-' * 10}  {'-' * 10}  {'-' * 8}"
    )
    for index, table_name in enumerate(table_order, start=1):
        source_count = count_table_rows(source_connection, source_meta.tables[table_name])
        target_count = count_table_rows(target_connection, target_meta.tables[table_name])
        diff_count = source_count - target_count
        source_total += source_count
        target_total += target_count
        print(
            f"{index:>3}  {table_name:<{table_width}}  {source_count:>10}  "
            f"{target_count:>10}  {diff_count:+8}"
        )
    total_diff = source_total - target_total
    print(
        f"{'tot':>3}  {'TOTAL':<{table_width}}  {source_total:>10}  "
        f"{target_total:>10}  {total_diff:+8}"
    )


def migrate(args: Args) -> int:
    """Execute full migration.

    Args:
        args: Parsed runtime args.

    Returns:
        Process exit code.
    """

    source_engine = create_db_engine(args.source_url, args.echo_sql)
    target_engine = create_db_engine(args.target_url, args.echo_sql)

    with source_engine.connect() as source_connection:
        run_sqlite_pragma_checks(source_connection)
        if source_connection.dialect.name == "sqlite":
            print("SQLite PRAGMA checks passed.")
        source_history_max_id = get_migratehistory_max_id(source_connection)
        with target_engine.connect() as target_connection:
            target_history_max_id = get_migratehistory_max_id(target_connection)
        if source_history_max_id != target_history_max_id:
            raise RuntimeError(
                "migratehistory max(id) mismatch: "
                f"source={source_history_max_id!r}, target={target_history_max_id!r}"
            )

        source_meta = reflect_tables(source_engine)
        target_meta = reflect_tables(target_engine)
        table_names = resolve_table_names(
            source_meta=source_meta,
            target_meta=target_meta,
            include_tables=args.tables,
            exclude_tables=args.exclude_tables,
        )
        table_order = table_dependency_order(source_meta, table_names)

        if args.list_table_order:
            print_order(table_order)
        if args.list_table_order:
            return 0
        if args.dry_run:
            with target_engine.connect() as target_connection:
                print_dry_run_counts(
                    source_connection=source_connection,
                    target_connection=target_connection,
                    source_meta=source_meta,
                    target_meta=target_meta,
                    table_order=table_order,
                )
            print("Dry-run finished. No data was copied.")
            return 0

        with target_engine.connect() as target_connection:
            current_tx = target_connection.begin()
            inserted_since_restart = 0
            totals = TableStats(table="__total__")
            try:
                if args.clean:
                    clean_target_tables(
                        connection=target_connection,
                        target_meta=target_meta,
                        table_order=table_order,
                    )
                    print("Target tables cleaned before import.")
                for table_name in table_order:
                    source_table = source_meta.tables[table_name]
                    target_table = target_meta.tables[table_name]
                    table_stats, inserted_since_restart, current_tx = migrate_table(
                        source_connection=source_connection,
                        target_connection=target_connection,
                        current_tx=current_tx,
                        source_table=source_table,
                        target_table=target_table,
                        args=args,
                        commit_since_restart=inserted_since_restart,
                    )
                    totals.scanned += table_stats.scanned
                    totals.inserted += table_stats.inserted
                    totals.skipped += table_stats.skipped
                    totals.failed_batches += table_stats.failed_batches
                    print(
                        f"{table_name}: scanned={table_stats.scanned} "
                        f"inserted={table_stats.inserted} skipped={table_stats.skipped} "
                        f"failed_batches={table_stats.failed_batches}"
                    )
                reset_postgresql_sequences(
                    connection=target_connection,
                    target_meta=target_meta,
                    table_names=table_order,
                )
                current_tx.commit()
            except Exception:
                current_tx.rollback()
                raise

        print(
            "Migration done: "
            f"scanned={totals.scanned} inserted={totals.inserted} "
            f"skipped={totals.skipped} failed_batches={totals.failed_batches}"
        )
        return 0


def main() -> int:
    """Program entrypoint.

    Returns:
        Exit code.
    """

    try:
        args = parse_args()
        return migrate(args)
    except KeyboardInterrupt:
        print("Interrupted.")
        return 130
    except Exception as error:  # pylint: disable=broad-except
        print(f"Error: {error}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
