from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import duckdb

type QueryParams = Sequence[Any]

SUPPORTED_COMPRESSIONS: set[str] = {"snappy", "gzip", "brotli", "lz4", "zstd"}


def connect(*, load_spatial: bool = False) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(database=":memory:")
    if load_spatial:
        try:
            connection.load_extension("spatial")
        except duckdb.Error:
            connection.install_extension("spatial")
            connection.load_extension("spatial")
    return connection


def normalize_parquet_compression(compression: str) -> str:
    normalized = compression.strip().lower()
    if normalized not in SUPPORTED_COMPRESSIONS:
        msg = f"Unsupported GeoParquet compression: {compression}"
        raise ValueError(msg)
    return normalized


def quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def relation_columns(
    connection: duckdb.DuckDBPyConnection,
    relation_query: str,
    params: QueryParams | None = None,
) -> set[str]:
    return {
        row[0]
        for row in connection.execute(
            f"DESCRIBE {relation_query}",
            [] if params is None else list(params),
        ).fetchall()
    }


def query_to_parquet_bytes(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    params: QueryParams | None = None,
    *,
    compression: str = "zstd",
) -> bytes:
    with TemporaryDirectory() as temp_dir:
        output_path = Path(temp_dir) / "result.parquet"
        copy_query_to_parquet(
            connection,
            query,
            output_path,
            params=params,
            compression=compression,
        )
        return output_path.read_bytes()


def copy_query_to_parquet(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    output_path: str | Path,
    params: QueryParams | None = None,
    *,
    compression: str = "zstd",
) -> None:
    normalized = normalize_parquet_compression(compression)
    escaped_output_path = str(output_path).replace("'", "''")
    connection.execute(
        f"COPY ({query}) TO '{escaped_output_path}' (FORMAT parquet, COMPRESSION {normalized})",
        [] if params is None else list(params),
    )
