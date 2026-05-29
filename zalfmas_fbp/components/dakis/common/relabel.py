from __future__ import annotations

from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

from zalfmas_fbp.components.dakis.common.duckdb_utils import (
    connect,
    query_to_parquet_bytes,
    quote_identifier,
    relation_columns,
)


def relabel_geoparquet_bytes(
    geoparquet_bytes: bytes,
    *,
    mapping_csv_path: str | Path | None = None,
    mapping_csv_bytes: bytes | None = None,
    source_code_column: str,
    target_code_column: str,
    priority_column: str,
    default_priority: int,
) -> bytes:
    with TemporaryDirectory() as temp_dir:
        geoparquet_path = Path(temp_dir) / "input.parquet"
        geoparquet_path.write_bytes(geoparquet_bytes)
        mapping_path = _prepare_mapping_path(
            temp_dir=temp_dir,
            mapping_csv_path=mapping_csv_path,
            mapping_csv_bytes=mapping_csv_bytes,
        )

        with closing(connect()) as connection:
            geoparquet_relation = "SELECT * FROM parquet_scan(?)"
            geoparquet_columns = relation_columns(connection, geoparquet_relation, [str(geoparquet_path)])
            _require_columns(geoparquet_columns, [source_code_column, "geometry"], "GeoParquet")

            mapping_relation = "SELECT * FROM read_csv_auto(?)"
            mapping_columns = relation_columns(connection, mapping_relation, [str(mapping_path)])
            _require_columns(mapping_columns, [source_code_column, target_code_column], "mapping CSV")

            source_column = quote_identifier(source_code_column)
            target_column = quote_identifier(target_code_column)
            priority_alias = quote_identifier(priority_column)
            default_priority_value = int(default_priority)
            if priority_column in mapping_columns:
                priority_expression = f"COALESCE(mapping.{quote_identifier(priority_column)}, {default_priority_value}) AS {priority_alias}"
            else:
                priority_expression = f"{default_priority_value} AS {priority_alias}"

            query = f"""
                SELECT
                    mapping.{target_column} AS {target_column},
                    {priority_expression},
                    geometries.geometry
                FROM parquet_scan(?) AS geometries
                LEFT JOIN read_csv_auto(?) AS mapping
                    ON geometries.{source_column} = mapping.{source_column}
                WHERE mapping.{target_column} IS NOT NULL
            """
            return query_to_parquet_bytes(
                connection,
                query,
                [str(geoparquet_path), str(mapping_path)],
            )


def _require_columns(available_columns: set[str], columns: list[str], label: str) -> None:
    missing = [column for column in columns if column not in available_columns]
    if missing:
        msg = f"{label} is missing required column(s): {', '.join(missing)}"
        raise ValueError(msg)


def _prepare_mapping_path(
    *,
    temp_dir: str,
    mapping_csv_path: str | Path | None,
    mapping_csv_bytes: bytes | None,
) -> Path:
    if mapping_csv_bytes is not None:
        mapping_path = Path(temp_dir) / "mapping.csv"
        mapping_path.write_bytes(mapping_csv_bytes)
        return mapping_path
    if mapping_csv_path is not None:
        return Path(mapping_csv_path)

    msg = "Either mapping_csv_path or mapping_csv_bytes must be provided."
    raise ValueError(msg)
