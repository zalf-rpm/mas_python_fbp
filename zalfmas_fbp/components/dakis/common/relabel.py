from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any, cast

import geopandas as gpd
import pandas as pd


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
    geometries = gpd.read_parquet(cast(Any, BytesIO(geoparquet_bytes)))
    mapping = _read_mapping(mapping_csv_path=mapping_csv_path, mapping_csv_bytes=mapping_csv_bytes)

    _require_columns(geometries, [source_code_column, "geometry"], "GeoParquet")
    _require_columns(mapping, [source_code_column, target_code_column], "mapping CSV")

    mapping_columns = [source_code_column, target_code_column]
    if priority_column in mapping.columns:
        mapping_columns.append(priority_column)

    joined = geometries.merge(mapping[mapping_columns], on=source_code_column, how="left")
    joined = joined[joined[target_code_column].notna()]

    if priority_column not in joined.columns:
        joined[priority_column] = default_priority
    else:
        priority = cast(pd.Series, joined[priority_column])
        joined[priority_column] = priority.fillna(default_priority)

    output = gpd.GeoDataFrame(
        joined[[target_code_column, priority_column, "geometry"]],
        geometry="geometry",
        crs=geometries.crs,
    )

    buffer = BytesIO()
    output.to_parquet(buffer, index=False, write_covering_bbox=True)
    return buffer.getvalue()


def _require_columns(frame: pd.DataFrame, columns: list[str], label: str) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        msg = f"{label} is missing required column(s): {', '.join(missing)}"
        raise ValueError(msg)


def _read_mapping(*, mapping_csv_path: str | Path | None, mapping_csv_bytes: bytes | None) -> pd.DataFrame:
    if mapping_csv_bytes is not None:
        return pd.read_csv(BytesIO(mapping_csv_bytes))
    if mapping_csv_path is not None:
        return pd.read_csv(mapping_csv_path)

    msg = "Either mapping_csv_path or mapping_csv_bytes must be provided."
    raise ValueError(msg)
