from __future__ import annotations

from collections.abc import Iterable
from io import BytesIO
from pathlib import Path
from tempfile import NamedTemporaryFile

import geopandas as gpd
import pandas as pd


def merge_relabel_geoparquet_bytes(
    geoparquet_parts: Iterable[bytes],
    *,
    code_column: str,
    priority_column: str,
) -> bytes:
    frames = [_read_geoparquet_bytes(part) for part in geoparquet_parts]
    if not frames:
        msg = "At least one GeoParquet input is required."
        raise ValueError(msg)

    for frame in frames:
        _require_columns(frame, [code_column, priority_column, "geometry"])

    target_crs = frames[0].crs
    if target_crs is None:
        msg = "GeoParquet geometries do not define a CRS."
        raise ValueError(msg)

    normalized_frames = [frame if frame.crs == target_crs else frame.to_crs(target_crs) for frame in frames]
    merged = gpd.GeoDataFrame(
        pd.concat(normalized_frames, ignore_index=True),
        geometry="geometry",
        crs=target_crs,
    )
    merged = merged[[code_column, priority_column, "geometry"]]

    output = BytesIO()
    merged.to_parquet(output, index=False, write_covering_bbox=True)
    return output.getvalue()


def _require_columns(frame: gpd.GeoDataFrame, columns: list[str]) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        msg = f"GeoParquet is missing required column(s): {', '.join(missing)}"
        raise ValueError(msg)


def _read_geoparquet_bytes(data: bytes) -> gpd.GeoDataFrame:
    # the file has to be closed before geopandas/pyarrow can reopen it by path on Windows
    with NamedTemporaryFile(suffix=".parquet", delete=False) as file:
        file.write(data)
        path = Path(file.name)
    try:
        return gpd.read_parquet(path)
    finally:
        path.unlink(missing_ok=True)
