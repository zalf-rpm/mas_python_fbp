from __future__ import annotations

import json
from contextlib import closing
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from rasterio.crs import CRS
from rasterio.io import MemoryFile
from rasterio.warp import transform_bounds

from zalfmas_fbp.components.dakis.common.duckdb_utils import connect, query_to_parquet_bytes, relation_columns

type Bounds = tuple[float, float, float, float]


def raster_bounds_and_crs(raster_bytes: bytes) -> tuple[Bounds, CRS]:
    with MemoryFile(raster_bytes) as memory_file:
        with memory_file.open() as dataset:
            if dataset.crs is None:
                msg = "Raster does not define a CRS."
                raise ValueError(msg)
            bounds = dataset.bounds
            return (bounds.left, bounds.bottom, bounds.right, bounds.top), dataset.crs


def overlapping_geometries_as_geoparquet(raster_bytes: bytes, geoparquet_path: str | Path) -> bytes:
    path = Path(geoparquet_path)
    bounds, raster_crs = raster_bounds_and_crs(raster_bytes)
    parquet_crs = geoparquet_crs(path) or raster_crs
    query_bounds = bounds
    if raster_crs != parquet_crs:
        query_bounds = transform_bounds(raster_crs, parquet_crs, *bounds, densify_pts=21)

    with closing(connect(load_spatial=True)) as connection:
        relation_query = "SELECT * FROM parquet_scan(?)"
        columns = relation_columns(connection, relation_query, [str(path)])
        if "geometry" not in columns:
            msg = f"GeoParquet file has no geometry column: {path}"
            raise ValueError(msg)

        filters: list[str] = []
        params: list[Any] = [str(path)]
        if "bbox" in columns:
            filters.append(
                "bbox.xmin <= ? AND bbox.xmax >= ? AND bbox.ymin <= ? AND bbox.ymax >= ?"
            )
            params.extend([query_bounds[2], query_bounds[0], query_bounds[3], query_bounds[1]])

        filters.append("ST_Intersects(geometry, ST_GeomFromText(?))")
        params.append(_bounds_as_polygon_wkt(query_bounds))

        query = relation_query
        if filters:
            query += " WHERE " + " AND ".join(filters)

        return query_to_parquet_bytes(connection, query, params)


def geoparquet_crs(path: str | Path) -> CRS | None:
    metadata = pq.read_metadata(path).metadata or {}
    geo_metadata = metadata.get(b"geo")
    if geo_metadata is None:
        return None

    geo = json.loads(geo_metadata)
    primary_column = geo.get("primary_column", "geometry")
    column = geo.get("columns", {}).get(primary_column, {})
    crs = column.get("crs")
    return CRS.from_user_input(crs) if crs else None


def _bounds_as_polygon_wkt(bounds: Bounds) -> str:
    min_x, min_y, max_x, max_y = bounds
    return (
        f"POLYGON(({min_x} {min_y}, {max_x} {min_y}, {max_x} {max_y}, "
        f"{min_x} {max_y}, {min_x} {min_y}))"
    )
