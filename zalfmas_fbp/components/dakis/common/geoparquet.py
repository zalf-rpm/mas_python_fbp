from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path
from typing import Any, cast

import geopandas as gpd
import pandas as pd
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from rasterio.crs import CRS
from rasterio.io import MemoryFile
from rasterio.warp import transform_bounds
from shapely import from_wkb
from shapely.geometry import box

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

    table = _read_bbox_candidates(path, query_bounds)
    if table.num_rows == 0:
        return _empty_geoparquet(path, parquet_crs)

    frame = table.drop(["bbox"]).to_pandas() if "bbox" in table.column_names else table.to_pandas()
    if "geometry" not in frame.columns:
        msg = f"GeoParquet file has no geometry column: {path}"
        raise ValueError(msg)

    frame["geometry"] = from_wkb(frame["geometry"])
    geometry_frame = gpd.GeoDataFrame(frame, geometry="geometry", crs=parquet_crs)
    query_geometry = box(query_bounds[0], query_bounds[1], query_bounds[2], query_bounds[3])
    geometry_frame = cast(gpd.GeoDataFrame, geometry_frame[geometry_frame.geometry.intersects(query_geometry)])

    return _write_geoparquet(geometry_frame)


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


def _read_bbox_candidates(path: Path, bounds: Bounds) -> Any:
    dataset = ds.dataset(path, format="parquet")
    min_x, min_y, max_x, max_y = bounds

    if _has_covering_bbox(dataset.schema):
        overlap = (
            (pc.field(("bbox", "xmin")) <= max_x)
            & (pc.field(("bbox", "xmax")) >= min_x)
            & (pc.field(("bbox", "ymin")) <= max_y)
            & (pc.field(("bbox", "ymax")) >= min_y)
        )
        return dataset.to_table(filter=overlap)

    return dataset.to_table()


def _has_covering_bbox(schema: Any) -> bool:
    if "bbox" not in schema.names:
        return False
    bbox_type = schema.field("bbox").type
    return all(name in bbox_type for name in ("xmin", "ymin", "xmax", "ymax"))


def _empty_geoparquet(source_path: Path, crs: CRS) -> bytes:
    schema = pq.read_schema(source_path)
    columns: dict[str, Any] = {}
    for field in schema:
        if field.name in ("geometry", "bbox"):
            continue
        columns[field.name] = pd.Series(dtype=field.type.to_pandas_dtype())
    return _write_geoparquet(gpd.GeoDataFrame(columns, geometry=[], crs=crs))


def _write_geoparquet(frame: gpd.GeoDataFrame) -> bytes:
    output = BytesIO()
    frame.to_parquet(output, index=False, write_covering_bbox=True)
    return output.getvalue()
