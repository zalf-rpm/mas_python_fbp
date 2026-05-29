from __future__ import annotations

from tempfile import NamedTemporaryFile

import geopandas as gpd
import numpy as np
from rasterio.features import rasterize
from rasterio.io import MemoryFile


def burn_geoparquet_on_raster_bytes(
    raster_bytes: bytes,
    geoparquet_bytes: bytes,
    *,
    code_column: str,
    priority_column: str,
    all_touched: bool,
    compression: str | None = None,
) -> bytes:
    geometries = _read_geoparquet_bytes(geoparquet_bytes)
    _require_columns(geometries, [code_column, priority_column, "geometry"])

    if geometries.empty:
        return raster_bytes

    with MemoryFile(raster_bytes) as source_memory_file, source_memory_file.open() as source:
        if source.crs is None:
            msg = "Raster does not define a CRS."
            raise ValueError(msg)
        if geometries.crs is None:
            msg = "GeoParquet geometries do not define a CRS."
            raise ValueError(msg)

        burn_geometries = geometries.to_crs(source.crs) if geometries.crs != source.crs else geometries
        burn_geometries = burn_geometries.dropna(subset=[code_column, priority_column, "geometry"])
        if burn_geometries.empty:
            return raster_bytes

        profile = source.profile.copy()
        band = source.read(1)
        dtype = _dtype_for_codes(band.dtype, burn_geometries[code_column].to_numpy())
        if dtype != band.dtype:
            band = band.astype(dtype, copy=False)
            profile["dtype"] = np.dtype(dtype).name

        sorted_geometries = burn_geometries.sort_values(priority_column, kind="stable")
        shapes = (
            (geometry, int(code))
            for geometry, code in zip(sorted_geometries.geometry, sorted_geometries[code_column], strict=True)
        )

        rasterize(
            shapes,
            out=band,
            transform=source.transform,
            fill=0,
            all_touched=all_touched,
        )

        if compression == "none":
            profile.pop("compress", None)
        elif compression is not None:
            profile["compress"] = compression

        with MemoryFile() as output_memory_file:
            with output_memory_file.open(**profile) as output:
                output.write(band, 1)
            return bytes(output_memory_file.read())


def _require_columns(frame: gpd.GeoDataFrame, columns: list[str]) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        msg = f"GeoParquet is missing required column(s): {', '.join(missing)}"
        raise ValueError(msg)


def _read_geoparquet_bytes(data: bytes) -> gpd.GeoDataFrame:
    with NamedTemporaryFile(suffix=".parquet") as file:
        file.write(data)
        file.flush()
        return gpd.read_parquet(file.name)


def _dtype_for_codes(source_dtype: np.dtype, values: np.ndarray) -> np.dtype:
    dtype = np.dtype(source_dtype)
    if not np.issubdtype(dtype, np.integer):
        return dtype

    if values.size == 0:
        return dtype

    min_value = int(np.nanmin(values))
    max_value = int(np.nanmax(values))
    if np.issubdtype(dtype, np.unsignedinteger) and min_value >= 0:
        if max_value <= np.iinfo(dtype).max:
            return dtype
        if max_value <= np.iinfo(np.uint16).max:
            return np.dtype("uint16")
        return np.dtype("uint32")

    if np.iinfo(dtype).min <= min_value and max_value <= np.iinfo(dtype).max:
        return dtype
    return np.dtype("int32")
