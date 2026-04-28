from __future__ import annotations

import os
from io import BytesIO
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Literal, cast

import geopandas as gpd

type ParquetCompression = Literal["snappy", "gzip", "brotli", "lz4", "zstd"]

_SUPPORTED_COMPRESSIONS: set[str] = {"snappy", "gzip", "brotli", "lz4", "zstd"}


def write_geoparquet_bytes(
    geoparquet_bytes: bytes,
    *,
    output_path: str | Path,
    compression: str,
) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = _maybe_recompress(geoparquet_bytes, compression)

    with NamedTemporaryFile("wb", dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)
        tmp_file.write(data)

    try:
        os.replace(tmp_path, path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

    return path


def _maybe_recompress(geoparquet_bytes: bytes, compression: str) -> bytes:
    frame = _read_geoparquet(geoparquet_bytes)
    normalized = compression.strip().lower()
    if normalized in ("", "preserve", "none"):
        return geoparquet_bytes
    if normalized not in _SUPPORTED_COMPRESSIONS:
        msg = f"Unsupported GeoParquet compression: {compression}"
        raise ValueError(msg)

    output = BytesIO()
    frame.to_parquet(output, index=False, compression=cast(ParquetCompression, normalized), write_covering_bbox=True)
    return output.getvalue()


def _read_geoparquet(geoparquet_bytes: bytes) -> gpd.GeoDataFrame:
    return gpd.read_parquet(cast(Any, BytesIO(geoparquet_bytes)))
