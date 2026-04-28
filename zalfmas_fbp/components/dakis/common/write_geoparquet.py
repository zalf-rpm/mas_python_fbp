from __future__ import annotations

import os
from contextlib import closing
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Literal

from zalfmas_fbp.components.dakis.common.duckdb_utils import (
    connect,
    normalize_parquet_compression,
    query_to_parquet_bytes,
)

type ParquetCompression = Literal["snappy", "gzip", "brotli", "lz4", "zstd"]


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
    normalized = compression.strip().lower()
    if normalized in ("", "preserve", "none"):
        return geoparquet_bytes
    normalized = normalize_parquet_compression(normalized)

    with TemporaryDirectory() as temp_dir:
        input_path = Path(temp_dir) / "input.parquet"
        input_path.write_bytes(geoparquet_bytes)
        with closing(connect()) as connection:
            return query_to_parquet_bytes(
                connection,
                "SELECT * FROM parquet_scan(?)",
                [str(input_path)],
                compression=normalized,
            )
