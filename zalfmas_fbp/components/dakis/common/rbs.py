from __future__ import annotations

import json
import logging
import secrets
import socket
import tempfile
import time
import urllib.error
from collections.abc import Sequence
from contextlib import closing, suppress
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
from owslib.feature.wfs200 import WebFeatureService_2_0_0
from pyproj import CRS as PyprojCRS
from rasterio.crs import CRS as RasterioCRS
from rasterio.warp import transform_bounds

from zalfmas_fbp.components.dakis.common.duckdb_utils import (
    connect,
    query_to_parquet_bytes,
    quote_identifier,
    relation_columns,
)
from zalfmas_fbp.components.dakis.common.geoparquet import raster_bounds_and_crs

type Bounds = tuple[float, float, float, float]

RBS_WFS_URL = "https://isk.geobasis-bb.de/ows/alkis_sf_wfs"
RBS_LAYER = "adv:AX_Bodenschaetzung"
RBS_WFS_SRS = "urn:ogc:def:crs:EPSG::25833"
RBS_EPSG = 25833
RBS_COLUMNS: tuple[str, ...] = (
    "gml_id",
    "geometry",
    "nutzungsart",
    "bodenart",
    "zustandsstufe",
    "bodenzahlOderGruenlandgrundzahl",
    "ackerzahlOderGruenlandzahl",
    "bodenstufe",
)
RBS_INTEGER_COLUMNS: frozenset[str] = frozenset(RBS_COLUMNS) - {"gml_id", "geometry"}

_SECURE_RANDOM = secrets.SystemRandom()

_transient_download_error_types: list[type[BaseException]] = [
    TimeoutError,
    socket.timeout,
    urllib.error.URLError,
]

try:
    import requests

    _transient_download_error_types.extend(
        [
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ],
    )
except ImportError:
    pass

try:
    import urllib3

    _transient_download_error_types.extend(
        [
            urllib3.exceptions.ReadTimeoutError,
            urllib3.exceptions.ConnectTimeoutError,
            urllib3.exceptions.ProtocolError,
        ],
    )
except ImportError:
    pass

_TRANSIENT_DOWNLOAD_ERROR_TYPES: tuple[type[BaseException], ...] = tuple(
    _transient_download_error_types,
)


@dataclass(slots=True, frozen=True)
class RetryConfig:
    max_cell_retries: int = 3
    retry_backoff_base_s: float = 1.0
    retry_backoff_factor: float = 2.0
    retry_max_delay_s: float = 30.0
    retry_jitter_s: float = 0.25


@dataclass(slots=True, frozen=True)
class RBSFetchConfig:
    grid_size: int = 4
    retry: RetryConfig = RetryConfig()


def rbs_for_raster_as_geoparquet(
    raster_bytes: bytes,
    *,
    config: RBSFetchConfig | None = None,
    logger: logging.Logger | None = None,
) -> bytes:
    resolved_config = config or RBSFetchConfig()
    resolved_logger = logger or logging.getLogger(__name__)
    bounds, raster_crs = raster_bounds_and_crs(raster_bytes)
    query_bounds = _bounds_in_rbs_crs(bounds, raster_crs)

    wfs = WebFeatureService_2_0_0(RBS_WFS_URL, version="2.0.0")
    with tempfile.TemporaryDirectory() as temp_dir:
        gml_files = _download_rbs_features(
            wfs,
            _grid_boxes(query_bounds, resolved_config.grid_size),
            Path(temp_dir),
            resolved_config.retry,
            resolved_logger,
        )
        return rbs_gml_files_to_geoparquet_bytes(gml_files)


def rbs_gml_files_to_geoparquet_bytes(gml_files: Sequence[Path]) -> bytes:
    with closing(connect(load_spatial=True)) as connection:
        if not gml_files:
            return _empty_rbs_geoparquet_bytes(connection)

        selects: list[str] = []
        params: list[str] = []
        for gml_file in gml_files:
            columns = relation_columns(connection, "SELECT * FROM ST_Read(?)", [str(gml_file)])
            if "geometry" not in columns:
                msg = f"RBS GML file has no geometry column: {gml_file}"
                raise ValueError(msg)
            if "gml_id" not in columns:
                msg = f"RBS GML file has no gml_id column: {gml_file}"
                raise ValueError(msg)

            selects.append(_st_read_select(columns))
            params.append(str(gml_file))

        query = f"""
            WITH source AS (
                {" UNION ALL ".join(selects)}
            ),
            cleaned AS (
                SELECT {", ".join(quote_identifier(column) for column in RBS_COLUMNS)}
                FROM source
                WHERE geometry IS NOT NULL AND gml_id IS NOT NULL
            ),
            deduped AS (
                SELECT
                    *,
                    row_number() OVER (PARTITION BY ST_AsWKB(geometry) ORDER BY gml_id) AS rn
                FROM cleaned
            )
            SELECT {", ".join(quote_identifier(column) for column in RBS_COLUMNS)}
            FROM deduped
            WHERE rn = 1
        """
        return query_to_parquet_bytes(connection, query, params)


def _bounds_in_rbs_crs(bounds: Bounds, raster_crs: RasterioCRS) -> Bounds:
    target_crs = f"EPSG:{RBS_EPSG}"
    if raster_crs.to_string() == target_crs or raster_crs.to_epsg() == RBS_EPSG:
        return bounds
    return transform_bounds(raster_crs, target_crs, *bounds, densify_pts=21)


def _grid_boxes(bounds: Bounds, grid_size: int) -> list[Bounds]:
    if grid_size < 1:
        msg = "RBS grid size must be at least 1."
        raise ValueError(msg)

    min_x, min_y, max_x, max_y = bounds
    x_incr = (max_x - min_x) / grid_size
    y_incr = (max_y - min_y) / grid_size
    return [
        (
            min_x + x_incr * i,
            min_y + y_incr * j,
            min_x + x_incr * (i + 1),
            min_y + y_incr * (j + 1),
        )
        for i in range(grid_size)
        for j in range(grid_size)
    ]


def _download_rbs_features(
    wfs: WebFeatureService_2_0_0,
    boxes: Sequence[Bounds],
    temp_path: Path,
    retry_config: RetryConfig,
    logger: logging.Logger,
) -> list[Path]:
    gml_files: list[Path] = []
    failed_cells: list[int] = []
    getfeature = getattr(wfs, "getfeature", None)
    if not callable(getfeature):
        msg = "WFS client does not expose getfeature()."
        raise TypeError(msg)

    for index, bounds in enumerate(boxes):
        logger.info("Fetching RBS grid cell %s/%s", index + 1, len(boxes))
        attempt = 0
        content = b""
        while True:
            feature: object | None = None
            try:
                feature = getfeature(
                    typename=[RBS_LAYER],
                    bbox=bounds,
                    srsname=RBS_WFS_SRS,
                )
                content = _read_feature_content(feature)
                break
            except Exception as exc:
                if not _is_transient_download_error(exc):
                    raise

                attempt += 1
                if attempt > retry_config.max_cell_retries:
                    logger.exception(
                        "Giving up RBS grid cell %s after %s retries",
                        index + 1,
                        retry_config.max_cell_retries,
                    )
                    failed_cells.append(index + 1)
                    break

                delay = min(
                    retry_config.retry_max_delay_s,
                    retry_config.retry_backoff_base_s * (retry_config.retry_backoff_factor ** (attempt - 1)),
                )
                delay = max(0.0, delay + _SECURE_RANDOM.uniform(0.0, retry_config.retry_jitter_s))
                logger.warning(
                    "Retrying RBS grid cell %s/%s (attempt %s/%s) after error: %s (sleep %.2fs)",
                    index + 1,
                    len(boxes),
                    attempt,
                    retry_config.max_cell_retries,
                    exc,
                    delay,
                )
                time.sleep(delay)
            finally:
                close = getattr(feature, "close", None)
                if callable(close):
                    with suppress(Exception):
                        close()

        if b"<wfs:member>" not in content and b"<gml:featureMember>" not in content:
            logger.debug("No RBS features found in grid cell %s", index + 1)
            continue

        filepath = temp_path / f"rbs_{index}.gml"
        filepath.write_bytes(content)
        gml_files.append(filepath)

    if failed_cells:
        logger.warning(
            "Failed to fetch %s/%s RBS grid cells after retries: %s",
            len(failed_cells),
            len(boxes),
            failed_cells,
        )
    return gml_files


def _read_feature_content(feature: object) -> bytes:
    read_fn = getattr(feature, "read", None)
    if not callable(read_fn):
        msg = "WFS feature response does not expose read()."
        raise TypeError(msg)
    content = read_fn()
    if not isinstance(content, bytes):
        msg = f"Expected bytes from WFS feature response, got {type(content)!r}."
        raise TypeError(msg)
    return content


def _is_transient_download_error(exc: BaseException) -> bool:
    if isinstance(exc, _TRANSIENT_DOWNLOAD_ERROR_TYPES):
        return True

    message = str(exc).lower()
    return "timed out" in message or "timeout" in message or "temporarily unavailable" in message


def _st_read_select(columns: set[str]) -> str:
    expressions: list[str] = []
    for column in RBS_COLUMNS:
        quoted = quote_identifier(column)
        if column == "geometry":
            expressions.append(f"{quoted} AS {quoted}")
        elif column in RBS_INTEGER_COLUMNS:
            if column in columns:
                expressions.append(f"try_cast({quoted} AS INTEGER) AS {quoted}")
            else:
                expressions.append(f"NULL::INTEGER AS {quoted}")
        elif column in columns:
            expressions.append(f"try_cast({quoted} AS VARCHAR) AS {quoted}")
        else:
            expressions.append(f"NULL::VARCHAR AS {quoted}")

    return f"SELECT {', '.join(expressions)} FROM ST_Read(?)"


def _empty_rbs_geoparquet_bytes(connection: duckdb.DuckDBPyConnection) -> bytes:
    expressions = [
        "''::VARCHAR AS gml_id",
        "ST_GeomFromText('POINT EMPTY') AS geometry",
        *(f"NULL::INTEGER AS {quote_identifier(column)}" for column in RBS_COLUMNS if column in RBS_INTEGER_COLUMNS),
    ]
    query = f"SELECT {', '.join(expressions)} WHERE false"
    return _with_geo_metadata(query_to_parquet_bytes(connection, query))


def _with_geo_metadata(parquet_bytes: bytes) -> bytes:
    table = pq.read_table(BytesIO(parquet_bytes))
    metadata = dict(table.schema.metadata or {})
    metadata[b"geo"] = json.dumps(
        {
            "version": "1.0.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "crs": PyprojCRS.from_epsg(RBS_EPSG).to_json_dict(),
                    "geometry_types": [],
                },
            },
            "creator": {"library": "zalfmas-fbp"},
        },
    ).encode()

    output = BytesIO()
    pq.write_table(table.replace_schema_metadata(metadata), output, compression="zstd")
    return output.getvalue()
