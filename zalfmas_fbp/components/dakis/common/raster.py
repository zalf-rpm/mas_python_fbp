from __future__ import annotations

import json
import math
from typing import Any, cast

from geojson_pydantic import FeatureCollection
from geojson_pydantic.types import BBox
from pydantic import ValidationError
from rasterio.crs import CRS
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from rasterio.warp import transform_bounds

type BBox2D = tuple[float, float, float, float]


def parse_geojson_bbox(value: str) -> BBox2D:
    text = value.strip()
    try:
        bbox = _validate_bbox(json.loads(text))
    except json.JSONDecodeError:
        bbox = _validate_bbox([float(part.strip()) for part in text.split(",")])

    offset = len(bbox) // 2
    west = float(bbox[0])
    south = float(bbox[1])
    east = float(bbox[offset])
    north = float(bbox[offset + 1])

    if west > east:
        msg = "BBOX values crossing the antimeridian are not supported for raster creation."
        raise ValueError(msg)

    return west, south, east, north


def _validate_bbox(value: Any) -> BBox:
    try:
        collection = FeatureCollection(type="FeatureCollection", features=[], bbox=value)
    except ValidationError:
        msg = "Invalid GeoJSON bbox."
        raise ValueError(msg) from None

    bbox = collection.bbox
    if bbox is None:
        msg = "Missing GeoJSON bbox."
        raise ValueError(msg)

    return cast("BBox", bbox)


def create_empty_raster_bytes(
    bbox: BBox2D,
    *,
    epsg: int,
    resolution_m: float,
    compression: str,
) -> bytes:
    if resolution_m <= 0:
        msg = "resolution_m must be greater than zero."
        raise ValueError(msg)

    dst_crs = CRS.from_epsg(epsg)
    min_x, min_y, max_x, max_y = transform_bounds("EPSG:4326", dst_crs, *bbox, densify_pts=21)
    if min_x >= max_x or min_y >= max_y:
        msg = "BBOX does not produce a valid raster extent."
        raise ValueError(msg)

    width = max(1, math.ceil((max_x - min_x) / resolution_m))
    height = max(1, math.ceil((max_y - min_y) / resolution_m))
    transform = from_origin(min_x, max_y, resolution_m, resolution_m)

    with MemoryFile() as memory_file:
        with memory_file.open(
            driver="GTiff",
            width=width,
            height=height,
            count=1,
            dtype="uint8",
            crs=dst_crs,
            transform=transform,
            nodata=0,
            compress=compression,
            sparse_ok=True,
            bigtiff="IF_SAFER",
        ):
            pass

        return bytes(memory_file.read())
