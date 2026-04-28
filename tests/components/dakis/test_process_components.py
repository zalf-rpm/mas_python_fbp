from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any, cast

import geopandas as gpd
from mas.schema.common import common_capnp
from rasterio.crs import CRS
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from shapely.geometry import box

from tests.component_harness import done_message, ip_message, run_process_component
from zalfmas_fbp.components.dakis.dakis_process.create_empty_raster import CreateEmptyRaster, meta
from zalfmas_fbp.components.dakis.dakis_process.filter_geoparquet_by_raster import (
    FilterGeoparquetByRaster,
)
from zalfmas_fbp.components.dakis.dakis_process.filter_geoparquet_by_raster import (
    meta as filter_geoparquet_meta,
)


def test_create_empty_raster_uses_default_config_and_writes_memory_file_bytes() -> None:
    component = CreateEmptyRaster(meta)

    writer = run_process_component(
        component,
        inputs={
            "in": [
                ip_message("[12.0, 52.0, 12.01, 52.01]"),
                done_message(),
            ]
        },
    ).output()

    assert component.config["epsg"].i64 == 25833
    assert component.config["resolution_m"].f64 == 100.0
    assert component.config["compression"].t == "zstd"
    assert len(writer.values) == 1

    raster_bytes = bytes(writer.values[0].content.as_struct(common_capnp.Value).d)
    with MemoryFile(raster_bytes) as memory_file:
        with memory_file.open() as dataset:
            assert dataset.crs.to_epsg() == 25833
            assert dataset.res == (100.0, 100.0)
            assert dataset.count == 1
            assert dataset.compression.value == "ZSTD"


def test_filter_geoparquet_by_raster_writes_overlapping_geometries_and_raster(tmp_path: Path) -> None:
    source_path = tmp_path / "source.parquet"
    source = gpd.GeoDataFrame(
        {
            "id": [1, 2, 3],
            "geometry": [
                box(2, 2, 4, 4),
                box(20, 20, 22, 22),
                box(8, 8, 12, 12),
            ],
        },
        crs="EPSG:25833",
    )
    source.to_parquet(source_path, index=False, write_covering_bbox=True)

    raster_bytes = _test_raster_bytes()
    component = FilterGeoparquetByRaster(filter_geoparquet_meta)
    component.config["geoparquet_path"] = common_capnp.Value.new_message(t=str(source_path))

    result = run_process_component(
        component,
        inputs={"in": [_raster_message(raster_bytes), done_message()]},
        outputs=("out", "raster"),
    )

    assert filter_geoparquet_meta["component"]["defaultConfig"]["geoparquet_path"]["value"] == (
        "resources/invekos_optimized.parquet"
    )
    assert bytes(result.output("raster").values[0].content.as_struct(common_capnp.Value).d) == raster_bytes

    geoparquet_bytes = bytes(result.output("out").values[0].content.as_struct(common_capnp.Value).d)
    filtered = gpd.read_parquet(cast(Any, BytesIO(geoparquet_bytes)))
    assert filtered.crs is not None
    assert filtered.crs.to_epsg() == 25833
    assert filtered["id"].to_list() == [1, 3]


def _test_raster_bytes() -> bytes:
    with MemoryFile() as memory_file:
        with memory_file.open(
            driver="GTiff",
            width=10,
            height=10,
            count=1,
            dtype="uint8",
            crs=CRS.from_epsg(25833),
            transform=from_origin(0, 10, 1, 1),
            nodata=0,
            compress="zstd",
        ):
            pass
        return bytes(memory_file.read())


def _raster_message(raster_bytes: bytes):
    return ip_message(common_capnp.Value.new_message(d=raster_bytes))
