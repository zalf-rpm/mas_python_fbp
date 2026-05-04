from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path
from typing import Any, cast

import geopandas as gpd
import pyarrow.parquet as pq
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
from zalfmas_fbp.components.dakis.dakis_process.relabel_geoparquet import RelabelGeoparquet
from zalfmas_fbp.components.dakis.dakis_process.relabel_geoparquet import meta as relabel_meta
from zalfmas_fbp.components.dakis.dakis_process.write_geoparquet import WriteGeoparquet
from zalfmas_fbp.components.dakis.dakis_process.write_geoparquet import meta as write_geoparquet_meta


def test_create_empty_raster_uses_default_config_and_writes_memory_file_bytes() -> None:
    component = CreateEmptyRaster(meta)

    writer = run_process_component(
        component,
        inputs={
            "in": [
                ip_message("[12.0, 52.0, 12.01, 52.01]"),
                done_message(),
            ],
        },
    ).output()

    assert component.config["epsg"].i64 == 25833
    assert component.config["resolution_m"].f64 == 100.0
    assert component.config["compression"].t == "zstd"
    assert len(writer.values) == 1

    raster_bytes = bytes(writer.values[0].content.as_struct(common_capnp.Value).d)
    with MemoryFile(raster_bytes) as memory_file, memory_file.open() as dataset:
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
    filtered = gpd.read_parquet(cast("Any", BytesIO(geoparquet_bytes)))
    assert filtered.crs is not None
    assert filtered.crs.to_epsg() == 25833
    assert filtered["id"].to_list() == [1, 3]
    assert _geo_metadata_from_bytes(geoparquet_bytes)["columns"]["geometry"]["crs"] is not None


def test_filter_geoparquet_by_raster_writes_geometries_without_optional_raster_output(tmp_path: Path) -> None:
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
    )

    geoparquet_bytes = bytes(result.output().values[0].content.as_struct(common_capnp.Value).d)
    filtered = gpd.read_parquet(cast("Any", BytesIO(geoparquet_bytes)))
    assert filtered["id"].to_list() == [1, 3]
    assert _geo_metadata_from_bytes(geoparquet_bytes)["columns"]["geometry"]["crs"] is not None


def test_relabel_geoparquet_maps_codes_drops_unmapped_rows_and_sets_default_priority(tmp_path: Path) -> None:
    mapping_path = tmp_path / "mapping.csv"
    mapping_path.write_text("code,lucode\n1,11\n2,12\n", encoding="utf-8")

    source = gpd.GeoDataFrame(
        {
            "code": [1, 2, 999],
            "extra": ["drop", "drop", "drop"],
            "geometry": [box(0, 0, 1, 1), box(2, 2, 3, 3), box(4, 4, 5, 5)],
        },
        crs="EPSG:25833",
    )
    source_bytes = _geoparquet_bytes(source)

    component = RelabelGeoparquet(relabel_meta)
    component.config["mapping_csv_path"] = common_capnp.Value.new_message(t=str(mapping_path))
    component.config["default_priority"] = common_capnp.Value.new_message(i64=7)

    result = run_process_component(
        component,
        inputs={"in": [_raster_message(source_bytes), done_message()]},
    )

    output_bytes = bytes(result.output().values[0].content.as_struct(common_capnp.Value).d)
    relabeled = gpd.read_parquet(cast("Any", BytesIO(output_bytes)))

    assert relabel_meta["component"]["defaultConfig"]["mapping_csv_path"]["value"] == (
        "resources/mappings/invekos_to_lulc.csv"
    )
    assert relabeled.columns.to_list() == ["lucode", "priority", "geometry"]
    assert relabeled["lucode"].to_list() == [11, 12]
    assert relabeled["priority"].to_list() == [7, 7]
    assert relabeled.crs is not None
    assert relabeled.crs.to_epsg() == 25833
    assert _geo_metadata_from_bytes(output_bytes)["columns"]["geometry"]["crs"] is not None


def test_relabel_geoparquet_uses_mapping_priority_column(tmp_path: Path) -> None:
    mapping_path = tmp_path / "mapping.csv"
    mapping_path.write_text("code,lucode,priority\n1,11,3\n2,12,4\n", encoding="utf-8")

    source = gpd.GeoDataFrame(
        {
            "code": [1, 2],
            "geometry": [box(0, 0, 1, 1), box(2, 2, 3, 3)],
        },
        crs="EPSG:25833",
    )
    component = RelabelGeoparquet(relabel_meta)
    component.config["mapping_csv_path"] = common_capnp.Value.new_message(t=str(mapping_path))

    result = run_process_component(
        component,
        inputs={"in": [_raster_message(_geoparquet_bytes(source)), done_message()]},
    )

    output_bytes = bytes(result.output().values[0].content.as_struct(common_capnp.Value).d)
    relabeled = gpd.read_parquet(cast("Any", BytesIO(output_bytes)))
    assert relabeled["priority"].to_list() == [3, 4]


def test_relabel_geoparquet_accepts_translation_csv_on_port() -> None:
    source = gpd.GeoDataFrame(
        {
            "code": [1, 2],
            "geometry": [box(0, 0, 1, 1), box(2, 2, 3, 3)],
        },
        crs="EPSG:25833",
    )
    component = RelabelGeoparquet(relabel_meta)

    result = run_process_component(
        component,
        inputs={
            "translation": [
                _raster_message(b"code,lucode,priority\n1,11,3\n2,12,4\n"),
                done_message(),
            ],
            "in": [_raster_message(_geoparquet_bytes(source)), done_message()],
        },
    )

    output_bytes = bytes(result.output().values[0].content.as_struct(common_capnp.Value).d)
    relabeled = gpd.read_parquet(cast("Any", BytesIO(output_bytes)))
    assert relabeled["lucode"].to_list() == [11, 12]
    assert relabeled["priority"].to_list() == [3, 4]


def test_write_geoparquet_writes_readable_file_to_configured_path(tmp_path: Path) -> None:
    output_path = tmp_path / "nested" / "out.parquet"
    geoparquet_bytes = _geoparquet_bytes(
        gpd.GeoDataFrame({"lucode": [11], "priority": [7], "geometry": [box(0, 0, 1, 1)]}, crs="EPSG:25833"),
    )
    component = WriteGeoparquet(write_geoparquet_meta)
    component.config["output_path"] = common_capnp.Value.new_message(t=str(output_path))

    run_process_component(
        component,
        inputs={"in": [_raster_message(geoparquet_bytes), done_message()]},
        outputs=(),
    )

    written = gpd.read_parquet(output_path)
    assert written["lucode"].to_list() == [11]
    assert written["priority"].to_list() == [7]
    assert written.crs is not None
    assert written.crs.to_epsg() == 25833
    assert _geo_metadata_from_path(output_path)["columns"]["geometry"]["crs"] is not None


def test_write_geoparquet_preserve_compression_writes_original_bytes(tmp_path: Path) -> None:
    output_path = tmp_path / "out.parquet"
    geoparquet_bytes = _geoparquet_bytes(
        gpd.GeoDataFrame({"lucode": [11], "priority": [7], "geometry": [box(0, 0, 1, 1)]}, crs="EPSG:25833"),
    )
    component = WriteGeoparquet(write_geoparquet_meta)
    component.config["output_path"] = common_capnp.Value.new_message(t=str(output_path))
    component.config["compression"] = common_capnp.Value.new_message(t="preserve")

    run_process_component(
        component,
        inputs={"in": [_raster_message(geoparquet_bytes), done_message()]},
        outputs=(),
    )

    assert output_path.read_bytes() == geoparquet_bytes


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


def _geoparquet_bytes(frame: gpd.GeoDataFrame) -> bytes:
    output = BytesIO()
    frame.to_parquet(output, index=False, write_covering_bbox=True)
    return output.getvalue()


def _geo_metadata_from_bytes(geoparquet_bytes: bytes) -> dict[str, Any]:
    metadata = pq.read_metadata(BytesIO(geoparquet_bytes)).metadata or {}
    return json.loads(metadata[b"geo"])


def _geo_metadata_from_path(path: Path) -> dict[str, Any]:
    metadata = pq.read_metadata(path).metadata or {}
    return json.loads(metadata[b"geo"])
