from __future__ import annotations

import asyncio
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
from zalfmas_common import common

from tests.component_harness import (
    InMemoryReader,
    InMemoryWriter,
    PortMessage,
    PortValue,
    done_message,
    ip_message,
    run_process_component,
)
from zalfmas_fbp.components.dakis.burn_geoparquet_on_raster import (
    METADATA as burn_geoparquet_metadata,
)
from zalfmas_fbp.components.dakis.burn_geoparquet_on_raster import (
    BurnGeoparquetOnRaster,
)
from zalfmas_fbp.components.dakis.common.file_payload import (
    prepared_file_bracket_ip,
    prepared_file_chunk_ips,
    prepared_file_ip,
)
from zalfmas_fbp.components.dakis.create_empty_raster import (
    METADATA as create_empty_raster_metadata,
)
from zalfmas_fbp.components.dakis.create_empty_raster import (
    CreateEmptyRaster,
)
from zalfmas_fbp.components.dakis.filter_geoparquet_by_raster import (
    METADATA as filter_geoparquet_metadata,
)
from zalfmas_fbp.components.dakis.filter_geoparquet_by_raster import (
    FilterGeoparquetByRaster,
)
from zalfmas_fbp.components.dakis.merge_geoparquet import (
    METADATA as merge_geoparquet_metadata,
)
from zalfmas_fbp.components.dakis.merge_geoparquet import (
    MergeGeoparquet,
)
from zalfmas_fbp.components.dakis.read_file_from_disk import (
    METADATA as read_file_from_disk_metadata,
)
from zalfmas_fbp.components.dakis.read_file_from_disk import (
    ReadFileFromDisk,
)
from zalfmas_fbp.components.dakis.read_file_from_object_store import (
    METADATA as read_file_from_object_store_metadata,
)
from zalfmas_fbp.components.dakis.read_file_from_object_store import (
    ReadFileFromObjectStore,
)
from zalfmas_fbp.components.dakis.relabel_geoparquet import (
    METADATA as relabel_geoparquet_metadata,
)
from zalfmas_fbp.components.dakis.relabel_geoparquet import (
    RelabelGeoparquet,
)
from zalfmas_fbp.components.dakis.write_file_to_disk import (
    METADATA as write_file_to_disk_metadata,
)
from zalfmas_fbp.components.dakis.write_file_to_disk import (
    WriteFileToDisk,
)
from zalfmas_fbp.components.dakis.write_file_to_object_store import (
    METADATA as write_file_to_object_store_metadata,
)
from zalfmas_fbp.components.dakis.write_file_to_object_store import (
    WriteFileToObjectStore,
)
from zalfmas_fbp.components.dakis.write_geoparquet import (
    METADATA as write_geoparquet_metadata,
)
from zalfmas_fbp.components.dakis.write_geoparquet import (
    WriteGeoparquet,
)
from zalfmas_fbp.components.dakis.write_raster import (
    METADATA as write_raster_metadata,
)
from zalfmas_fbp.components.dakis.write_raster import (
    WriteRaster,
)
from zalfmas_fbp.run import process


def test_create_empty_raster_uses_default_config_and_writes_memory_file_bytes() -> None:
    component = CreateEmptyRaster(create_empty_raster_metadata)

    writer = run_process_component(
        component,
        inputs={
            "in": [
                ip_message("[12.0, 52.0, 12.01, 52.01]"),
                done_message(),
            ],
        },
    ).output()

    assert component.config.epsg == 25833
    assert component.config.resolution_m == 100.0
    assert component.config.compression == "zstd"
    assert writer.values[0].type == "openBracket"
    assert writer.values[-1].type == "closeBracket"

    raster_bytes = bytes(_prepared_payload_from_messages(writer.values).content.as_struct(common_capnp.Value).d)
    with MemoryFile(raster_bytes) as memory_file, memory_file.open() as dataset:
        assert dataset.crs.to_epsg() == 25833
        assert dataset.res == (100.0, 100.0)
        assert dataset.count == 1
        assert dataset.compression.value == "ZSTD"


def test_create_empty_raster_reads_conf_port_before_processing_input() -> None:
    component = CreateEmptyRaster(create_empty_raster_metadata)

    writer = run_process_component(
        component,
        inputs={
            "conf": [
                ip_message(
                    common_capnp.StructuredText.new_message(
                        type="toml",
                        value='epsg = 4326\nresolution_m = 50.0\ncompression = "lzw"',
                    ),
                ),
                done_message(),
            ],
            "in": [
                ip_message("[12.0, 52.0, 12.01, 52.01]"),
                done_message(),
            ],
        },
    ).output()

    assert component.config.epsg == 4326
    assert component.config.resolution_m == 50.0
    assert component.config.compression == "lzw"

    raster_bytes = bytes(_prepared_payload_from_messages(writer.values).content.as_struct(common_capnp.Value).d)
    with MemoryFile(raster_bytes) as memory_file, memory_file.open() as dataset:
        assert dataset.crs.to_epsg() == 4326
        assert dataset.res == (50.0, 50.0)
        assert dataset.compression.value == "LZW"


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
    component = FilterGeoparquetByRaster(filter_geoparquet_metadata)
    component.apply_config_values({"geoparquet_path": str(source_path)})

    result = run_process_component(
        component,
        inputs={"in": [_raster_message(raster_bytes), done_message()]},
        outputs=("out", "raster"),
    )

    assert filter_geoparquet_metadata.defaultConfig["geoparquet_path"].value == "resources/invekos_optimized.parquet"
    assert (
        bytes(_prepared_payload_from_messages(result.output("raster").values).content.as_struct(common_capnp.Value).d)
        == raster_bytes
    )

    geoparquet_bytes = bytes(
        _prepared_payload_from_messages(result.output("out").values).content.as_struct(common_capnp.Value).d
    )
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
    component = FilterGeoparquetByRaster(filter_geoparquet_metadata)
    component.apply_config_values({"geoparquet_path": str(source_path)})

    result = run_process_component(
        component,
        inputs={"in": [_raster_message(raster_bytes), done_message()]},
    )

    geoparquet_bytes = bytes(
        _prepared_payload_from_messages(result.output().values).content.as_struct(common_capnp.Value).d
    )
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

    component = RelabelGeoparquet(relabel_geoparquet_metadata)
    component.apply_config_values({"mapping_csv_path": str(mapping_path), "default_priority": 7})

    result = run_process_component(
        component,
        inputs={"in": [_raster_message(source_bytes), done_message()]},
    )

    output_bytes = bytes(
        _prepared_payload_from_messages(result.output().values).content.as_struct(common_capnp.Value).d
    )
    relabeled = gpd.read_parquet(cast("Any", BytesIO(output_bytes)))

    assert (
        relabel_geoparquet_metadata.defaultConfig["mapping_csv_path"].value == "resources/mappings/invekos_to_lulc.csv"
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
    component = RelabelGeoparquet(relabel_geoparquet_metadata)
    component.apply_config_values({"mapping_csv_path": str(mapping_path)})

    result = run_process_component(
        component,
        inputs={"in": [_raster_message(_geoparquet_bytes(source)), done_message()]},
    )

    output_bytes = bytes(
        _prepared_payload_from_messages(result.output().values).content.as_struct(common_capnp.Value).d
    )
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
    component = RelabelGeoparquet(relabel_geoparquet_metadata)

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

    output_bytes = bytes(
        _prepared_payload_from_messages(result.output().values).content.as_struct(common_capnp.Value).d
    )
    relabeled = gpd.read_parquet(cast("Any", BytesIO(output_bytes)))
    assert relabeled["lucode"].to_list() == [11, 12]
    assert relabeled["priority"].to_list() == [3, 4]


def test_burn_geoparquet_on_raster_burns_lucode_using_priority_order() -> None:
    geometries = gpd.GeoDataFrame(
        {
            "lucode": [10, 20],
            "priority": [1, 9],
            "geometry": [box(0, 0, 4, 4), box(1, 1, 3, 3)],
        },
        crs="EPSG:25833",
    )
    component = BurnGeoparquetOnRaster(burn_geoparquet_metadata)

    result = run_process_component(
        component,
        inputs={
            "raster": [_raster_message(_test_raster_bytes(width=4, height=4)), done_message()],
            "geometries": [_raster_message(_geoparquet_bytes(geometries)), done_message()],
        },
    )

    raster_bytes = bytes(
        _prepared_payload_from_messages(result.output().values).content.as_struct(common_capnp.Value).d
    )
    with MemoryFile(raster_bytes) as memory_file, memory_file.open() as dataset:
        values = dataset.read(1)

    assert values.tolist() == [
        [10, 10, 10, 10],
        [10, 20, 20, 10],
        [10, 20, 20, 10],
        [10, 10, 10, 10],
    ]


def test_burn_geoparquet_on_raster_promotes_dtype_for_lulc_codes_above_uint8() -> None:
    geometries = gpd.GeoDataFrame(
        {
            "lucode": [523],
            "priority": [1],
            "geometry": [box(0, 0, 1, 1)],
        },
        crs="EPSG:25833",
    )
    component = BurnGeoparquetOnRaster(burn_geoparquet_metadata)

    result = run_process_component(
        component,
        inputs={
            "raster": [_raster_message(_test_raster_bytes(width=1, height=1)), done_message()],
            "geometries": [_raster_message(_geoparquet_bytes(geometries)), done_message()],
        },
    )

    raster_bytes = bytes(
        _prepared_payload_from_messages(result.output().values).content.as_struct(common_capnp.Value).d
    )
    with MemoryFile(raster_bytes) as memory_file, memory_file.open() as dataset:
        assert dataset.dtypes == ("uint16",)
        assert dataset.read(1).tolist() == [[523]]


def test_burn_geoparquet_on_raster_stops_without_output_when_geometries_is_missing() -> None:
    class CountingReader(InMemoryReader):
        def __init__(self, messages):
            super().__init__(messages)
            self.reads = 0

        async def read(self):
            self.reads += 1
            return await super().read()

    raster_reader = CountingReader([_raster_message(_test_raster_bytes(width=4, height=4)), done_message()])
    component = BurnGeoparquetOnRaster(burn_geoparquet_metadata)
    writer = InMemoryWriter()
    component.in_ports["raster"] = cast("Any", raster_reader)
    component.out_ports["out"] = cast("Any", writer)

    asyncio.run(_run_process(component))

    assert raster_reader.reads == 1
    assert writer.values == []


def test_merge_geoparquet_uses_zip_array_input_and_preserves_relabel_schema() -> None:
    first = _geoparquet_bytes(
        gpd.GeoDataFrame({"lucode": [11], "priority": [3], "geometry": [box(0, 0, 1, 1)]}, crs="EPSG:25833"),
    )
    second = _geoparquet_bytes(
        gpd.GeoDataFrame({"lucode": [12], "priority": [4], "geometry": [box(2, 2, 3, 3)]}, crs="EPSG:25833"),
    )
    component = MergeGeoparquet(merge_geoparquet_metadata)
    writer = InMemoryWriter()
    component.array_in_ports["in"] = [
        cast("Any", InMemoryReader([_raster_message(first), done_message()])),
        cast("Any", InMemoryReader([_raster_message(second), done_message()])),
    ]
    component.out_ports["out"] = cast("Any", writer)

    asyncio.run(_run_process(component))

    output_bytes = bytes(_prepared_payload_from_messages(writer.values).content.as_struct(common_capnp.Value).d)
    merged = gpd.read_parquet(cast("Any", BytesIO(output_bytes)))
    assert merged.columns.to_list() == ["lucode", "priority", "geometry"]
    assert merged["lucode"].to_list() == [11, 12]
    assert merged["priority"].to_list() == [3, 4]
    assert merged.crs is not None
    assert merged.crs.to_epsg() == 25833
    assert _geo_metadata_from_bytes(output_bytes)["columns"]["geometry"]["crs"] is not None


def test_write_geoparquet_prepares_readable_file_payload(monkeypatch) -> None:
    monkeypatch.setattr(process, "DEFAULT_BRACKETED_CHUNK_SIZE", 5)
    geoparquet_bytes = _geoparquet_bytes(
        gpd.GeoDataFrame({"lucode": [11], "priority": [7], "geometry": [box(0, 0, 1, 1)]}, crs="EPSG:25833"),
    )
    component = WriteGeoparquet(write_geoparquet_metadata)
    component.apply_config_values({"path": "prepared/geoparquet", "filename": "out.parquet"})

    result = run_process_component(
        component,
        inputs={"in": [_raster_message(geoparquet_bytes), done_message()]},
    )

    assert result.output().values[0].type == "openBracket"
    assert result.output().values[-1].type == "closeBracket"
    assert len(result.output().values) > 3
    out_ip = _prepared_payload_from_messages(result.output().values)
    assert common.get_fbp_attr(out_ip, "path").as_text() == "prepared/geoparquet"
    assert common.get_fbp_attr(out_ip, "filename").as_text() == "out.parquet"
    assert common.get_fbp_attr(out_ip, "content_type").as_text() == "application/geoparquet"
    output_bytes = bytes(out_ip.content.as_struct(common_capnp.Value).d)
    prepared = gpd.read_parquet(cast("Any", BytesIO(output_bytes)))
    assert prepared["lucode"].to_list() == [11]
    assert prepared["priority"].to_list() == [7]
    assert prepared.crs is not None
    assert prepared.crs.to_epsg() == 25833
    assert _geo_metadata_from_bytes(output_bytes)["columns"]["geometry"]["crs"] is not None


def test_write_geoparquet_none_compression_writes_uncompressed_readable_payload(monkeypatch) -> None:
    monkeypatch.setattr(process, "DEFAULT_BRACKETED_CHUNK_SIZE", 5)
    geoparquet_bytes = _geoparquet_bytes(
        gpd.GeoDataFrame({"lucode": [11], "priority": [7], "geometry": [box(0, 0, 1, 1)]}, crs="EPSG:25833"),
    )
    component = WriteGeoparquet(write_geoparquet_metadata)
    component.apply_config_values({"compression": "none"})

    result = run_process_component(
        component,
        inputs={"in": [_raster_message(geoparquet_bytes), done_message()]},
    )

    output_bytes = bytes(
        _prepared_payload_from_messages(result.output().values).content.as_struct(common_capnp.Value).d
    )
    prepared = gpd.read_parquet(cast("Any", BytesIO(output_bytes)))
    assert prepared["lucode"].to_list() == [11]
    parquet_file = pq.ParquetFile(BytesIO(output_bytes))
    assert parquet_file.metadata.row_group(0).column(0).compression == "UNCOMPRESSED"


def test_write_geoparquet_preserve_compression_keeps_original_payload_bytes(monkeypatch) -> None:
    monkeypatch.setattr(process, "DEFAULT_BRACKETED_CHUNK_SIZE", 5)
    geoparquet_bytes = _geoparquet_bytes(
        gpd.GeoDataFrame({"lucode": [11], "priority": [7], "geometry": [box(0, 0, 1, 1)]}, crs="EPSG:25833"),
    )
    component = WriteGeoparquet(write_geoparquet_metadata)
    component.apply_config_values({"compression": "preserve"})

    result = run_process_component(
        component,
        inputs={"in": [_raster_message(geoparquet_bytes), done_message()]},
    )

    assert (
        bytes(_prepared_payload_from_messages(result.output().values).content.as_struct(common_capnp.Value).d)
        == geoparquet_bytes
    )


def test_write_raster_prepares_readable_geotiff_file_payload(monkeypatch) -> None:
    monkeypatch.setattr(process, "DEFAULT_BRACKETED_CHUNK_SIZE", 5)
    raster_bytes = _test_raster_bytes(width=3, height=2)
    component = WriteRaster(write_raster_metadata)
    component.apply_config_values({"path": "prepared/rasters", "filename": "raster.tif", "compression": "lzw"})

    result = run_process_component(
        component,
        inputs={"in": [_raster_message(raster_bytes), done_message()]},
    )

    assert result.output().values[0].type == "openBracket"
    assert result.output().values[-1].type == "closeBracket"
    assert len(result.output().values) > 3
    out_ip = _prepared_payload_from_messages(result.output().values)
    assert common.get_fbp_attr(out_ip, "path").as_text() == "prepared/rasters"
    assert common.get_fbp_attr(out_ip, "filename").as_text() == "raster.tif"
    assert common.get_fbp_attr(out_ip, "content_type").as_text() == "image/tiff"
    with (
        MemoryFile(bytes(out_ip.content.as_struct(common_capnp.Value).d)) as memory_file,
        memory_file.open() as dataset,
    ):
        assert dataset.crs.to_epsg() == 25833
        assert dataset.width == 3
        assert dataset.height == 2
        assert dataset.count == 1
        assert dataset.compression.value == "LZW"


def test_write_raster_none_compression_writes_uncompressed_geotiff(monkeypatch) -> None:
    monkeypatch.setattr(process, "DEFAULT_BRACKETED_CHUNK_SIZE", 5)
    raster_bytes = _test_raster_bytes(width=3, height=2)
    component = WriteRaster(write_raster_metadata)
    component.apply_config_values({"compression": "none"})

    result = run_process_component(
        component,
        inputs={"in": [_raster_message(raster_bytes), done_message()]},
    )

    output_bytes = bytes(
        _prepared_payload_from_messages(result.output().values).content.as_struct(common_capnp.Value).d
    )
    with MemoryFile(output_bytes) as memory_file, memory_file.open() as dataset:
        assert dataset.compression is None


def test_write_raster_preserve_compression_keeps_original_payload_bytes(monkeypatch) -> None:
    monkeypatch.setattr(process, "DEFAULT_BRACKETED_CHUNK_SIZE", 5)
    raster_bytes = _test_raster_bytes(width=3, height=2)
    component = WriteRaster(write_raster_metadata)
    component.apply_config_values({"compression": "preserve"})

    result = run_process_component(
        component,
        inputs={"in": [_raster_message(raster_bytes), done_message()]},
    )

    assert (
        bytes(_prepared_payload_from_messages(result.output().values).content.as_struct(common_capnp.Value).d)
        == raster_bytes
    )


def test_write_raster_write_as_cog_outputs_cloud_optimized_geotiff(monkeypatch) -> None:
    monkeypatch.setattr(process, "DEFAULT_BRACKETED_CHUNK_SIZE", 5)
    raster_bytes = _test_raster_bytes(width=256, height=256)
    component = WriteRaster(write_raster_metadata)
    component.apply_config_values({"write_as_cog": True, "compression": "preserve"})

    result = run_process_component(
        component,
        inputs={"in": [_raster_message(raster_bytes), done_message()]},
    )

    output_bytes = bytes(
        _prepared_payload_from_messages(result.output().values).content.as_struct(common_capnp.Value).d
    )
    with MemoryFile(output_bytes) as memory_file, memory_file.open() as dataset:
        image_structure = dataset.tags(ns="IMAGE_STRUCTURE")
        assert image_structure["LAYOUT"] == "COG"
        assert image_structure["COMPRESSION"] == "ZSTD"
        assert dataset.compression.value == "ZSTD"


def test_write_raster_write_as_cog_can_write_uncompressed_output(monkeypatch) -> None:
    monkeypatch.setattr(process, "DEFAULT_BRACKETED_CHUNK_SIZE", 5)
    raster_bytes = _test_raster_bytes(width=256, height=256)
    component = WriteRaster(write_raster_metadata)
    component.apply_config_values({"write_as_cog": True, "compression": "none"})

    result = run_process_component(
        component,
        inputs={"in": [_raster_message(raster_bytes), done_message()]},
    )

    output_bytes = bytes(
        _prepared_payload_from_messages(result.output().values).content.as_struct(common_capnp.Value).d
    )
    with MemoryFile(output_bytes) as memory_file, memory_file.open() as dataset:
        image_structure = dataset.tags(ns="IMAGE_STRUCTURE")
        assert image_structure["LAYOUT"] == "COG"
        assert dataset.compression is None


def test_write_file_to_disk_uses_prepared_file_path_and_filename(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(process, "DEFAULT_BRACKETED_CHUNK_SIZE", 2)
    component = WriteFileToDisk(write_file_to_disk_metadata)
    file_ips = list(
        prepared_file_chunk_ips(
            b"payload",
            path=str(tmp_path / "nested"),
            filename="prepared.bin",
            content_type="application/octet-stream",
        )
    )

    run_process_component(
        component,
        inputs={"in": [_ip_reader_message(ip) for ip in file_ips] + [done_message()]},
        outputs=(),
    )

    assert (tmp_path / "nested" / "prepared.bin").read_bytes() == b"payload"


def test_write_file_to_disk_rejects_unterminated_bracketed_payload(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(process, "DEFAULT_BRACKETED_CHUNK_SIZE", 2)
    component = WriteFileToDisk(write_file_to_disk_metadata)
    path = str(tmp_path / "nested")
    open_ip = prepared_file_bracket_ip(
        bracket_type="openBracket",
        path=path,
        filename="prepared.bin",
        content_type="application/octet-stream",
    )
    chunk_ip = prepared_file_ip(
        b"pa",
        path=path,
        filename="prepared.bin",
        content_type="application/octet-stream",
    )

    try:
        run_process_component(
            component,
            inputs={"in": [_ip_reader_message(open_ip), _ip_reader_message(chunk_ip), done_message()]},
            outputs=(),
        )
    except ValueError as exc:
        assert "closed before the prepared file payload ended" in str(exc)
    else:
        raise AssertionError("write file to disk should reject unterminated bracketed payloads")


def test_write_file_to_disk_finishes_before_a_late_input_connection(tmp_path: Path) -> None:
    async def run_test() -> None:
        component = WriteFileToDisk(write_file_to_disk_metadata)
        output_path = tmp_path / "nested"
        file_ips = list(
            prepared_file_chunk_ips(
                b"payload",
                path=str(output_path),
                filename="prepared.bin",
                content_type="application/octet-stream",
            )
        )

        assert await component.start(cast("Any", None)) is True
        if component._run_task is None:
            raise AssertionError("Process component did not create a run task.")

        await asyncio.wait_for(component._run_task, timeout=1)
        assert component.process_state == "idle"

        component.in_ports["in"] = cast(
            "Any",
            InMemoryReader([_ip_reader_message(ip) for ip in file_ips] + [done_message()]),
        )
        assert (output_path / "prepared.bin").exists() is False

    asyncio.run(run_test())


def test_write_file_to_object_store_uploads_to_configured_s3_endpoint(monkeypatch) -> None:
    monkeypatch.setattr(process, "DEFAULT_BRACKETED_CHUNK_SIZE", 2)
    captured: dict[str, Any] = {}

    class FakeClient:
        def put_object(self, **kwargs):
            if hasattr(kwargs["Body"], "read"):
                kwargs = {**kwargs, "Body": kwargs["Body"].read()}
            captured["put_object"] = kwargs

    class FakeSession:
        def __init__(self, **kwargs):
            captured["session"] = kwargs

        def client(self, service_name: str, *, endpoint_url: str):
            captured["client"] = {"service_name": service_name, "endpoint_url": endpoint_url}
            return FakeClient()

    from zalfmas_fbp.components.dakis import write_file_to_object_store

    monkeypatch.setattr(write_file_to_object_store, "Session", FakeSession)

    component = WriteFileToObjectStore(write_file_to_object_store_metadata)
    component.apply_config_values(
        {
            "object_store_url": "https://objects.dakispro.de",
            "access_key": "access",
            "secret_key": "secret",
            "bucket": "bucket",
        },
    )
    file_ips = list(
        prepared_file_chunk_ips(
            b"payload",
            path="prepared/geoparquet",
            filename="out.parquet",
            content_type="application/geoparquet",
        )
    )

    run_process_component(
        component,
        inputs={"in": [_ip_reader_message(ip) for ip in file_ips] + [done_message()]},
        outputs=(),
    )

    assert captured["session"] == {"aws_access_key_id": "access", "aws_secret_access_key": "secret"}
    assert captured["client"] == {"service_name": "s3", "endpoint_url": "https://objects.dakispro.de"}
    assert captured["put_object"] == {
        "Bucket": "bucket",
        "Key": "prepared/geoparquet/out.parquet",
        "Body": b"payload",
        "ContentType": "application/geoparquet",
    }


def test_read_file_from_disk_outputs_prepared_file_payload(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(process, "DEFAULT_BRACKETED_CHUNK_SIZE", 2)
    path = tmp_path / "nested"
    path.mkdir()
    (path / "input.bin").write_bytes(b"payload")
    component = ReadFileFromDisk(read_file_from_disk_metadata)
    component.apply_config_values(
        {
            "path": str(path),
            "filename": "input.bin",
            "content_type": "application/octet-stream",
        },
    )

    result = run_process_component(component, inputs={}, outputs=("out",))

    assert [ip.type for ip in result.output().values] == [
        "openBracket",
        "standard",
        "standard",
        "standard",
        "standard",
        "closeBracket",
    ]
    out_ip = _prepared_payload_from_messages(result.output().values)
    assert bytes(out_ip.content.as_struct(common_capnp.Value).d) == b"payload"
    assert common.get_fbp_attr(out_ip, "path").as_text() == str(path)
    assert common.get_fbp_attr(out_ip, "filename").as_text() == "input.bin"
    assert common.get_fbp_attr(out_ip, "content_type").as_text() == "application/octet-stream"


def test_read_file_from_disk_missing_input_emits_no_partial_payload(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(process, "DEFAULT_BRACKETED_CHUNK_SIZE", 2)
    component = ReadFileFromDisk(read_file_from_disk_metadata)
    component.apply_config_values(
        {
            "path": str(tmp_path),
            "filename": "missing.bin",
            "content_type": "application/octet-stream",
        },
    )

    result = run_process_component(component, inputs={}, outputs=("out",))

    assert result.output().values == []


def test_read_file_from_object_store_downloads_from_configured_s3_endpoint(monkeypatch) -> None:
    monkeypatch.setattr(process, "DEFAULT_BRACKETED_CHUNK_SIZE", 2)
    captured: dict[str, Any] = {}

    class FakeBody:
        def __init__(self):
            self._chunks = [b"pa", b"yl", b"oa", b"d", b""]

        def read(self, _size: int = -1) -> bytes:
            return self._chunks.pop(0)

    class FakeClient:
        def get_object(self, **kwargs):
            captured["get_object"] = kwargs
            return {"Body": FakeBody()}

    class FakeSession:
        def __init__(self, **kwargs):
            captured["session"] = kwargs

        def client(self, service_name: str, *, endpoint_url: str):
            captured["client"] = {"service_name": service_name, "endpoint_url": endpoint_url}
            return FakeClient()

    from zalfmas_fbp.components.dakis import read_file_from_object_store

    monkeypatch.setattr(read_file_from_object_store, "Session", FakeSession)

    component = ReadFileFromObjectStore(read_file_from_object_store_metadata)
    component.apply_config_values(
        {
            "object_store_url": "https://objects.dakispro.de",
            "access_key": "access",
            "secret_key": "secret",
            "bucket": "bucket",
            "path": "prepared/geoparquet",
            "filename": "out.parquet",
            "content_type": "application/geoparquet",
        },
    )

    result = run_process_component(component, inputs={}, outputs=("out",))

    assert captured["session"] == {"aws_access_key_id": "access", "aws_secret_access_key": "secret"}
    assert captured["client"] == {"service_name": "s3", "endpoint_url": "https://objects.dakispro.de"}
    assert captured["get_object"] == {"Bucket": "bucket", "Key": "prepared/geoparquet/out.parquet"}
    assert [ip.type for ip in result.output().values] == [
        "openBracket",
        "standard",
        "standard",
        "standard",
        "standard",
        "closeBracket",
    ]
    out_ip = _prepared_payload_from_messages(result.output().values)
    assert bytes(out_ip.content.as_struct(common_capnp.Value).d) == b"payload"
    assert common.get_fbp_attr(out_ip, "path").as_text() == "prepared/geoparquet"
    assert common.get_fbp_attr(out_ip, "filename").as_text() == "out.parquet"
    assert common.get_fbp_attr(out_ip, "content_type").as_text() == "application/geoparquet"


async def _run_process(component) -> None:
    await component.start(cast("Any", None))
    if component._run_task is None:
        raise AssertionError("Process component did not create a run task.")
    await component._run_task
    if component._run_exception is not None:
        raise component._run_exception


def _test_raster_bytes(*, width: int = 10, height: int = 10) -> bytes:
    with MemoryFile() as memory_file:
        with memory_file.open(
            driver="GTiff",
            width=width,
            height=height,
            count=1,
            dtype="uint8",
            crs=CRS.from_epsg(25833),
            transform=from_origin(0, height, 1, 1),
            nodata=0,
            compress="zstd",
        ):
            pass
        return bytes(memory_file.read())


def _raster_message(raster_bytes: bytes):
    return ip_message(common_capnp.Value.new_message(d=raster_bytes))


def _ip_reader_message(ip):
    return PortMessage(PortValue(ip))


def _prepared_payload_from_messages(messages):
    assert messages[0].type == "openBracket"
    assert messages[-1].type == "closeBracket"
    data = b"".join(bytes(ip.content.as_struct(common_capnp.Value).d) for ip in messages[1:-1])
    return prepared_file_ip(
        data,
        path=_attr_text(messages[0], "path"),
        filename=_attr_text(messages[0], "filename"),
        content_type=_attr_text(messages[0], "content_type") or "application/octet-stream",
    )


def _attr_text(ip, key: str) -> str:
    attr = common.get_fbp_attr(ip, key)
    return attr.as_text() if attr is not None else ""


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
