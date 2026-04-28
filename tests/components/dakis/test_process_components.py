from __future__ import annotations

from mas.schema.common import common_capnp
from rasterio.io import MemoryFile

from tests.component_harness import done_message, ip_message, run_process_component
from zalfmas_fbp.components.dakis.dakis_process.create_empty_raster import CreateEmptyRaster, meta


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
