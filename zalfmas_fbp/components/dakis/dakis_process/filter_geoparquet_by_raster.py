#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, override

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.geoparquet import overlapping_geometries_as_geoparquet
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import IPBuilder

logger = logging.getLogger(__name__)
configure_logging()

meta = {
    "category": {"id": "dakis", "name": "DAKIS"},
    "component": {
        "info": {
            "id": "22023991-782c-4c2c-ae72-8e8a19e9f24a",
            "name": "filter geoparquet by raster",
            "description": "Select GeoParquet geometries overlapping a raster and forward both datasets.",
        },
        "type": "process",
        "inPorts": [{"name": "in", "contentType": "common.capnp:Value[Data]", "desc": "Compressed raster bytes."}],
        "outPorts": [
            {"name": "out", "contentType": "common.capnp:Value[Data]", "desc": "Filtered GeoParquet bytes."},
            {"name": "raster", "contentType": "common.capnp:Value[Data]", "desc": "Original raster bytes."},
        ],
        "defaultConfig": {
            "geoparquet_path": {
                "value": "resources/invekos_optimized.parquet",
                "type": "string",
                "desc": "Path to the source GeoParquet file.",
            },
        },
    },
}


class FilterGeoparquetByRaster(process.Process):
    def __init__(self, metadata: dict[str, Any] | None, con_man: common.ConnectionManager | None = None):
        process.Process.__init__(self, metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        while True:
            in_msg = await self.read_in("in")
            if in_msg is None:
                break

            try:
                raster_bytes = bytes(in_msg.content.as_struct(common_capnp.Value).d)
                geoparquet_bytes = overlapping_geometries_as_geoparquet(
                    raster_bytes,
                    self.config["geoparquet_path"].t,
                )

                if not await self.write_out("out", _data_ip(geoparquet_bytes)):
                    logger.info("%s process finished", self.name)
                    return
                await self.write_out("raster", _data_ip(raster_bytes))
                logger.info("%s sent %s GeoParquet bytes", self.name, len(geoparquet_bytes))

            except (OSError, TypeError, ValueError):
                logger.exception("%s failed to filter GeoParquet", self.name)

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(FilterGeoparquetByRaster(meta), meta)


def _data_ip(data: bytes) -> IPBuilder:
    return fbp_capnp.IP.new_message(content=common_capnp.Value.new_message(d=data))


if __name__ == "__main__":
    main()
