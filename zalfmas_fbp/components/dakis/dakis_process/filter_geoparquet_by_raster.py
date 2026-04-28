#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from typing import Any, override

import capnp
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from zalfmas_common import common

import zalfmas_fbp.run.process as process
from zalfmas_fbp.components.dakis.common.geoparquet import overlapping_geometries_as_geoparquet
from zalfmas_fbp.run.logging_config import configure_logging

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
            }
        },
    },
}


class FilterGeoparquetByRaster(process.Process):
    def __init__(self, metadata: dict[str, Any] | None, con_man: common.ConnectionManager | None = None):
        process.Process.__init__(self, metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        await self.process_started()
        logger.info("%s process started", self.name)

        if self.out_ports["out"] is None:
            logger.warning("%s out port not connected; cannot forward filtered GeoParquet", self.name)
        if self.out_ports["raster"] is None:
            logger.info("%s raster output not connected; skipping raster forwarding", self.name)

        while self.in_ports["in"] and self.out_ports["out"]:
            if self.is_canceled():
                break

            try:
                in_port = self.in_ports["in"]
                out_port = self.out_ports["out"]
                raster_port = self.out_ports["raster"]
                if not in_port or not out_port:
                    break

                in_msg = await in_port.read()
                if in_msg.which() == "done":
                    self.in_ports["in"] = None
                    continue

                raster_bytes = bytes(in_msg.value.as_struct(fbp_capnp.IP).content.as_struct(common_capnp.Value).d)
                geoparquet_bytes = overlapping_geometries_as_geoparquet(
                    raster_bytes,
                    self.config["geoparquet_path"].t,
                )

                await out_port.write(value=_data_ip(geoparquet_bytes))
                if raster_port:
                    await raster_port.write(value=_data_ip(raster_bytes))
                logger.info("%s sent %s GeoParquet bytes", self.name, len(geoparquet_bytes))

            except capnp.KjException as e:
                logger.error("%s RPC Exception: %s", self.name, e.description)
                if e.type in ["DISCONNECTED"]:
                    break
            except (OSError, TypeError, ValueError):
                logger.exception("%s failed to filter GeoParquet", self.name)

        logger.info("%s process finished", self.name)
        await self.process_stopped()


def main():
    process.run_process_from_metadata_and_cmd_args(FilterGeoparquetByRaster(meta), meta)


def _data_ip(data: bytes) -> Any:
    return fbp_capnp.IP.new_message(content=common_capnp.Value.new_message(d=data))


if __name__ == "__main__":
    main()
