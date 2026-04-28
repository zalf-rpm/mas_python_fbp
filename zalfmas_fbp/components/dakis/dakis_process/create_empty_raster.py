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
from zalfmas_fbp.components.dakis.common.raster import create_empty_raster_bytes, parse_geojson_bbox

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s @ %(name)s - %(levelname)-8s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

meta = {
    "category": {"id": "dakis", "name": "DAKIS"},
    "component": {
        "info": {
            "id": "b822f996-67da-4826-9e91-3712b795e648",
            "name": "create empty raster",
            "description": "Create an empty compressed in-memory raster from a GeoJSON bbox.",
        },
        "type": "process",
        "inPorts": [
            {"name": "in", "contentType": "Text", "desc": "GeoJSON bbox as text."},
            {"name": "conf", "contentType": "common.capnp:StructuredText[JSON | TOML]"},
        ],
        "outPorts": [{"name": "out", "contentType": "common.capnp:Value[Data]", "desc": "Compressed GeoTIFF bytes."}],
        "defaultConfig": {
            "epsg": {"value": 25833, "type": "int", "desc": "EPSG code of the output raster CRS."},
            "resolution_m": {"value": 100.0, "type": "float", "desc": "Raster resolution in meters per pixel."},
            "compression": {"value": "zstd", "type": "string", "desc": "GeoTIFF compression algorithm."},
        },
    },
}


class CreateEmptyRaster(process.Process):
    def __init__(self, metadata: dict[str, Any] | None, con_man: common.ConnectionManager | None = None):
        process.Process.__init__(self, metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        await self.process_started()
        logger.info("%s process started", self.name)

        while self.in_ports["in"] and self.out_ports["out"]:
            if self.is_canceled():
                break

            try:
                in_port = self.in_ports["in"]
                out_port = self.out_ports["out"]
                if not in_port or not out_port:
                    break

                in_msg = await in_port.read()
                if in_msg.which() == "done":
                    self.in_ports["in"] = None
                    continue

                bbox_text = in_msg.value.as_struct(fbp_capnp.IP).content.as_text()
                bbox = parse_geojson_bbox(bbox_text)
                raster_bytes = create_empty_raster_bytes(
                    bbox,
                    epsg=self.config["epsg"].i64,
                    resolution_m=self.config["resolution_m"].f64,
                    compression=self.config["compression"].t,
                )

                out_ip = fbp_capnp.IP.new_message(content=common_capnp.Value.new_message(d=raster_bytes))
                await out_port.write(value=out_ip)
                logger.info("%s sent raster with %s bytes", self.name, len(raster_bytes))

            except capnp.KjException as e:
                logger.error("%s RPC Exception: %s", self.name, e.description)
                if e.type in ["DISCONNECTED"]:
                    break
            except (TypeError, ValueError):
                logger.exception("%s failed to create raster", self.name)

        logger.info("%s process finished", self.name)
        await self.process_stopped()


def main():
    process.run_process_from_metadata_and_cmd_args(CreateEmptyRaster(meta), meta)


if __name__ == "__main__":
    main()
