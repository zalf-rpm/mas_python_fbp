#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from typing import Any, override

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.raster import create_empty_raster_bytes, parse_geojson_bbox
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()

meta = {
    "category": {"id": "dakis", "name": "DAKIS"},
    "component": {
        "info": {
            "id": "2a1a5561-fee4-4c21-9b0d-e7626db1585e",
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
        logger.info("%s process running", self.name)

        while True:
            in_msg = await self.read_in("in")
            if in_msg is None:
                break

            try:
                bbox_text = in_msg.content.as_text()
                bbox = parse_geojson_bbox(bbox_text)
                raster_bytes = create_empty_raster_bytes(
                    bbox,
                    epsg=self.config["epsg"].i64,
                    resolution_m=self.config["resolution_m"].f64,
                    compression=self.config["compression"].t,
                )

                out_ip = fbp_capnp.IP.new_message(content=common_capnp.Value.new_message(d=raster_bytes))
                if not await self.write_out("out", out_ip):
                    logger.info("%s process finished", self.name)
                    return
                logger.info("%s sent raster with %s bytes", self.name, len(raster_bytes))

            except (TypeError, ValueError):
                logger.exception("%s failed to create raster", self.name)

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(CreateEmptyRaster(meta), meta)


if __name__ == "__main__":
    main()
