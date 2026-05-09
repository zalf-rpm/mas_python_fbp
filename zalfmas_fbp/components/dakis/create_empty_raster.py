#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from typing import Literal, override

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.raster import create_empty_raster_bytes, parse_geojson_bbox
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging
from zalfmas_fbp.run.metadata import ComponentMetadata

logger = logging.getLogger(__name__)
configure_logging()

type GeoTiffCompression = Literal["zstd", "deflate", "lzw", "none"]
GEOTIFF_COMPRESSION_OPTIONS = ["zstd", "deflate", "lzw", "none"]

METADATA = ComponentMetadata.model_validate(
    {
        "category": {"id": "dakis", "name": "DAKIS"},
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
            "compression": {
                "value": "zstd",
                "type": GEOTIFF_COMPRESSION_OPTIONS,
                "desc": "GeoTIFF compression algorithm.",
            },
        },
    },
)


class CreateEmptyRasterConfig(process.ProcessConfig):
    epsg: int = 25833
    resolution_m: float = 100.0
    compression: GeoTiffCompression = "zstd"


class CreateEmptyRaster(process.Process[CreateEmptyRasterConfig]):
    def __init__(
        self,
        metadata: ComponentMetadata = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)
        if await self.update_config_from_port("conf"):
            logger.info("%s updated config from conf port", self.name)

        while True:
            in_msg = await self.read_in("in")
            if in_msg is None:
                break

            try:
                bbox_text = in_msg.content.as_text()
                bbox = parse_geojson_bbox(bbox_text)
                raster_bytes = create_empty_raster_bytes(
                    bbox,
                    epsg=self.config.epsg,
                    resolution_m=self.config.resolution_m,
                    compression=self.config.compression,
                )

                out_ip = fbp_capnp.IP.new_message(content=common_capnp.Value.new_message(d=raster_bytes))
                if not await self.write_out("out", out_ip, True):
                    logger.info("%s process finished", self.name)
                    return
                logger.info("%s sent raster with %s bytes", self.name, len(raster_bytes))

            except (TypeError, ValueError):
                logger.exception("%s failed to create raster", self.name)

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(CreateEmptyRaster(METADATA), METADATA)


if __name__ == "__main__":
    main()
