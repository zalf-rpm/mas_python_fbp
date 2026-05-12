#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from typing import Literal, override

from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.file_payload import GEOTIFF_CONTENT_TYPE, blob_content_type
from zalfmas_fbp.components.dakis.common.raster import create_empty_raster_bytes, parse_geojson_bbox
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()

type GeoTiffCompression = Literal["zstd", "deflate", "lzw", "none"]


class CreateEmptyRasterConfig(process.ProcessConfig):
    epsg: int = Field(25833, description="EPSG code of the output raster CRS.")
    resolution_m: float = Field(100.0, description="Raster resolution in meters per pixel.")
    compression: GeoTiffCompression = Field("zstd", description="GeoTIFF compression algorithm.")


METADATA = meta.Component(
    category=meta.Category(
        id="dakis",
        name="DAKIS",
    ),
    info=meta.Info(
        id="2a1a5561-fee4-4c21-9b0d-e7626db1585e",
        name="create empty raster",
        description="Create an empty compressed in-memory raster from a GeoJSON bbox.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType="Text",
            desc="GeoJSON bbox as text.",
        ),
        meta.Port(
            name="conf",
            contentType="common.capnp:StructuredText[JSON | TOML]",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType=blob_content_type(GEOTIFF_CONTENT_TYPE),
            desc="Compressed GeoTIFF bytes.",
        ),
    ],
    config=CreateEmptyRasterConfig,
)


class CreateEmptyRaster(process.Process[CreateEmptyRasterConfig]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
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

                out_ip = process.blob_ip(raster_bytes, content_type=GEOTIFF_CONTENT_TYPE)
                if not await self.write_out_chunked("out", out_ip):
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
