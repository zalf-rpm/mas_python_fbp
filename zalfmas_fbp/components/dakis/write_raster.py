#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from typing import Literal, override

from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.file_payload import (
    COG_CONTENT_TYPE,
    GEOTIFF_CONTENT_TYPE,
    blob_content_type,
    prepared_file_ip,
)
from zalfmas_fbp.components.dakis.common.write_raster import prepare_raster_bytes
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()

type WriteRasterCompression = Literal["preserve", "zstd", "deflate", "lzw", "none"]
RASTER_COMPRESSION_OPTIONS = ["preserve", "zstd", "deflate", "lzw", "none"]

METADATA = meta.Component(
    category=meta.Category(
        id="dakis",
        name="DAKIS",
    ),
    info=meta.Info(
        id="f706a283-6d07-4aae-828f-6cf8e4bf62c2",
        name="write raster",
        description="Prepare received raster bytes as a GeoTIFF or Cloud Optimized GeoTIFF file payload.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType=blob_content_type(GEOTIFF_CONTENT_TYPE),
            desc="GeoTIFF raster bytes.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType=f"{blob_content_type(GEOTIFF_CONTENT_TYPE)} | {blob_content_type(COG_CONTENT_TYPE)}",
            desc="Prepared GeoTIFF or COG file payload.",
        ),
    ],
    defaultConfig={
        "path": meta.ConfigEntry(
            value="",
            type="string",
            desc="Optional target path or object key prefix for the prepared file. Empty uses the sink default.",
        ),
        "filename": meta.ConfigEntry(
            value="raster.tif",
            type="string",
            desc="Target filename for the prepared GeoTIFF.",
        ),
        "compression": meta.ConfigEntry(
            value="preserve",
            type=RASTER_COMPRESSION_OPTIONS,
            desc="GeoTIFF/COG compression algorithm. Use 'preserve' to keep the source compression.",
        ),
        "write_as_cog": meta.ConfigEntry(
            value=False,
            type="bool",
            desc="If true, rewrite the output as a Cloud Optimized GeoTIFF (COG).",
        ),
    },
)


class WriteRasterConfig(process.ProcessConfig):
    path: str = ""
    filename: str = "raster.tif"
    compression: WriteRasterCompression = "preserve"
    write_as_cog: bool = False


class WriteRaster(process.Process[WriteRasterConfig]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        while True:
            in_msg = await self.read_in_chunked("in")
            if in_msg is None:
                break

            try:
                raster_bytes, _content_type = process.read_ip_data(in_msg)
                output_bytes = prepare_raster_bytes(
                    raster_bytes,
                    compression=self.config.compression,
                    write_as_cog=self.config.write_as_cog,
                )
                content_type = COG_CONTENT_TYPE if self.config.write_as_cog else GEOTIFF_CONTENT_TYPE
                if not await self.write_out_chunked(
                    "out",
                    prepared_file_ip(
                        output_bytes,
                        path=self.config.path,
                        filename=self.config.filename,
                        content_type=content_type,
                    ),
                ):
                    logger.info("%s process finished", self.name)
                    return
                logger.info("%s prepared GeoTIFF file payload", self.name)

            except (OSError, TypeError, ValueError):
                logger.exception("%s failed to prepare raster file payload", self.name)

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(WriteRaster(METADATA), METADATA)


if __name__ == "__main__":
    main()
