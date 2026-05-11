#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from typing import Literal, override

from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.burn import burn_geoparquet_on_raster_bytes
from zalfmas_fbp.components.dakis.common.file_payload import (
    GEOPARQUET_CONTENT_TYPE,
    GEOTIFF_CONTENT_TYPE,
    blob_content_type,
)
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()

type BurnRasterCompression = Literal["preserve", "zstd", "deflate", "lzw", "none"]
BURN_RASTER_COMPRESSION_OPTIONS = ["preserve", "zstd", "deflate", "lzw", "none"]

METADATA = meta.Component(
    category=meta.Category(
        id="dakis",
        name="DAKIS",
    ),
    info=meta.Info(
        id="18df1635-f2c8-4d86-8b40-622035290c8f",
        name="burn geoparquet on raster",
        description="Burn relabeled GeoParquet geometries into a raster using priority order.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="raster",
            contentType=blob_content_type(GEOTIFF_CONTENT_TYPE),
            desc="Compressed raster bytes.",
        ),
        meta.Port(
            name="geometries",
            contentType=blob_content_type(GEOPARQUET_CONTENT_TYPE),
            desc="GeoParquet bytes with lucode, priority, and geometry columns.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType=blob_content_type(GEOTIFF_CONTENT_TYPE),
            desc="Burned raster bytes.",
        ),
    ],
    defaultConfig={
        "code_column": meta.ConfigEntry(
            value="lucode",
            type="string",
            desc="Column containing the land-use/land-cover code to burn into the raster.",
        ),
        "priority_column": meta.ConfigEntry(
            value="priority",
            type="string",
            desc="Column used for burn order. Higher values overwrite lower values.",
        ),
        "all_touched": meta.ConfigEntry(
            value=False,
            type="bool",
            desc="Burn every pixel touched by a geometry instead of only pixels whose center is within it.",
        ),
        "compression": meta.ConfigEntry(
            value="preserve",
            type=BURN_RASTER_COMPRESSION_OPTIONS,
            desc="Output raster compression algorithm. Use 'preserve' to keep the incoming raster compression.",
        ),
    },
)


class BurnGeoparquetOnRasterConfig(process.ProcessConfig):
    code_column: str = "lucode"
    priority_column: str = "priority"
    all_touched: bool = False
    compression: BurnRasterCompression = "preserve"


class BurnGeoparquetOnRaster(process.Process[BurnGeoparquetOnRasterConfig]):
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
            raster_msg = await self.read_in_chunked("raster")
            if raster_msg is None:
                break
            geometries_msg = await self.read_in_chunked("geometries")
            if geometries_msg is None:
                break

            try:
                raster_bytes, _raster_content_type = process.read_ip_data(raster_msg)
                geoparquet_bytes, _geoparquet_content_type = process.read_ip_data(geometries_msg)
                output_bytes = burn_geoparquet_on_raster_bytes(
                    raster_bytes,
                    geoparquet_bytes,
                    code_column=self.config.code_column,
                    priority_column=self.config.priority_column,
                    all_touched=self.config.all_touched,
                    compression=None if self.config.compression == "preserve" else self.config.compression,
                )

                if not await self.write_out_chunked(
                    "out", process.blob_ip(output_bytes, content_type=GEOTIFF_CONTENT_TYPE)
                ):
                    logger.info("%s process finished", self.name)
                    return
                logger.info("%s sent %s burned raster bytes", self.name, len(output_bytes))

            except (OSError, TypeError, ValueError):
                logger.exception("%s failed to burn GeoParquet on raster", self.name)

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(BurnGeoparquetOnRaster(METADATA), METADATA)


if __name__ == "__main__":
    main()
