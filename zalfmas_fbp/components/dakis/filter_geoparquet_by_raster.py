#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from typing import override

from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.file_payload import (
    GEOPARQUET_CONTENT_TYPE,
    GEOTIFF_CONTENT_TYPE,
    blob_content_type,
)
from zalfmas_fbp.components.dakis.common.geoparquet import overlapping_geometries_as_geoparquet
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()

METADATA = meta.Component(
    category=meta.Category(
        id="dakis",
        name="DAKIS",
    ),
    info=meta.Info(
        id="22023991-782c-4c2c-ae72-8e8a19e9f24a",
        name="filter geoparquet by raster",
        description="Select GeoParquet geometries overlapping a raster and forward both datasets.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType=blob_content_type(GEOTIFF_CONTENT_TYPE),
            desc="Compressed raster bytes.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType=blob_content_type(GEOPARQUET_CONTENT_TYPE),
            desc="Filtered GeoParquet bytes.",
        ),
        meta.Port(
            name="raster",
            contentType=blob_content_type(GEOTIFF_CONTENT_TYPE),
            desc="Original raster bytes.",
        ),
    ],
    defaultConfig={
        "geoparquet_path": meta.ConfigEntry(
            value="resources/invekos_optimized.parquet",
            type="string",
            desc="Path to the source GeoParquet file.",
        ),
    },
)


class FilterGeoparquetByRasterConfig(process.ProcessConfig):
    geoparquet_path: str = "resources/invekos_optimized.parquet"


class FilterGeoparquetByRaster(process.Process[FilterGeoparquetByRasterConfig]):
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
                geoparquet_bytes = overlapping_geometries_as_geoparquet(
                    raster_bytes,
                    self.config.geoparquet_path,
                )

                if not await self.write_out_chunked(
                    "out",
                    process.blob_ip(geoparquet_bytes, content_type=GEOPARQUET_CONTENT_TYPE),
                ):
                    logger.info("%s process finished", self.name)
                    return
                _ = await self.write_out_chunked(
                    "raster",
                    process.blob_ip(raster_bytes, content_type=GEOTIFF_CONTENT_TYPE),
                )
                logger.info("%s sent %s GeoParquet bytes", self.name, len(geoparquet_bytes))

            except (OSError, TypeError, ValueError):
                logger.exception("%s failed to filter GeoParquet", self.name)

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(FilterGeoparquetByRaster(METADATA), METADATA)


if __name__ == "__main__":
    main()
