#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from typing import override

from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.file_payload import (
    GEOPARQUET_CONTENT_TYPE,
    GEOTIFF_CONTENT_TYPE,
    blob_content_type,
)
from zalfmas_fbp.components.dakis.common.rbs import RBSFetchConfig, RetryConfig, rbs_for_raster_as_geoparquet
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()


class FetchRBSByRasterConfig(process.ProcessConfig):
    grid_size: int = Field(4, description="Number of WFS request cells per raster axis.")
    max_cell_retries: int = Field(3, description="Maximum retry attempts per WFS grid cell.")
    retry_backoff_base_s: float = Field(1.0, description="Initial retry delay in seconds.")
    retry_backoff_factor: float = Field(2.0, description="Retry delay multiplier.")
    retry_max_delay_s: float = Field(30.0, description="Maximum retry delay in seconds.")
    retry_jitter_s: float = Field(0.25, description="Maximum random retry jitter in seconds.")


METADATA = meta.Component(
    category=meta.Category(
        id="dakis",
        name="DAKIS",
    ),
    info=meta.Info(
        id="0ea8bdef-e3b9-4c7f-9d0a-75cd5876277d",
        name="fetch RBS by raster",
        description="Fetch RBS soil valuation geometries overlapping a raster and emit GeoParquet bytes.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType=blob_content_type(GEOTIFF_CONTENT_TYPE),
            desc="Compressed raster bytes used as the bounding box.",
        ),
        meta.Port(
            name="conf",
            contentType="common.capnp:StructuredText[JSON | TOML]",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType=blob_content_type(GEOPARQUET_CONTENT_TYPE),
            desc="Fetched RBS GeoParquet bytes.",
        ),
    ],
    config=FetchRBSByRasterConfig,
)


class FetchRBSByRaster(process.Process[FetchRBSByRasterConfig]):
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
            in_msg = await self.read_in_chunked("in")
            if in_msg is None:
                break

            try:
                raster_bytes, _content_type = process.read_ip_data(in_msg)
                geoparquet_bytes = rbs_for_raster_as_geoparquet(
                    raster_bytes,
                    config=RBSFetchConfig(
                        grid_size=self.config.grid_size,
                        retry=RetryConfig(
                            max_cell_retries=self.config.max_cell_retries,
                            retry_backoff_base_s=self.config.retry_backoff_base_s,
                            retry_backoff_factor=self.config.retry_backoff_factor,
                            retry_max_delay_s=self.config.retry_max_delay_s,
                            retry_jitter_s=self.config.retry_jitter_s,
                        ),
                    ),
                    logger=logger,
                )

                if not await self.write_out_chunked(
                    "out",
                    process.blob_ip(geoparquet_bytes, content_type=GEOPARQUET_CONTENT_TYPE),
                ):
                    logger.info("%s process finished", self.name)
                    return
                logger.info("%s sent %s RBS GeoParquet bytes", self.name, len(geoparquet_bytes))

            except (OSError, TypeError, ValueError):
                logger.exception("%s failed to fetch RBS GeoParquet", self.name)

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(FetchRBSByRaster(METADATA), METADATA)


if __name__ == "__main__":
    main()
