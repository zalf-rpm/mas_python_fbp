#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from typing import Literal, override

from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.file_payload import (
    GEOPARQUET_CONTENT_TYPE,
    blob_content_type,
    prepared_file_ip,
)
from zalfmas_fbp.components.dakis.common.write_geoparquet import prepare_geoparquet_bytes
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()

type WriteParquetCompression = Literal["preserve", "zstd", "gzip", "snappy", "none"]


class WriteGeoparquetConfig(process.ProcessConfig):
    path: str = Field(
        "",
        description="Optional target path or object key prefix for the prepared file. Empty uses the sink default.",
    )
    filename: str = Field("geometries.parquet", description="Target filename for the prepared GeoParquet.")
    compression: WriteParquetCompression = Field(
        "zstd",
        description="GeoParquet compression algorithm. Use 'preserve' to write bytes unchanged.",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="dakis",
        name="DAKIS",
    ),
    info=meta.Info(
        id="d2f24a18-7210-44b9-9741-eec72f299715",
        name="write geoparquet",
        description="Prepare received GeoParquet bytes as a file payload.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType=blob_content_type(GEOPARQUET_CONTENT_TYPE),
            desc="GeoParquet bytes.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType=blob_content_type(GEOPARQUET_CONTENT_TYPE),
            desc="Prepared GeoParquet file payload.",
        ),
    ],
    config=WriteGeoparquetConfig,
)


class WriteGeoparquet(process.Process[WriteGeoparquetConfig]):
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
                geoparquet_bytes, _content_type = process.read_ip_data(in_msg)
                output_bytes = prepare_geoparquet_bytes(
                    geoparquet_bytes,
                    compression=self.config.compression,
                )
                if not await self.write_out_chunked(
                    "out",
                    prepared_file_ip(
                        output_bytes,
                        path=self.config.path,
                        filename=self.config.filename,
                        content_type=GEOPARQUET_CONTENT_TYPE,
                    ),
                ):
                    logger.info("%s process finished", self.name)
                    return
                logger.info("%s prepared GeoParquet file payload", self.name)

            except (OSError, TypeError, ValueError):
                logger.exception("%s failed to prepare GeoParquet file payload", self.name)

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(WriteGeoparquet(METADATA), METADATA)


if __name__ == "__main__":
    main()
