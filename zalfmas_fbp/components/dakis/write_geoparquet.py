#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from typing import Literal, override

from mas.schema.common import common_capnp
from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.file_payload import prepared_file_ip
from zalfmas_fbp.components.dakis.common.write_geoparquet import prepare_geoparquet_bytes
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()

type WriteParquetCompression = Literal["preserve", "zstd", "gzip", "snappy", "none"]
PARQUET_COMPRESSION_OPTIONS = ["preserve", "zstd", "gzip", "snappy", "none"]

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
            contentType="common.capnp:Value[Data]",
            desc="GeoParquet bytes.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="common.capnp:Value[Data]",
            desc="Prepared GeoParquet file payload.",
        ),
    ],
    defaultConfig={
        "path": meta.ConfigEntry(
            value="",
            type="string",
            desc="Optional target path or object key prefix for the prepared file. Empty uses the sink default.",
        ),
        "filename": meta.ConfigEntry(
            value="geometries.parquet",
            type="string",
            desc="Target filename for the prepared GeoParquet.",
        ),
        "compression": meta.ConfigEntry(
            value="zstd",
            type=PARQUET_COMPRESSION_OPTIONS,
            desc="GeoParquet compression algorithm. Use 'preserve' to write bytes unchanged.",
        ),
    },
)


class WriteGeoparquetConfig(process.ProcessConfig):
    path: str = ""
    filename: str = "geometries.parquet"
    compression: WriteParquetCompression = "zstd"


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
            in_msg = await self.read_in("in", True)
            if in_msg is None:
                break

            try:
                geoparquet_bytes = bytes(in_msg.content.as_struct(common_capnp.Value).d)
                output_bytes = prepare_geoparquet_bytes(
                    geoparquet_bytes,
                    compression=self.config.compression,
                )
                out_ip = prepared_file_ip(
                    output_bytes,
                    path=self.config.path,
                    filename=self.config.filename,
                    content_type="application/geoparquet",
                )
                if not await self.write_out("out", out_ip, True):
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
