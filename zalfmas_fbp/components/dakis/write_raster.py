#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from typing import Literal, override

from mas.schema.common import common_capnp
from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.file_payload import prepared_file_ip
from zalfmas_fbp.components.dakis.common.write_raster import prepare_raster_bytes
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging
from zalfmas_fbp.run.metadata import ComponentMetadata

logger = logging.getLogger(__name__)
configure_logging()

type WriteRasterCompression = Literal["preserve", "zstd", "deflate", "lzw", "none"]
RASTER_COMPRESSION_OPTIONS = ["preserve", "zstd", "deflate", "lzw", "none"]

METADATA = ComponentMetadata.model_validate(
    {
        "category": {"id": "dakis", "name": "DAKIS"},
        "info": {
            "id": "f706a283-6d07-4aae-828f-6cf8e4bf62c2",
            "name": "write raster",
            "description": "Prepare received raster bytes as a GeoTIFF or Cloud Optimized GeoTIFF file payload.",
        },
        "type": "process",
        "inPorts": [{"name": "in", "contentType": "common.capnp:Value[Data]", "desc": "GeoTIFF raster bytes."}],
        "outPorts": [
            {"name": "out", "contentType": "common.capnp:Value[Data]", "desc": "Prepared GeoTIFF or COG file payload."}
        ],
        "defaultConfig": {
            "path": {
                "value": "",
                "type": "string",
                "desc": "Optional target path or object key prefix for the prepared file. Empty uses the sink default.",
            },
            "filename": {
                "value": "raster.tif",
                "type": "string",
                "desc": "Target filename for the prepared GeoTIFF.",
            },
            "compression": {
                "value": "preserve",
                "type": RASTER_COMPRESSION_OPTIONS,
                "desc": "GeoTIFF/COG compression algorithm. Use 'preserve' to keep the source compression.",
            },
            "write_as_cog": {
                "value": False,
                "type": "bool",
                "desc": "If true, rewrite the output as a Cloud Optimized GeoTIFF (COG).",
            },
        },
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
        metadata: ComponentMetadata = METADATA,
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
                raster_bytes = bytes(in_msg.content.as_struct(common_capnp.Value).d)
                output_bytes = prepare_raster_bytes(
                    raster_bytes,
                    compression=self.config.compression,
                    write_as_cog=self.config.write_as_cog,
                )
                out_ip = prepared_file_ip(
                    output_bytes,
                    path=self.config.path,
                    filename=self.config.filename,
                    content_type="image/tiff",
                )
                if not await self.write_out("out", out_ip, True):
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
