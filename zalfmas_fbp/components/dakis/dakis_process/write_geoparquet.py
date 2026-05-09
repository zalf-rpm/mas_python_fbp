#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from typing import override

from mas.schema.common import common_capnp
from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.write_geoparquet import write_geoparquet_bytes
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging
from zalfmas_fbp.run.metadata import ComponentMetadata

logger = logging.getLogger(__name__)
configure_logging()

METADATA = ComponentMetadata.model_validate(
    {
        "category": {"id": "dakis", "name": "DAKIS"},
        "info": {
            "id": "d2f24a18-7210-44b9-9741-eec72f299715",
            "name": "write geoparquet",
            "description": "Write received GeoParquet bytes to disk.",
        },
        "type": "process",
        "inPorts": [{"name": "in", "contentType": "common.capnp:Value[Data]", "desc": "GeoParquet bytes."}],
        "outPorts": [],
        "defaultConfig": {
            "output_path": {
                "value": "outputs/dakis/geometries.parquet",
                "type": "string",
                "desc": "Path of the GeoParquet file to write.",
            },
            "compression": {
                "value": "zstd",
                "type": "string",
                "desc": "Parquet compression to use. Use 'preserve' to write bytes unchanged.",
            },
        },
    },
)


class WriteGeoparquetConfig(process.ProcessConfig):
    output_path: str = "outputs/dakis/geometries.parquet"
    compression: str = "zstd"


class WriteGeoparquet(process.Process[WriteGeoparquetConfig]):
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
            in_msg = await self.read_in("in")
            if in_msg is None:
                break

            try:
                geoparquet_bytes = bytes(in_msg.content.as_struct(common_capnp.Value).d)
                output_path = write_geoparquet_bytes(
                    geoparquet_bytes,
                    output_path=self.config.output_path,
                    compression=self.config.compression,
                )
                logger.info("%s wrote GeoParquet to %s", self.name, output_path)

            except (OSError, TypeError, ValueError):
                logger.exception("%s failed to write GeoParquet", self.name)

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(WriteGeoparquet(METADATA), METADATA)


if __name__ == "__main__":
    main()
