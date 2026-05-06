#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from typing import Any, override

from mas.schema.common import common_capnp
from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.write_geoparquet import write_geoparquet_bytes
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()

meta = {
    "category": {"id": "dakis", "name": "DAKIS"},
    "component": {
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
}


class WriteGeoparquet(process.Process):
    def __init__(self, metadata: dict[str, Any] | None, con_man: common.ConnectionManager | None = None):
        process.Process.__init__(self, metadata=metadata, con_man=con_man)

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
                    output_path=self.config["output_path"].t,
                    compression=self.config["compression"].t,
                )
                logger.info("%s wrote GeoParquet to %s", self.name, output_path)

            except (OSError, TypeError, ValueError):
                logger.exception("%s failed to write GeoParquet", self.name)

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(WriteGeoparquet(meta), meta)


if __name__ == "__main__":
    main()
