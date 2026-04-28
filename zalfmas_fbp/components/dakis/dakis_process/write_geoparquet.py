#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from typing import Any, override

import capnp
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from zalfmas_common import common

import zalfmas_fbp.run.process as process
from zalfmas_fbp.components.dakis.common.write_geoparquet import write_geoparquet_bytes

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s @ %(name)s - %(levelname)-8s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

meta = {
    "category": {"id": "dakis", "name": "DAKIS"},
    "component": {
        "info": {
            "id": "bf812f76-4672-4c62-9f7d-e0aeaf7d4d02",
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
        await self.process_started()
        logger.info("%s process started", self.name)

        while self.in_ports["in"]:
            if self.is_canceled():
                break

            try:
                in_port = self.in_ports["in"]
                if not in_port:
                    break

                in_msg = await in_port.read()
                if in_msg.which() == "done":
                    self.in_ports["in"] = None
                    continue

                geoparquet_bytes = bytes(in_msg.value.as_struct(fbp_capnp.IP).content.as_struct(common_capnp.Value).d)
                output_path = write_geoparquet_bytes(
                    geoparquet_bytes,
                    output_path=self.config["output_path"].t,
                    compression=self.config["compression"].t,
                )
                logger.info("%s wrote GeoParquet to %s", self.name, output_path)

            except capnp.KjException as e:
                logger.error("%s RPC Exception: %s", self.name, e.description)
                if e.type in ["DISCONNECTED"]:
                    break
            except (OSError, TypeError, ValueError):
                logger.exception("%s failed to write GeoParquet", self.name)

        logger.info("%s process finished", self.name)
        await self.process_stopped()


def main():
    process.run_process_from_metadata_and_cmd_args(WriteGeoparquet(meta), meta)


if __name__ == "__main__":
    main()
