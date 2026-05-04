#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, override

import capnp
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.relabel import relabel_geoparquet_bytes
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import IPBuilder
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)
configure_logging()

meta = {
    "category": {"id": "dakis", "name": "DAKIS"},
    "component": {
        "info": {
            "id": "9db20ca7-ba92-4179-8f54-8e9945893421",
            "name": "relabel geoparquet",
            "description": "Relabel GeoParquet geometries using a CSV mapping table.",
        },
        "type": "process",
        "inPorts": [
            {"name": "in", "contentType": "common.capnp:Value[Data]", "desc": "GeoParquet bytes."},
            {
                "name": "translation",
                "contentType": "Text | common.capnp:Value[Data]",
                "desc": "Optional CSV mapping table bytes or path. If unconnected, mapping_csv_path is used.",
            },
        ],
        "outPorts": [{"name": "out", "contentType": "common.capnp:Value[Data]", "desc": "Relabeled GeoParquet bytes."}],
        "defaultConfig": {
            "mapping_csv_path": {
                "value": "resources/mappings/invekos_to_lulc.csv",
                "type": "string",
                "desc": "Path to the CSV mapping table.",
            },
            "source_code_column": {
                "value": "code",
                "type": "string",
                "desc": "Column in the GeoParquet and mapping CSV containing the source code.",
            },
            "target_code_column": {
                "value": "lucode",
                "type": "string",
                "desc": "Column in the mapping CSV to write to the output GeoParquet.",
            },
            "priority_column": {
                "value": "priority",
                "type": "string",
                "desc": "Optional priority column in the mapping CSV and output GeoParquet.",
            },
            "default_priority": {
                "value": 0,
                "type": "int",
                "desc": "Priority value used when the mapping CSV has no priority column or value.",
            },
        },
    },
}


class RelabelGeoparquet(process.Process):
    def __init__(self, metadata: dict[str, Any] | None, con_man: common.ConnectionManager | None = None):
        process.Process.__init__(self, metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        mapping_csv_path = str(self.config["mapping_csv_path"].t)
        mapping_csv_bytes = None

        while True:
            in_msg = await self.read_in("in")
            if in_msg is None:
                break

            try:
                translation_msg = await self.read_in("translation")
                if translation_msg is not None:
                    mapping_csv_path, mapping_csv_bytes = _read_translation(
                        translation_msg,
                        fallback_path=mapping_csv_path,
                    )

                geoparquet_bytes = bytes(in_msg.content.as_struct(common_capnp.Value).d)
                output_bytes = relabel_geoparquet_bytes(
                    geoparquet_bytes,
                    mapping_csv_path=mapping_csv_path,
                    mapping_csv_bytes=mapping_csv_bytes,
                    source_code_column=self.config["source_code_column"].t,
                    target_code_column=self.config["target_code_column"].t,
                    priority_column=self.config["priority_column"].t,
                    default_priority=self.config["default_priority"].i64,
                )

                if not await self.write_out("out", _data_ip(output_bytes)):
                    logger.info("%s process finished", self.name)
                    return
                logger.info("%s sent %s relabeled GeoParquet bytes", self.name, len(output_bytes))

            except capnp.KjException as e:
                logger.error("%s RPC Exception: %s", self.name, e.description)
                if e.type in ["DISCONNECTED"]:
                    break
            except (OSError, TypeError, ValueError):
                logger.exception("%s failed to relabel GeoParquet", self.name)

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(RelabelGeoparquet(meta), meta)


def _data_ip(data: bytes) -> IPBuilder:
    return fbp_capnp.IP.new_message(content=common_capnp.Value.new_message(d=data))


def _read_translation(ip: IPReader, *, fallback_path: str) -> tuple[str, bytes | None]:
    try:
        return fallback_path, bytes(ip.content.as_struct(common_capnp.Value).d)
    except (capnp.KjException, TypeError):
        return ip.content.as_text() or fallback_path, None


if __name__ == "__main__":
    main()
