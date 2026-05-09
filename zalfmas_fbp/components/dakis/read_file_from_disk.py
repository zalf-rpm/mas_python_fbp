#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from pathlib import Path
from typing import override

from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.file_payload import prepared_file_bracket_ip, prepared_file_ip
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging
from zalfmas_fbp.run.metadata import ComponentMetadata

logger = logging.getLogger(__name__)
configure_logging()

METADATA = ComponentMetadata.model_validate(
    {
        "category": {"id": "dakis", "name": "DAKIS"},
        "info": {
            "id": "5e4af951-ddeb-4c72-97b4-f2b8bcb60d53",
            "name": "read file from disk",
            "description": "Read a binary file from local disk and emit it as a prepared file payload.",
        },
        "type": "process",
        "inPorts": [{"name": "trigger", "contentType": "AnyPointer", "desc": "Optional trigger messages."}],
        "outPorts": [
            {
                "name": "out",
                "contentType": "common.capnp:Value[Data]",
                "desc": "Prepared file payload bytes with path, filename, and content_type attributes.",
            },
        ],
        "defaultConfig": {
            "path": {
                "value": "outputs/dakis",
                "type": "string",
                "desc": "Directory containing the file.",
            },
            "filename": {
                "value": "output.bin",
                "type": "string",
                "desc": "Filename to read.",
            },
            "content_type": {
                "value": "application/octet-stream",
                "type": "string",
                "desc": "Content type attribute attached to the outgoing file payload.",
            },
            "read_once_without_trigger": {
                "value": True,
                "type": "bool",
                "desc": "If true, read once when no trigger port is connected.",
            },
        },
    },
)


class ReadFileFromDiskConfig(process.ProcessConfig):
    path: str = "outputs/dakis"
    filename: str = "output.bin"
    content_type: str = "application/octet-stream"
    read_once_without_trigger: bool = True


class ReadFileFromDisk(process.Process[ReadFileFromDiskConfig]):
    def __init__(
        self,
        metadata: ComponentMetadata = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        if self.in_ports.get("trigger") is None and self.config.read_once_without_trigger:
            await self._read_and_send()
            logger.info("%s process finished", self.name)
            return

        while True:
            trigger_msg = await self.read_in("trigger")
            if trigger_msg is None:
                break
            if not await self._read_and_send():
                logger.info("%s process finished", self.name)
                return

        logger.info("%s process finished", self.name)

    async def _read_and_send(self) -> bool:
        try:
            file_path = Path(self.config.path) / self.config.filename
            with file_path.open("rb") as file:
                if not await self.write_out(
                    "out",
                    prepared_file_bracket_ip(
                        bracket_type="openBracket",
                        path=self.config.path,
                        filename=self.config.filename,
                        content_type=self.config.content_type,
                    ),
                ):
                    return False

                while chunk := file.read(process.DEFAULT_BRACKETED_CHUNK_SIZE):
                    if not await self.write_out(
                        "out",
                        prepared_file_ip(
                            chunk,
                            path=self.config.path,
                            filename=self.config.filename,
                            content_type=self.config.content_type,
                        ),
                    ):
                        return False

            if not await self.write_out(
                "out",
                prepared_file_bracket_ip(
                    bracket_type="closeBracket",
                    path=self.config.path,
                    filename=self.config.filename,
                    content_type=self.config.content_type,
                ),
            ):
                return False
            logger.info("%s read file from %s", self.name, file_path)
            return True

        except OSError:
            logger.exception("%s failed to read file from disk", self.name)
            return True


def main():
    process.run_process_from_metadata_and_cmd_args(ReadFileFromDisk(METADATA), METADATA)


if __name__ == "__main__":
    main()
