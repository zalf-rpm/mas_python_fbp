#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import override

from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.file_payload import (
    read_prepared_file,
    read_prepared_file_chunk,
    read_prepared_file_metadata,
)
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging
from zalfmas_fbp.run.metadata import ComponentMetadata

logger = logging.getLogger(__name__)
configure_logging()

METADATA = ComponentMetadata.model_validate(
    {
        "category": {"id": "dakis", "name": "DAKIS"},
        "info": {
            "id": "34f58819-e23f-4cc1-9691-87d5225b122d",
            "name": "write file to disk",
            "description": "Write prepared binary file payloads to local disk.",
        },
        "type": "process",
        "inPorts": [
            {
                "name": "in",
                "contentType": "common.capnp:Value[Data]",
                "desc": "Prepared file payload bytes with optional path and filename attributes.",
            },
        ],
        "outPorts": [],
        "defaultConfig": {
            "path": {
                "value": "outputs/dakis",
                "type": "string",
                "desc": "Fallback output directory if the incoming file payload has no path attribute.",
            },
            "filename": {
                "value": "output.bin",
                "type": "string",
                "desc": "Fallback filename if the incoming file payload has no filename attribute.",
            },
        },
    },
)


class WriteFileToDiskConfig(process.ProcessConfig):
    path: str = "outputs/dakis"
    filename: str = "output.bin"


class WriteFileToDisk(process.Process[WriteFileToDiskConfig]):
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
            in_msg = await self._read_in_raw("in")
            if in_msg is None:
                break

            if in_msg.type == "openBracket":
                output_path = await self._write_chunked_file(in_msg)
            elif in_msg.type == "closeBracket":
                continue
            else:
                data, path, filename, _content_type = read_prepared_file(
                    in_msg,
                    default_path=self.config.path,
                    default_filename=self.config.filename,
                )
                output_path = _write_file(data, path=path, filename=filename)
            logger.info("%s wrote file to %s", self.name, output_path)

        logger.info("%s process finished", self.name)

    async def _write_chunked_file(self, open_ip) -> Path:
        path, filename, _content_type = read_prepared_file_metadata(
            open_ip,
            default_path=self.config.path,
            default_filename=self.config.filename,
        )
        output_path = Path(path) / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path: Path | None = None

        try:
            with NamedTemporaryFile(
                "wb", dir=output_path.parent, prefix=f".{output_path.name}.", suffix=".tmp", delete=False
            ) as tmp_file:
                tmp_path = Path(tmp_file.name)
                while True:
                    chunk_ip = await self._read_in_raw("in")
                    if chunk_ip is None:
                        msg = "Input port 'in' closed before the prepared file payload ended."
                        raise ValueError(msg)
                    if chunk_ip.type == "closeBracket":
                        break
                    if chunk_ip.type == "openBracket":
                        msg = "Nested file payload brackets are not supported."
                        raise ValueError(msg)
                    tmp_file.write(read_prepared_file_chunk(chunk_ip))

            os.replace(tmp_path, output_path)
        except Exception:
            if tmp_path is not None:
                tmp_path.unlink(missing_ok=True)
            raise

        return output_path


def _write_file(data: bytes, *, path: str, filename: str) -> Path:
    output_path = Path(path) / filename
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with NamedTemporaryFile(
        "wb", dir=output_path.parent, prefix=f".{output_path.name}.", suffix=".tmp", delete=False
    ) as tmp_file:
        tmp_path = Path(tmp_file.name)
        tmp_file.write(data)

    try:
        os.replace(tmp_path, output_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

    return output_path


def main():
    process.run_process_from_metadata_and_cmd_args(WriteFileToDisk(METADATA), METADATA)


if __name__ == "__main__":
    main()
