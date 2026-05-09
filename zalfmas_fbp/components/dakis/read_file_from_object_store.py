#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from pathlib import PurePosixPath
from typing import override

import boto3
from boto3.session import Session
from botocore.response import StreamingBody
from types_boto3_s3.client import S3Client
from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.file_payload import object_key, prepared_file_bracket_ip, prepared_file_ip
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging
from zalfmas_fbp.run.metadata import ComponentMetadata

logger = logging.getLogger(__name__)
configure_logging()

METADATA = ComponentMetadata.model_validate(
    {
        "category": {"id": "dakis", "name": "DAKIS"},
        "info": {
            "id": "29aa4f52-3e62-4f03-9d05-d2b82d611b71",
            "name": "read file from object store",
            "description": "Read a binary file from an S3-compatible object store and emit it as a prepared file payload.",
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
            "object_store_url": {
                "value": "https://objects.dakispro.de",
                "type": "string",
                "desc": "S3-compatible object store endpoint URL.",
            },
            "access_key": {
                "value": "",
                "type": "string",
                "desc": "Object store access key.",
            },
            "secret_key": {
                "value": "",
                "type": "string",
                "desc": "Object store secret key.",
            },
            "bucket": {
                "value": "",
                "type": "string",
                "desc": "Object store bucket. If empty, the first path segment is used as the bucket.",
            },
            "path": {
                "value": "dakis",
                "type": "string",
                "desc": "Object key prefix or bucket/prefix when bucket is empty.",
            },
            "filename": {
                "value": "output.bin",
                "type": "string",
                "desc": "Object filename to read.",
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


class ReadFileFromObjectStoreConfig(process.ProcessConfig):
    object_store_url: str = "https://objects.dakispro.de"
    access_key: str = ""
    secret_key: str = ""
    bucket: str = ""
    path: str = "dakis"
    filename: str = "output.bin"
    content_type: str = "application/octet-stream"
    read_once_without_trigger: bool = True


class ReadFileFromObjectStore(process.Process[ReadFileFromObjectStoreConfig]):
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
            bucket, key = _bucket_and_key(
                bucket=self.config.bucket,
                path=self.config.path,
                filename=self.config.filename,
            )
            body = _get_object_body(
                endpoint_url=self.config.object_store_url,
                access_key=self.config.access_key,
                secret_key=self.config.secret_key,
                bucket=bucket,
                key=key,
            )
            try:
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

                while chunk := body.read(process.DEFAULT_BRACKETED_CHUNK_SIZE):
                    if not await self.write_out(
                        "out",
                        prepared_file_ip(
                            bytes(chunk),
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
            finally:
                close = getattr(body, "close", None)
                if callable(close):
                    close()
            logger.info("%s read file from object store at s3://%s/%s", self.name, bucket, key)
            return True

        except (ImportError, TypeError, ValueError):
            logger.exception("%s failed to read file from object store", self.name)
            return True


def _bucket_and_key(*, bucket: str, path: str, filename: str) -> tuple[str, str]:
    bucket = bucket.strip("/")
    path = path.strip("/")
    filename = filename.strip("/")

    if bucket:
        return bucket, object_key(path, filename)

    parts = PurePosixPath(object_key(path, filename)).parts
    if len(parts) < 2:
        msg = "Object store bucket is required when path does not include a bucket segment."
        raise ValueError(msg)

    return parts[0], str(PurePosixPath(*parts[1:]))


def _get_object_body(
    *,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    bucket: str,
    key: str,
) -> StreamingBody:
    session: boto3.session.Session = Session(
        aws_access_key_id=access_key or None,
        aws_secret_access_key=secret_key or None,
    )
    s3_client: S3Client = session.client("s3", endpoint_url=endpoint_url)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response["Body"]


def main():
    process.run_process_from_metadata_and_cmd_args(ReadFileFromObjectStore(METADATA), METADATA)


if __name__ == "__main__":
    main()
