#!/usr/bin/python
# -*- coding: UTF-8

from __future__ import annotations

import logging
from pathlib import PurePosixPath
from tempfile import TemporaryFile
from typing import Any, BinaryIO, override

import boto3
from boto3.session import Session
from types_boto3_s3.client import S3Client
from zalfmas_common import common

from zalfmas_fbp.components.dakis.common.file_payload import (
    object_key,
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
            "id": "28c6182f-0a3a-4afd-b3e1-856c796f1b2a",
            "name": "write file to object store",
            "description": "Write prepared binary file payloads to an S3-compatible object store.",
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
                "desc": "Fallback object key prefix if the incoming file payload has no path attribute.",
            },
            "filename": {
                "value": "output.bin",
                "type": "string",
                "desc": "Fallback filename if the incoming file payload has no filename attribute.",
            },
        },
    },
)


class WriteFileToObjectStoreConfig(process.ProcessConfig):
    object_store_url: str = "https://objects.dakispro.de"
    access_key: str = ""
    secret_key: str = ""
    bucket: str = ""
    path: str = "dakis"
    filename: str = "output.bin"


class WriteFileToObjectStore(process.Process[WriteFileToObjectStoreConfig]):
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
                path, filename, content_type, body = await self._chunked_body(in_msg)
            elif in_msg.type == "closeBracket":
                continue
            else:
                data, path, filename, content_type = read_prepared_file(
                    in_msg,
                    default_path=self.config.path,
                    default_filename=self.config.filename,
                )
                body = data
            bucket, key = _bucket_and_key(
                bucket=self.config.bucket,
                path=path,
                filename=filename,
            )
            _put_object(
                endpoint_url=self.config.object_store_url,
                access_key=self.config.access_key,
                secret_key=self.config.secret_key,
                bucket=bucket,
                key=key,
                body=body,
                content_type=content_type,
            )
            if not isinstance(body, bytes):
                body.close()
            logger.info("%s wrote file to object store at s3://%s/%s", self.name, bucket, key)

        logger.info("%s process finished", self.name)

    async def _chunked_body(self, open_ip) -> tuple[str, str, str, BinaryIO]:
        path, filename, content_type = read_prepared_file_metadata(
            open_ip,
            default_path=self.config.path,
            default_filename=self.config.filename,
        )
        tmp_file: BinaryIO = TemporaryFile("w+b")
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
        tmp_file.seek(0)
        return path, filename, content_type, tmp_file


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


def _put_object(
    *,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    bucket: str,
    key: str,
    body: Any,
    content_type: str,
) -> None:
    session: boto3.session.Session = Session(
        aws_access_key_id=access_key or None,
        aws_secret_access_key=secret_key or None,
    )
    s3_client: S3Client = session.client("s3", endpoint_url=endpoint_url)
    s3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)


def main():
    process.run_process_from_metadata_and_cmd_args(WriteFileToObjectStore(METADATA), METADATA)


if __name__ == "__main__":
    main()
